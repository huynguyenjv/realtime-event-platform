package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

type Store struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func New(ctx context.Context, databaseURL string, logger *slog.Logger) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	logger.Info("Connected to TimescaleDB")
	return &Store{pool: pool, logger: logger}, nil
}

// InsertBatch inserts multiple processed events using pgx batch.
func (s *Store) InsertBatch(ctx context.Context, events []model.ProcessedEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, e := range events {
		batch.Queue(
			`INSERT INTO events (id, event_type, source, payload, metadata, timestamp, processed_at, event_size_bytes)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			e.ID, e.Type, e.Source, e.Payload, e.Metadata, e.Timestamp, e.ProcessedAt, e.EventSizeBytes,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range events {
		if _, err := br.Exec(); err != nil {
			s.logger.Error("Failed to insert event", "error", err)
			return fmt.Errorf("batch insert: %w", err)
		}
	}

	return nil
}

// QueryHistory returns aggregated metrics from continuous aggregates.
func (s *Store) QueryHistory(ctx context.Context, q model.HistoryQuery) (*model.HistoryMetrics, error) {
	interval := q.Interval
	if interval == "" {
		interval = autoSelectInterval(q)
	}

	view := intervalToView(interval)

	query := fmt.Sprintf(
		`SELECT bucket, event_type, event_count, avg_value, min_value, max_value
		 FROM %s
		 WHERE bucket >= $1 AND bucket < $2`,
		view,
	)
	args := []any{q.From, q.To}

	if q.EventType != "" {
		query += " AND event_type = $3"
		args = append(args, q.EventType)
	}
	query += " ORDER BY bucket DESC"

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}
	defer rows.Close()

	var data []model.MetricsBucket
	for rows.Next() {
		var b model.MetricsBucket
		if err := rows.Scan(&b.Bucket, &b.EventType, &b.Count, &b.AvgValue, &b.MinValue, &b.MaxValue); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		data = append(data, b)
	}

	return &model.HistoryMetrics{
		Interval: interval,
		From:     q.From,
		To:       q.To,
		Data:     data,
	}, nil
}

// QueryTodayStats returns today's stats for a specific event type.
func (s *Store) QueryTodayStats(ctx context.Context, eventType string) (*model.TodayStats, error) {
	today := time.Now().Truncate(24 * time.Hour)

	row := s.pool.QueryRow(ctx,
		`SELECT COALESCE(SUM(event_count), 0), COALESCE(AVG(avg_value), 0)
		 FROM event_metrics_1h
		 WHERE event_type = $1 AND bucket >= $2`,
		eventType, today,
	)

	var stats model.TodayStats
	if err := row.Scan(&stats.TotalCount, &stats.AvgValue); err != nil {
		return nil, fmt.Errorf("query today stats: %w", err)
	}
	return &stats, nil
}

func (s *Store) Health(ctx context.Context) bool {
	return s.pool.Ping(ctx) == nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func autoSelectInterval(q model.HistoryQuery) string {
	duration := q.To.Sub(q.From)
	switch {
	case duration <= time.Hour:
		return "1m"
	case duration <= 24*time.Hour:
		return "5m"
	default:
		return "1h"
	}
}

func intervalToView(interval string) string {
	switch interval {
	case "1m":
		return "event_metrics_1m"
	case "5m":
		return "event_metrics_5m"
	case "1h":
		return "event_metrics_1h"
	default:
		return "event_metrics_1m"
	}
}
