# Analytics Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a stream-processing analytics service that consumes raw events from Kafka, processes them through a channel-based pipeline, stores in TimescaleDB, caches real-time metrics in Redis, and publishes processed events back to Kafka.

**Architecture:** Channel-based pipeline (validate → transform → fan-out) consuming from Kafka `events.raw`. Fan-out writes to three destinations in parallel: in-memory aggregator (flushed to Redis every 10s), TimescaleDB (batch insert), and Kafka producer (events.processed + events.analytics). HTTP REST API serves real-time metrics from Redis and historical data from TimescaleDB continuous aggregates.

**Tech Stack:** Go 1.26.3, Gin, segmentio/kafka-go, pgx/v5, go-redis/v9, TimescaleDB

**Spec:** `docs/superpowers/specs/2026-06-14-analytics-service-design.md`

---

## File Map

```
services/analytics-service/
├── cmd/server/main.go                  # Entry point, wiring, graceful shutdown
├── internal/
│   ├── config/config.go                # Environment-based config loading
│   ├── model/
│   │   ├── event.go                    # ProcessedEvent, AggregationSnapshot
│   │   └── metrics.go                  # API response models
│   ├── consumer/consumer.go            # Kafka consumer group, feeds pipeline
│   ├── pipeline/
│   │   ├── pipeline.go                 # Orchestrator — wires stages, manages channels
│   │   ├── validator.go                # Stage 1: validate event structure
│   │   ├── transformer.go             # Stage 2: normalize + enrich
│   │   └── fanout.go                   # Stage 3: dispatch to aggregate/store/publish
│   ├── aggregator/
│   │   ├── window.go                   # In-memory time-window counters
│   │   └── flusher.go                  # Periodic flush to Redis
│   ├── store/timescaledb.go            # Batch insert + query methods
│   ├── publisher/publisher.go          # Kafka producer for processed + analytics
│   ├── cache/redis.go                  # Redis read/write for metrics
│   └── handler/
│       ├── handler.go                  # Route registration
│       ├── metrics_handler.go          # GET /metrics/realtime, /history, /stats
│       └── health.go                   # GET /health, /ready
├── migrations/
│   ├── 000001_enable_timescaledb.up.sql
│   ├── 000001_enable_timescaledb.down.sql
│   ├── 000002_create_hypertable.up.sql
│   ├── 000002_create_hypertable.down.sql
│   ├── 000003_continuous_aggregates.up.sql
│   └── 000003_continuous_aggregates.down.sql
├── Dockerfile
└── go.mod
```

---

### Task 1: Project Setup and Dependencies

**Files:**
- Modify: `services/analytics-service/go.mod`
- Modify: `services/analytics-service/cmd/server/main.go` (minimal, just verify build)

- [ ] **Step 1: Add dependencies to go.mod**

Replace `services/analytics-service/go.mod` with:

```go
module github.com/huynguyenjv/realtime-event-platform/analytics-service

go 1.26.3

require (
	github.com/gin-gonic/gin v1.12.0
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.7.5
	github.com/redis/go-redis/v9 v9.10.0
	github.com/segmentio/kafka-go v0.4.51
)
```

- [ ] **Step 2: Run go mod tidy**

Run: `cd services/analytics-service && go mod tidy`
Expected: Dependencies downloaded, `go.sum` generated

- [ ] **Step 3: Verify the service compiles**

Run: `cd services/analytics-service && go build ./cmd/server/`
Expected: Build succeeds (existing stub main.go compiles)

- [ ] **Step 4: Commit**

```bash
git add services/analytics-service/go.mod services/analytics-service/go.sum
git commit -m "feat(analytics): add project dependencies"
```

---

### Task 2: Configuration

**Files:**
- Create: `services/analytics-service/internal/config/config.go`

- [ ] **Step 1: Write config test**

Create `services/analytics-service/internal/config/config_test.go`:

```go
package config

import (
	"os"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear any env vars that could interfere
	os.Unsetenv("PORT")
	os.Unsetenv("APP_ENV")
	os.Unsetenv("KAFKA_BROKERS")

	cfg := Load()

	if cfg.Port != "8080" {
		t.Errorf("Port = %q, want %q", cfg.Port, "8080")
	}
	if cfg.AppEnv != "development" {
		t.Errorf("AppEnv = %q, want %q", cfg.AppEnv, "development")
	}
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != "localhost:9092" {
		t.Errorf("KafkaBrokers = %v, want [localhost:9092]", cfg.KafkaBrokers)
	}
	if cfg.KafkaGroupID != "analytics-service" {
		t.Errorf("KafkaGroupID = %q, want %q", cfg.KafkaGroupID, "analytics-service")
	}
	if cfg.ConsumeTopic != "events.raw" {
		t.Errorf("ConsumeTopic = %q, want %q", cfg.ConsumeTopic, "events.raw")
	}
	if cfg.DatabaseURL != "postgres://postgres:postgres@localhost:5433/eventdb?sslmode=disable" {
		t.Errorf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr = %q, want %q", cfg.RedisAddr, "localhost:6379")
	}
	if cfg.WorkerCount != 4 {
		t.Errorf("WorkerCount = %d, want 4", cfg.WorkerCount)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", cfg.BatchSize)
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	os.Setenv("PORT", "9999")
	os.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	defer func() {
		os.Unsetenv("PORT")
		os.Unsetenv("KAFKA_BROKERS")
	}()

	cfg := Load()

	if cfg.Port != "9999" {
		t.Errorf("Port = %q, want %q", cfg.Port, "9999")
	}
	if len(cfg.KafkaBrokers) != 2 {
		t.Errorf("KafkaBrokers len = %d, want 2", len(cfg.KafkaBrokers))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/analytics-service && go test ./internal/config/ -v`
Expected: FAIL — package doesn't exist yet

- [ ] **Step 3: Write implementation**

Create `services/analytics-service/internal/config/config.go`:

```go
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port   string
	AppEnv string

	// Kafka
	KafkaBrokers []string
	KafkaGroupID string
	ConsumeTopic string

	// TimescaleDB
	DatabaseURL string

	// Redis
	RedisAddr     string
	RedisPassword string

	// Pipeline
	WorkerCount        int
	ChannelBuffer      int
	BatchSize          int
	BatchFlushInterval time.Duration

	// Aggregation
	FlushInterval time.Duration
	MaxEventAge   time.Duration
}

func Load() *Config {
	return &Config{
		Port:   getEnv("PORT", "8080"),
		AppEnv: getEnv("APP_ENV", "development"),

		KafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "analytics-service"),
		ConsumeTopic: getEnv("KAFKA_CONSUME_TOPIC", "events.raw"),

		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5433/eventdb?sslmode=disable"),

		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),

		WorkerCount:        getEnvInt("WORKER_COUNT", 4),
		ChannelBuffer:      getEnvInt("CHANNEL_BUFFER", 1000),
		BatchSize:          getEnvInt("BATCH_SIZE", 100),
		BatchFlushInterval: getEnvDuration("BATCH_FLUSH_INTERVAL", 5*time.Second),

		FlushInterval: getEnvDuration("FLUSH_INTERVAL", 10*time.Second),
		MaxEventAge:   getEnvDuration("MAX_EVENT_AGE", 24*time.Hour),
	}
}

func (c *Config) IsDevelopment() bool {
	return c.AppEnv == "development"
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd services/analytics-service && go test ./internal/config/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/analytics-service/internal/config/
git commit -m "feat(analytics): add configuration with env loading"
```

---

### Task 3: Domain Models

**Files:**
- Create: `services/analytics-service/internal/model/event.go`
- Create: `services/analytics-service/internal/model/metrics.go`

- [ ] **Step 1: Write model test**

Create `services/analytics-service/internal/model/event_test.go`:

```go
package model

import (
	"testing"
	"time"
)

func TestProcessedEvent_HasRequiredFields(t *testing.T) {
	now := time.Now()
	e := ProcessedEvent{
		ID:             "test-id",
		Type:           "user.click",
		Source:         "web",
		Payload:        map[string]any{"value": 42.5},
		Metadata:       map[string]string{"region": "us"},
		Timestamp:      now,
		ProcessedAt:    now,
		EventSizeBytes: 128,
	}

	if e.ID != "test-id" {
		t.Errorf("ID = %q, want %q", e.ID, "test-id")
	}
	if e.EventSizeBytes != 128 {
		t.Errorf("EventSizeBytes = %d, want 128", e.EventSizeBytes)
	}
}

func TestAggregationSnapshot_Fields(t *testing.T) {
	snap := AggregationSnapshot{
		EventType:  "user.click",
		Window:     "1m",
		Count:      1523,
		RatePerSec: 25.38,
		AvgValue:   42.5,
		P95Value:   89.2,
		P99Value:   97.1,
	}

	if snap.Count != 1523 {
		t.Errorf("Count = %d, want 1523", snap.Count)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/analytics-service && go test ./internal/model/ -v`
Expected: FAIL — types not defined

- [ ] **Step 3: Write event models**

Create `services/analytics-service/internal/model/event.go`:

```go
package model

import "time"

// ProcessedEvent is an event after validation and transformation.
type ProcessedEvent struct {
	ID             string            `json:"id"`
	Type           string            `json:"type"`
	Source         string            `json:"source"`
	Payload        map[string]any    `json:"payload"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Timestamp      time.Time         `json:"timestamp"`
	ProcessedAt    time.Time         `json:"processed_at"`
	EventSizeBytes int               `json:"event_size_bytes"`
}

// AggregationSnapshot is a periodic summary published to Kafka.
type AggregationSnapshot struct {
	EventType  string    `json:"event_type"`
	Window     string    `json:"window"`
	Timestamp  time.Time `json:"timestamp"`
	Count      int64     `json:"count"`
	RatePerSec float64   `json:"rate_per_sec"`
	AvgValue   float64   `json:"avg_value,omitempty"`
	MinValue   float64   `json:"min_value,omitempty"`
	MaxValue   float64   `json:"max_value,omitempty"`
	P50Value   float64   `json:"p50_value,omitempty"`
	P95Value   float64   `json:"p95_value,omitempty"`
	P99Value   float64   `json:"p99_value,omitempty"`
}
```

- [ ] **Step 4: Write metrics response models**

Create `services/analytics-service/internal/model/metrics.go`:

```go
package model

import "time"

// RealtimeMetrics is the response for GET /api/v1/metrics/realtime.
type RealtimeMetrics struct {
	TotalEvents     int64            `json:"total_events"`
	EventsPerSecond float64          `json:"events_per_second"`
	TopEventTypes   []EventTypeStats `json:"top_event_types"`
	Window          string           `json:"window"`
	Timestamp       time.Time        `json:"timestamp"`
}

type EventTypeStats struct {
	Type  string  `json:"type"`
	Count int64   `json:"count"`
	Rate  float64 `json:"rate"`
}

// HistoryQuery is the parsed query for GET /api/v1/metrics/history.
type HistoryQuery struct {
	From      time.Time
	To        time.Time
	Interval  string
	EventType string
}

// HistoryMetrics is the response for GET /api/v1/metrics/history.
type HistoryMetrics struct {
	Interval string          `json:"interval"`
	From     time.Time       `json:"from"`
	To       time.Time       `json:"to"`
	Data     []MetricsBucket `json:"data"`
}

type MetricsBucket struct {
	Bucket    time.Time `json:"bucket"`
	EventType string    `json:"event_type"`
	Count     int64     `json:"count"`
	AvgValue  float64   `json:"avg_value,omitempty"`
	MinValue  float64   `json:"min_value,omitempty"`
	MaxValue  float64   `json:"max_value,omitempty"`
	P50Value  float64   `json:"p50_value,omitempty"`
	P95Value  float64   `json:"p95_value,omitempty"`
	P99Value  float64   `json:"p99_value,omitempty"`
}

// EventDetailStats is the response for GET /api/v1/stats/:event_type.
type EventDetailStats struct {
	EventType string              `json:"event_type"`
	Realtime  RealtimeTypeStats   `json:"realtime"`
	Today     TodayStats          `json:"today"`
}

type RealtimeTypeStats struct {
	Count1m    int64   `json:"count_1m"`
	Count5m    int64   `json:"count_5m"`
	Count1h    int64   `json:"count_1h"`
	RatePerSec float64 `json:"rate_per_sec"`
}

type TodayStats struct {
	TotalCount int64   `json:"total_count"`
	AvgValue   float64 `json:"avg_value,omitempty"`
	P95Value   float64 `json:"p95_value,omitempty"`
	P99Value   float64 `json:"p99_value,omitempty"`
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd services/analytics-service && go test ./internal/model/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add services/analytics-service/internal/model/
git commit -m "feat(analytics): add domain and response models"
```

---

### Task 4: TimescaleDB Store

**Files:**
- Create: `services/analytics-service/migrations/` (SQL files)
- Create: `services/analytics-service/internal/store/timescaledb.go`

- [ ] **Step 1: Create migration — enable TimescaleDB**

Create `services/analytics-service/migrations/000001_enable_timescaledb.up.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

Create `services/analytics-service/migrations/000001_enable_timescaledb.down.sql`:

```sql
-- Cannot safely drop timescaledb if other objects depend on it
```

- [ ] **Step 2: Create migration — hypertable + columns**

Create `services/analytics-service/migrations/000002_create_hypertable.up.sql`:

```sql
-- Add columns needed by analytics service
ALTER TABLE events ADD COLUMN IF NOT EXISTS processed_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE events ADD COLUMN IF NOT EXISTS event_size_bytes INTEGER;

-- Convert events table to hypertable
SELECT create_hypertable('events', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);

-- Retention policy: drop raw events older than 30 days
SELECT add_retention_policy('events', INTERVAL '30 days', if_not_exists => TRUE);
```

Create `services/analytics-service/migrations/000002_create_hypertable.down.sql`:

```sql
SELECT remove_retention_policy('events', if_exists => TRUE);
ALTER TABLE events DROP COLUMN IF EXISTS processed_at;
ALTER TABLE events DROP COLUMN IF EXISTS event_size_bytes;
```

- [ ] **Step 3: Create migration — continuous aggregates**

Create `services/analytics-service/migrations/000003_continuous_aggregates.up.sql`:

```sql
-- 1-minute aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS event_metrics_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    event_type,
    COUNT(*) AS event_count,
    AVG((payload->>'value')::numeric) AS avg_value,
    MIN((payload->>'value')::numeric) AS min_value,
    MAX((payload->>'value')::numeric) AS max_value
FROM events
GROUP BY bucket, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_metrics_1m',
    start_offset => INTERVAL '5 minutes',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

-- 5-minute aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS event_metrics_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', timestamp) AS bucket,
    event_type,
    COUNT(*) AS event_count,
    AVG((payload->>'value')::numeric) AS avg_value,
    MIN((payload->>'value')::numeric) AS min_value,
    MAX((payload->>'value')::numeric) AS max_value
FROM events
GROUP BY bucket, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_metrics_5m',
    start_offset => INTERVAL '30 minutes',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE);

-- 1-hour aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS event_metrics_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', timestamp) AS bucket,
    event_type,
    COUNT(*) AS event_count,
    AVG((payload->>'value')::numeric) AS avg_value,
    MIN((payload->>'value')::numeric) AS min_value,
    MAX((payload->>'value')::numeric) AS max_value
FROM events
GROUP BY bucket, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_metrics_1h',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE);

-- Retention policies for aggregates
SELECT add_retention_policy('event_metrics_1m', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('event_metrics_5m', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('event_metrics_1h', INTERVAL '365 days', if_not_exists => TRUE);
```

Create `services/analytics-service/migrations/000003_continuous_aggregates.down.sql`:

```sql
SELECT remove_retention_policy('event_metrics_1h', if_exists => TRUE);
SELECT remove_retention_policy('event_metrics_5m', if_exists => TRUE);
SELECT remove_retention_policy('event_metrics_1m', if_exists => TRUE);

DROP MATERIALIZED VIEW IF EXISTS event_metrics_1h;
DROP MATERIALIZED VIEW IF EXISTS event_metrics_5m;
DROP MATERIALIZED VIEW IF EXISTS event_metrics_1m;
```

- [ ] **Step 4: Write store test**

Create `services/analytics-service/internal/store/timescaledb_test.go`:

```go
package store

import (
	"testing"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func TestAutoSelectInterval(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{"under 1h uses 1m", 30 * time.Minute, "1m"},
		{"under 24h uses 5m", 12 * time.Hour, "5m"},
		{"over 24h uses 1h", 48 * time.Hour, "1h"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := model.HistoryQuery{
				From: time.Now().Add(-tt.duration),
				To:   time.Now(),
			}
			got := autoSelectInterval(q)
			if got != tt.want {
				t.Errorf("autoSelectInterval() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIntervalToView(t *testing.T) {
	tests := []struct {
		interval string
		want     string
	}{
		{"1m", "event_metrics_1m"},
		{"5m", "event_metrics_5m"},
		{"1h", "event_metrics_1h"},
		{"unknown", "event_metrics_1m"},
	}

	for _, tt := range tests {
		t.Run(tt.interval, func(t *testing.T) {
			got := intervalToView(tt.interval)
			if got != tt.want {
				t.Errorf("intervalToView(%q) = %q, want %q", tt.interval, got, tt.want)
			}
		})
	}
}
```

- [ ] **Step 5: Run test to verify it fails**

Run: `cd services/analytics-service && go test ./internal/store/ -v`
Expected: FAIL — package doesn't exist

- [ ] **Step 6: Write store implementation**

Create `services/analytics-service/internal/store/timescaledb.go`:

```go
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
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `cd services/analytics-service && go test ./internal/store/ -v`
Expected: PASS (unit tests for helper functions)

- [ ] **Step 8: Commit**

```bash
git add services/analytics-service/migrations/ services/analytics-service/internal/store/
git commit -m "feat(analytics): add TimescaleDB store with migrations and batch insert"
```

---

### Task 5: Redis Cache

**Files:**
- Create: `services/analytics-service/internal/cache/redis.go`

- [ ] **Step 1: Write cache test**

Create `services/analytics-service/internal/cache/redis_test.go`:

```go
package cache

import (
	"testing"
)

func TestMetricsKey(t *testing.T) {
	tests := []struct {
		eventType string
		suffix    string
		want      string
	}{
		{"user.click", "count", "metrics:realtime:user.click:count"},
		{"order.created", "rate", "metrics:realtime:order.created:rate"},
	}

	for _, tt := range tests {
		got := metricsKey(tt.eventType, tt.suffix)
		if got != tt.want {
			t.Errorf("metricsKey(%q, %q) = %q, want %q", tt.eventType, tt.suffix, got, tt.want)
		}
	}
}

func TestWindowKey(t *testing.T) {
	got := windowKey("1m", "user.click")
	want := "metrics:window:1m:user.click"
	if got != want {
		t.Errorf("windowKey() = %q, want %q", got, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/analytics-service && go test ./internal/cache/ -v`
Expected: FAIL

- [ ] **Step 3: Write cache implementation**

Create `services/analytics-service/internal/cache/redis.go`:

```go
package cache

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

type Cache struct {
	rdb    *redis.Client
	logger *slog.Logger
}

func New(addr, password string, logger *slog.Logger) (*Cache, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connect to redis: %w", err)
	}

	logger.Info("Connected to Redis")
	return &Cache{rdb: rdb, logger: logger}, nil
}

// FlushCounters writes aggregated counters to Redis in a pipeline.
func (c *Cache) FlushCounters(ctx context.Context, counters map[string]int64, totalCount int64) error {
	pipe := c.rdb.Pipeline()
	ttl := 5 * time.Minute

	for eventType, count := range counters {
		pipe.IncrBy(ctx, metricsKey(eventType, "count"), count)
		pipe.Expire(ctx, metricsKey(eventType, "count"), ttl)
		pipe.ZIncrBy(ctx, "metrics:realtime:top_types", float64(count), eventType)
	}

	pipe.IncrBy(ctx, "metrics:realtime:total_count", totalCount)
	pipe.Expire(ctx, "metrics:realtime:total_count", ttl)
	pipe.Expire(ctx, "metrics:realtime:top_types", ttl)

	_, err := pipe.Exec(ctx)
	if err != nil {
		c.logger.Error("Failed to flush counters to Redis", "error", err)
		return err
	}
	return nil
}

// FlushWindowStats writes window statistics as a Redis hash.
func (c *Cache) FlushWindowStats(ctx context.Context, window string, stats map[string]model.WindowStats) error {
	pipe := c.rdb.Pipeline()

	for eventType, s := range stats {
		key := windowKey(window, eventType)
		pipe.HSet(ctx, key, map[string]any{
			"count": s.Count,
			"sum":   s.Sum,
			"min":   s.Min,
			"max":   s.Max,
		})
		pipe.Expire(ctx, key, windowTTL(window))
	}

	_, err := pipe.Exec(ctx)
	return err
}

// GetRealtimeMetrics reads current metrics from Redis.
func (c *Cache) GetRealtimeMetrics(ctx context.Context) (*model.RealtimeMetrics, error) {
	totalStr, err := c.rdb.Get(ctx, "metrics:realtime:total_count").Result()
	if err == redis.Nil {
		totalStr = "0"
	} else if err != nil {
		return nil, err
	}

	total, _ := strconv.ParseInt(totalStr, 10, 64)

	// Get top event types
	topTypes, err := c.rdb.ZRevRangeWithScores(ctx, "metrics:realtime:top_types", 0, 9).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	var topStats []model.EventTypeStats
	for _, z := range topTypes {
		topStats = append(topStats, model.EventTypeStats{
			Type:  z.Member.(string),
			Count: int64(z.Score),
		})
	}

	return &model.RealtimeMetrics{
		TotalEvents:   total,
		TopEventTypes: topStats,
		Window:        "1m",
		Timestamp:     time.Now(),
	}, nil
}

// GetEventTypeStats reads per-type realtime stats from Redis.
func (c *Cache) GetEventTypeStats(ctx context.Context, eventType string) (*model.RealtimeTypeStats, error) {
	pipe := c.rdb.Pipeline()

	count1m := pipe.HGet(ctx, windowKey("1m", eventType), "count")
	count5m := pipe.HGet(ctx, windowKey("5m", eventType), "count")
	count1h := pipe.HGet(ctx, windowKey("1h", eventType), "count")

	_, _ = pipe.Exec(ctx)

	stats := &model.RealtimeTypeStats{}
	stats.Count1m, _ = strconv.ParseInt(count1m.Val(), 10, 64)
	stats.Count5m, _ = strconv.ParseInt(count5m.Val(), 10, 64)
	stats.Count1h, _ = strconv.ParseInt(count1h.Val(), 10, 64)

	return stats, nil
}

func (c *Cache) Health(ctx context.Context) bool {
	return c.rdb.Ping(ctx).Err() == nil
}

func (c *Cache) Close() error {
	return c.rdb.Close()
}

func metricsKey(eventType, suffix string) string {
	return fmt.Sprintf("metrics:realtime:%s:%s", eventType, suffix)
}

func windowKey(window, eventType string) string {
	return fmt.Sprintf("metrics:window:%s:%s", window, eventType)
}

func windowTTL(window string) time.Duration {
	switch window {
	case "1m":
		return 2 * time.Minute
	case "5m":
		return 6 * time.Minute
	case "1h":
		return 61 * time.Minute
	default:
		return 5 * time.Minute
	}
}

// WindowStats holds aggregated values for an in-memory window.
type WindowStats = model.WindowStats
```

- [ ] **Step 4: Add WindowStats to model**

Add to `services/analytics-service/internal/model/metrics.go`:

```go
// WindowStats holds in-memory aggregation values per event type per window.
type WindowStats struct {
	Count int64   `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd services/analytics-service && go test ./internal/cache/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add services/analytics-service/internal/cache/ services/analytics-service/internal/model/metrics.go
git commit -m "feat(analytics): add Redis cache for realtime metrics"
```

---

### Task 6: Kafka Publisher

**Files:**
- Create: `services/analytics-service/internal/publisher/publisher.go`

- [ ] **Step 1: Write publisher test**

Create `services/analytics-service/internal/publisher/publisher_test.go`:

```go
package publisher

import (
	"testing"
)

func TestNewPublisher_TopicConfig(t *testing.T) {
	cfg := Config{
		Brokers:        []string{"localhost:9092"},
		ProcessedTopic: "events.processed",
		AnalyticsTopic: "events.analytics",
	}

	if cfg.ProcessedTopic != "events.processed" {
		t.Errorf("ProcessedTopic = %q", cfg.ProcessedTopic)
	}
	if cfg.AnalyticsTopic != "events.analytics" {
		t.Errorf("AnalyticsTopic = %q", cfg.AnalyticsTopic)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/analytics-service && go test ./internal/publisher/ -v`
Expected: FAIL

- [ ] **Step 3: Write publisher implementation**

Create `services/analytics-service/internal/publisher/publisher.go`:

```go
package publisher

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

type Config struct {
	Brokers        []string
	ProcessedTopic string
	AnalyticsTopic string
}

type Publisher struct {
	processed *kafka.Writer
	analytics *kafka.Writer
	logger    *slog.Logger
}

func New(cfg Config, logger *slog.Logger) *Publisher {
	mkWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:                   kafka.TCP(cfg.Brokers...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			BatchSize:              100,
			BatchTimeout:           10 * time.Millisecond,
			RequiredAcks:           kafka.RequireOne,
			WriteTimeout:           5 * time.Second,
			AllowAutoTopicCreation: true,
		}
	}

	logger.Info("Kafka publishers initialized",
		"processed_topic", cfg.ProcessedTopic,
		"analytics_topic", cfg.AnalyticsTopic,
	)

	return &Publisher{
		processed: mkWriter(cfg.ProcessedTopic),
		analytics: mkWriter(cfg.AnalyticsTopic),
		logger:    logger,
	}
}

// PublishProcessed sends a normalized event to events.processed.
func (p *Publisher) PublishProcessed(ctx context.Context, event *model.ProcessedEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = p.processed.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(event.ID),
		Value: data,
		Time:  time.Now(),
	})
	if err != nil {
		p.logger.Error("Failed to publish processed event", "event_id", event.ID, "error", err)
	}
	return err
}

// PublishAnalytics sends an aggregation snapshot to events.analytics.
func (p *Publisher) PublishAnalytics(ctx context.Context, snapshot *model.AggregationSnapshot) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = p.analytics.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(snapshot.EventType),
		Value: data,
		Time:  time.Now(),
	})
	if err != nil {
		p.logger.Error("Failed to publish analytics snapshot", "event_type", snapshot.EventType, "error", err)
	}
	return err
}

func (p *Publisher) Close() error {
	errP := p.processed.Close()
	errA := p.analytics.Close()
	if errP != nil {
		return errP
	}
	return errA
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/analytics-service && go test ./internal/publisher/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/analytics-service/internal/publisher/
git commit -m "feat(analytics): add Kafka publisher for processed and analytics topics"
```

---

### Task 7: Pipeline — Validator Stage

**Files:**
- Create: `services/analytics-service/internal/pipeline/validator.go`

- [ ] **Step 1: Write validator test**

Create `services/analytics-service/internal/pipeline/validator_test.go`:

```go
package pipeline

import (
	"testing"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func TestValidate_ValidEvent(t *testing.T) {
	e := model.ProcessedEvent{
		ID:        "abc-123",
		Type:      "user.click",
		Source:    "web",
		Payload:   map[string]any{"value": 1},
		Timestamp: time.Now(),
	}

	err := validate(e, 24*time.Hour)
	if err != nil {
		t.Errorf("expected valid, got error: %v", err)
	}
}

func TestValidate_MissingID(t *testing.T) {
	e := model.ProcessedEvent{
		Type:      "user.click",
		Source:    "web",
		Payload:   map[string]any{},
		Timestamp: time.Now(),
	}

	err := validate(e, 24*time.Hour)
	if err == nil {
		t.Error("expected error for missing ID")
	}
}

func TestValidate_MissingType(t *testing.T) {
	e := model.ProcessedEvent{
		ID:        "abc-123",
		Source:    "web",
		Payload:   map[string]any{},
		Timestamp: time.Now(),
	}

	err := validate(e, 24*time.Hour)
	if err == nil {
		t.Error("expected error for missing type")
	}
}

func TestValidate_MissingSource(t *testing.T) {
	e := model.ProcessedEvent{
		ID:        "abc-123",
		Type:      "user.click",
		Payload:   map[string]any{},
		Timestamp: time.Now(),
	}

	err := validate(e, 24*time.Hour)
	if err == nil {
		t.Error("expected error for missing source")
	}
}

func TestValidate_TooOld(t *testing.T) {
	e := model.ProcessedEvent{
		ID:        "abc-123",
		Type:      "user.click",
		Source:    "web",
		Payload:   map[string]any{},
		Timestamp: time.Now().Add(-48 * time.Hour),
	}

	err := validate(e, 24*time.Hour)
	if err == nil {
		t.Error("expected error for old event")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/analytics-service && go test ./internal/pipeline/ -v -run TestValidate`
Expected: FAIL

- [ ] **Step 3: Write validator implementation**

Create `services/analytics-service/internal/pipeline/validator.go`:

```go
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func validate(e model.ProcessedEvent, maxAge time.Duration) error {
	if e.ID == "" {
		return fmt.Errorf("missing event ID")
	}
	if e.Type == "" {
		return fmt.Errorf("missing event type")
	}
	if e.Source == "" {
		return fmt.Errorf("missing event source")
	}
	if time.Since(e.Timestamp) > maxAge {
		return fmt.Errorf("event too old: %v", e.Timestamp)
	}
	return nil
}

// runValidator reads from input, validates, and sends valid events to output.
func runValidator(ctx context.Context, input <-chan model.ProcessedEvent, output chan<- model.ProcessedEvent, maxAge time.Duration, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-input:
			if !ok {
				return
			}
			if err := validate(event, maxAge); err != nil {
				logger.Warn("Event validation failed",
					"event_id", event.ID,
					"error", err,
				)
				continue
			}
			select {
			case output <- event:
			case <-ctx.Done():
				return
			}
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/analytics-service && go test ./internal/pipeline/ -v -run TestValidate`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/analytics-service/internal/pipeline/validator.go services/analytics-service/internal/pipeline/validator_test.go
git commit -m "feat(analytics): add pipeline validator stage"
```

---

### Task 8: Pipeline — Transformer Stage

**Files:**
- Create: `services/analytics-service/internal/pipeline/transformer.go`

- [ ] **Step 1: Write transformer test**

Create `services/analytics-service/internal/pipeline/transformer_test.go`:

```go
package pipeline

import (
	"testing"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func TestTransform_NormalizesType(t *testing.T) {
	e := model.ProcessedEvent{
		ID:        "abc-123",
		Type:      "User.CLICK",
		Source:    "web",
		Payload:   map[string]any{"value": 42},
		Timestamp: time.Now(),
	}

	result := transform(e)

	if result.Type != "user.click" {
		t.Errorf("Type = %q, want %q", result.Type, "user.click")
	}
}

func TestTransform_SetsProcessedAt(t *testing.T) {
	e := model.ProcessedEvent{
		ID:        "abc-123",
		Type:      "user.click",
		Source:    "web",
		Payload:   map[string]any{},
		Timestamp: time.Now(),
	}

	before := time.Now()
	result := transform(e)

	if result.ProcessedAt.Before(before) {
		t.Error("ProcessedAt should be set to current time")
	}
}

func TestTransform_ComputesEventSize(t *testing.T) {
	e := model.ProcessedEvent{
		ID:      "abc-123",
		Type:    "user.click",
		Source:  "web",
		Payload: map[string]any{"key": "value"},
	}

	result := transform(e)

	if result.EventSizeBytes <= 0 {
		t.Errorf("EventSizeBytes = %d, want > 0", result.EventSizeBytes)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/analytics-service && go test ./internal/pipeline/ -v -run TestTransform`
Expected: FAIL

- [ ] **Step 3: Write transformer implementation**

Create `services/analytics-service/internal/pipeline/transformer.go`:

```go
package pipeline

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func transform(e model.ProcessedEvent) model.ProcessedEvent {
	e.Type = strings.ToLower(e.Type)
	e.ProcessedAt = time.Now()

	// Compute serialized payload size
	if data, err := json.Marshal(e.Payload); err == nil {
		e.EventSizeBytes = len(data)
	}

	return e
}

// runTransformer reads from input, transforms, and sends to output.
func runTransformer(ctx context.Context, input <-chan model.ProcessedEvent, output chan<- model.ProcessedEvent, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-input:
			if !ok {
				return
			}
			transformed := transform(event)
			logger.Debug("Event transformed",
				"event_id", transformed.ID,
				"type", transformed.Type,
				"size_bytes", transformed.EventSizeBytes,
			)
			select {
			case output <- transformed:
			case <-ctx.Done():
				return
			}
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/analytics-service && go test ./internal/pipeline/ -v -run TestTransform`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/analytics-service/internal/pipeline/transformer.go services/analytics-service/internal/pipeline/transformer_test.go
git commit -m "feat(analytics): add pipeline transformer stage"
```

---

### Task 9: Pipeline — Fan-out Stage

**Files:**
- Create: `services/analytics-service/internal/pipeline/fanout.go`

- [ ] **Step 1: Write fanout test**

Create `services/analytics-service/internal/pipeline/fanout_test.go`:

```go
package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func TestRunFanout_DispatchesToAllChannels(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	input := make(chan model.ProcessedEvent, 1)
	storeCh := make(chan model.ProcessedEvent, 1)
	aggregateCh := make(chan model.ProcessedEvent, 1)
	publishCh := make(chan model.ProcessedEvent, 1)

	go runFanout(ctx, input, storeCh, aggregateCh, publishCh)

	event := model.ProcessedEvent{
		ID:   "test-1",
		Type: "user.click",
	}

	input <- event
	close(input)

	// All three channels should receive the event
	select {
	case e := <-storeCh:
		if e.ID != "test-1" {
			t.Errorf("storeCh got ID = %q", e.ID)
		}
	case <-time.After(time.Second):
		t.Error("storeCh timed out")
	}

	select {
	case e := <-aggregateCh:
		if e.ID != "test-1" {
			t.Errorf("aggregateCh got ID = %q", e.ID)
		}
	case <-time.After(time.Second):
		t.Error("aggregateCh timed out")
	}

	select {
	case e := <-publishCh:
		if e.ID != "test-1" {
			t.Errorf("publishCh got ID = %q", e.ID)
		}
	case <-time.After(time.Second):
		t.Error("publishCh timed out")
	}
}

func TestRunFanout_SkipsFullChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	input := make(chan model.ProcessedEvent, 1)
	storeCh := make(chan model.ProcessedEvent, 1)
	aggregateCh := make(chan model.ProcessedEvent) // unbuffered = will block
	publishCh := make(chan model.ProcessedEvent, 1)

	go runFanout(ctx, input, storeCh, aggregateCh, publishCh)

	event := model.ProcessedEvent{ID: "test-1", Type: "user.click"}
	input <- event
	close(input)

	// storeCh and publishCh should still receive despite aggregateCh being full
	select {
	case <-storeCh:
	case <-time.After(time.Second):
		t.Error("storeCh timed out — fanout should not block")
	}

	select {
	case <-publishCh:
	case <-time.After(time.Second):
		t.Error("publishCh timed out — fanout should not block")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/analytics-service && go test ./internal/pipeline/ -v -run TestRunFanout`
Expected: FAIL

- [ ] **Step 3: Write fanout implementation**

Create `services/analytics-service/internal/pipeline/fanout.go`:

```go
package pipeline

import (
	"context"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

// runFanout reads from input and dispatches to all three output channels.
// Non-blocking sends: if a channel is full, the event is skipped for that channel.
func runFanout(ctx context.Context, input <-chan model.ProcessedEvent, storeCh, aggregateCh, publishCh chan<- model.ProcessedEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-input:
			if !ok {
				return
			}

			// Non-blocking send to store
			select {
			case storeCh <- event:
			default:
			}

			// Non-blocking send to aggregate
			select {
			case aggregateCh <- event:
			default:
			}

			// Non-blocking send to publish
			select {
			case publishCh <- event:
			default:
			}
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/analytics-service && go test ./internal/pipeline/ -v -run TestRunFanout`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/analytics-service/internal/pipeline/fanout.go services/analytics-service/internal/pipeline/fanout_test.go
git commit -m "feat(analytics): add pipeline fanout stage with non-blocking dispatch"
```

---

### Task 10: In-Memory Aggregator

**Files:**
- Create: `services/analytics-service/internal/aggregator/window.go`
- Create: `services/analytics-service/internal/aggregator/flusher.go`

- [ ] **Step 1: Write window aggregator test**

Create `services/analytics-service/internal/aggregator/window_test.go`:

```go
package aggregator

import (
	"testing"
)

func TestWindowAggregator_Add(t *testing.T) {
	w := NewWindowAggregator()

	w.Add("user.click", 10.0)
	w.Add("user.click", 20.0)
	w.Add("order.created", 5.0)

	counters, total := w.SnapshotAndReset()

	if counters["user.click"] != 2 {
		t.Errorf("user.click count = %d, want 2", counters["user.click"])
	}
	if counters["order.created"] != 1 {
		t.Errorf("order.created count = %d, want 1", counters["order.created"])
	}
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
}

func TestWindowAggregator_SnapshotAndReset_Clears(t *testing.T) {
	w := NewWindowAggregator()
	w.Add("user.click", 10.0)

	w.SnapshotAndReset()

	counters, total := w.SnapshotAndReset()
	if total != 0 {
		t.Errorf("total after reset = %d, want 0", total)
	}
	if len(counters) != 0 {
		t.Errorf("counters after reset = %v, want empty", counters)
	}
}

func TestWindowAggregator_WindowStats(t *testing.T) {
	w := NewWindowAggregator()
	w.Add("user.click", 10.0)
	w.Add("user.click", 20.0)
	w.Add("user.click", 5.0)

	stats := w.WindowStatsSnapshot()

	s, ok := stats["user.click"]
	if !ok {
		t.Fatal("user.click stats not found")
	}
	if s.Count != 3 {
		t.Errorf("Count = %d, want 3", s.Count)
	}
	if s.Min != 5.0 {
		t.Errorf("Min = %f, want 5.0", s.Min)
	}
	if s.Max != 20.0 {
		t.Errorf("Max = %f, want 20.0", s.Max)
	}
	if s.Sum != 35.0 {
		t.Errorf("Sum = %f, want 35.0", s.Sum)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/analytics-service && go test ./internal/aggregator/ -v`
Expected: FAIL

- [ ] **Step 3: Write window aggregator**

Create `services/analytics-service/internal/aggregator/window.go`:

```go
package aggregator

import (
	"math"
	"sync"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

type WindowAggregator struct {
	mu       sync.Mutex
	counters map[string]int64
	stats    map[string]*runningStats
	total    int64
}

type runningStats struct {
	count int64
	sum   float64
	min   float64
	max   float64
}

func NewWindowAggregator() *WindowAggregator {
	return &WindowAggregator{
		counters: make(map[string]int64),
		stats:    make(map[string]*runningStats),
	}
}

// Add records an event with an optional numeric value.
func (w *WindowAggregator) Add(eventType string, value float64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.counters[eventType]++
	w.total++

	s, ok := w.stats[eventType]
	if !ok {
		s = &runningStats{min: math.MaxFloat64, max: -math.MaxFloat64}
		w.stats[eventType] = s
	}
	s.count++
	s.sum += value
	if value < s.min {
		s.min = value
	}
	if value > s.max {
		s.max = value
	}
}

// SnapshotAndReset returns current counters and total, then resets.
func (w *WindowAggregator) SnapshotAndReset() (map[string]int64, int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	counters := w.counters
	total := w.total

	w.counters = make(map[string]int64)
	w.stats = make(map[string]*runningStats)
	w.total = 0

	return counters, total
}

// WindowStatsSnapshot returns per-type stats without resetting.
func (w *WindowAggregator) WindowStatsSnapshot() map[string]model.WindowStats {
	w.mu.Lock()
	defer w.mu.Unlock()

	result := make(map[string]model.WindowStats, len(w.stats))
	for k, s := range w.stats {
		minVal := s.min
		if minVal == math.MaxFloat64 {
			minVal = 0
		}
		maxVal := s.max
		if maxVal == -math.MaxFloat64 {
			maxVal = 0
		}
		result[k] = model.WindowStats{
			Count: s.count,
			Sum:   s.sum,
			Min:   minVal,
			Max:   maxVal,
		}
	}
	return result
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/analytics-service && go test ./internal/aggregator/ -v`
Expected: PASS

- [ ] **Step 5: Write flusher**

Create `services/analytics-service/internal/aggregator/flusher.go`:

```go
package aggregator

import (
	"context"
	"log/slog"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/cache"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

type Flusher struct {
	window   *WindowAggregator
	cache    *cache.Cache
	interval time.Duration
	logger   *slog.Logger
}

func NewFlusher(window *WindowAggregator, cache *cache.Cache, interval time.Duration, logger *slog.Logger) *Flusher {
	return &Flusher{
		window:   window,
		cache:    cache,
		interval: interval,
		logger:   logger,
	}
}

// Run starts the periodic flush loop. Blocks until ctx is cancelled.
func (f *Flusher) Run(ctx context.Context) {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush on shutdown
			f.flush(context.Background())
			return
		case <-ticker.C:
			f.flush(ctx)
		}
	}
}

func (f *Flusher) flush(ctx context.Context) {
	counters, total := f.window.SnapshotAndReset()
	if total == 0 {
		return
	}

	if err := f.cache.FlushCounters(ctx, counters, total); err != nil {
		f.logger.Error("Failed to flush counters to Redis", "error", err)
		return
	}

	// Flush window stats for each window size
	stats := f.window.WindowStatsSnapshot()
	for _, w := range []string{"1m", "5m", "1h"} {
		if err := f.cache.FlushWindowStats(ctx, w, stats); err != nil {
			f.logger.Error("Failed to flush window stats", "window", w, "error", err)
		}
	}

	f.logger.Debug("Flushed metrics to Redis",
		"total", total,
		"types", len(counters),
	)
}

// PublishSnapshots creates aggregation snapshots for publishing to Kafka.
func (f *Flusher) BuildSnapshots(counters map[string]int64, total int64) []model.AggregationSnapshot {
	var snapshots []model.AggregationSnapshot
	now := time.Now()

	for eventType, count := range counters {
		snapshots = append(snapshots, model.AggregationSnapshot{
			EventType:  eventType,
			Window:     "1m",
			Timestamp:  now,
			Count:      count,
			RatePerSec: float64(count) / f.interval.Seconds(),
		})
	}
	return snapshots
}
```

- [ ] **Step 6: Commit**

```bash
git add services/analytics-service/internal/aggregator/
git commit -m "feat(analytics): add in-memory window aggregator with Redis flusher"
```

---

### Task 11: Pipeline Orchestrator

**Files:**
- Create: `services/analytics-service/internal/pipeline/pipeline.go`

- [ ] **Step 1: Write pipeline test**

Create `services/analytics-service/internal/pipeline/pipeline_test.go`:

```go
package pipeline

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func TestPipeline_EndToEnd(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := Config{
		ChannelBuffer: 10,
		MaxEventAge:   24 * time.Hour,
	}

	p := New(cfg, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	storeCh, aggregateCh, publishCh := p.Start(ctx)

	// Send a valid event
	p.Send(model.ProcessedEvent{
		ID:        "test-1",
		Type:      "User.Click",
		Source:    "web",
		Payload:   map[string]any{"value": 42},
		Timestamp: time.Now(),
	})

	// All three output channels should receive the transformed event
	select {
	case e := <-storeCh:
		if e.Type != "user.click" {
			t.Errorf("storeCh Type = %q, want %q", e.Type, "user.click")
		}
		if e.ProcessedAt.IsZero() {
			t.Error("ProcessedAt should be set")
		}
	case <-time.After(2 * time.Second):
		t.Error("storeCh timed out")
	}

	select {
	case <-aggregateCh:
	case <-time.After(2 * time.Second):
		t.Error("aggregateCh timed out")
	}

	select {
	case <-publishCh:
	case <-time.After(2 * time.Second):
		t.Error("publishCh timed out")
	}
}

func TestPipeline_DropsInvalidEvent(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := Config{
		ChannelBuffer: 10,
		MaxEventAge:   24 * time.Hour,
	}

	p := New(cfg, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	storeCh, _, _ := p.Start(ctx)

	// Send invalid event (missing ID)
	p.Send(model.ProcessedEvent{
		Type:      "user.click",
		Source:    "web",
		Timestamp: time.Now(),
	})

	select {
	case <-storeCh:
		t.Error("invalid event should not reach storeCh")
	case <-time.After(500 * time.Millisecond):
		// Expected — no event received
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd services/analytics-service && go test ./internal/pipeline/ -v -run TestPipeline`
Expected: FAIL

- [ ] **Step 3: Write pipeline orchestrator**

Create `services/analytics-service/internal/pipeline/pipeline.go`:

```go
package pipeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

type Config struct {
	ChannelBuffer int
	MaxEventAge   time.Duration
}

type Pipeline struct {
	cfg    Config
	logger *slog.Logger
	input  chan model.ProcessedEvent
}

func New(cfg Config, logger *slog.Logger) *Pipeline {
	return &Pipeline{
		cfg:    cfg,
		logger: logger,
		input:  make(chan model.ProcessedEvent, cfg.ChannelBuffer),
	}
}

// Start launches all pipeline stages and returns the three output channels.
func (p *Pipeline) Start(ctx context.Context) (storeCh, aggregateCh, publishCh <-chan model.ProcessedEvent) {
	validCh := make(chan model.ProcessedEvent, p.cfg.ChannelBuffer)
	transformedCh := make(chan model.ProcessedEvent, p.cfg.ChannelBuffer)
	store := make(chan model.ProcessedEvent, p.cfg.ChannelBuffer)
	aggregate := make(chan model.ProcessedEvent, p.cfg.ChannelBuffer)
	publish := make(chan model.ProcessedEvent, p.cfg.ChannelBuffer)

	// Stage 1: Validate
	go runValidator(ctx, p.input, validCh, p.cfg.MaxEventAge, p.logger)

	// Stage 2: Transform
	go runTransformer(ctx, validCh, transformedCh, p.logger)

	// Stage 3: Fan-out
	go runFanout(ctx, transformedCh, store, aggregate, publish)

	p.logger.Info("Pipeline started",
		"buffer", p.cfg.ChannelBuffer,
		"max_event_age", p.cfg.MaxEventAge,
	)

	return store, aggregate, publish
}

// Send pushes an event into the pipeline.
func (p *Pipeline) Send(event model.ProcessedEvent) {
	select {
	case p.input <- event:
	default:
		p.logger.Warn("Pipeline input channel full, dropping event",
			"event_id", event.ID,
		)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd services/analytics-service && go test ./internal/pipeline/ -v`
Expected: PASS (all pipeline tests)

- [ ] **Step 5: Commit**

```bash
git add services/analytics-service/internal/pipeline/pipeline.go services/analytics-service/internal/pipeline/pipeline_test.go
git commit -m "feat(analytics): add pipeline orchestrator wiring all stages"
```

---

### Task 12: Kafka Consumer

**Files:**
- Create: `services/analytics-service/internal/consumer/consumer.go`

- [ ] **Step 1: Write consumer implementation**

Create `services/analytics-service/internal/consumer/consumer.go`:

```go
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/segmentio/kafka-go"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/pipeline"
)

type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

type Consumer struct {
	reader   *kafka.Reader
	pipeline *pipeline.Pipeline
	logger   *slog.Logger
}

func New(cfg Config, p *pipeline.Pipeline, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	logger.Info("Kafka consumer initialized",
		"topic", cfg.Topic,
		"group_id", cfg.GroupID,
	)

	return &Consumer{
		reader:   reader,
		pipeline: p,
		logger:   logger,
	}
}

// Run starts consuming messages and feeding them into the pipeline.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				c.logger.Error("Failed to fetch message", "error", err)
				continue
			}

			var event model.ProcessedEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				c.logger.Error("Failed to unmarshal event",
					"offset", msg.Offset,
					"error", err,
				)
				// Commit bad message to avoid reprocessing
				_ = c.reader.CommitMessages(ctx, msg)
				continue
			}

			c.pipeline.Send(event)

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error("Failed to commit message",
					"offset", msg.Offset,
					"error", err,
				)
			}
		}
	}
}

func (c *Consumer) Close() error {
	c.logger.Info("Closing Kafka consumer")
	return c.reader.Close()
}
```

- [ ] **Step 2: Verify build**

Run: `cd services/analytics-service && go build ./internal/consumer/`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
git add services/analytics-service/internal/consumer/
git commit -m "feat(analytics): add Kafka consumer feeding events into pipeline"
```

---

### Task 13: HTTP Handlers

**Files:**
- Create: `services/analytics-service/internal/handler/handler.go`
- Create: `services/analytics-service/internal/handler/health.go`
- Create: `services/analytics-service/internal/handler/metrics_handler.go`

- [ ] **Step 1: Write handler route setup**

Create `services/analytics-service/internal/handler/handler.go`:

```go
package handler

import (
	"github.com/gin-gonic/gin"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/cache"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/store"
)

type Handler struct {
	store *store.Store
	cache *cache.Cache
}

func New(store *store.Store, cache *cache.Cache) *Handler {
	return &Handler{store: store, cache: cache}
}

func (h *Handler) SetupRoutes(r *gin.Engine) {
	r.GET("/health", h.Health)
	r.GET("/ready", h.Ready)

	api := r.Group("/api/v1")
	{
		api.GET("/metrics/realtime", h.GetRealtimeMetrics)
		api.GET("/metrics/history", h.GetHistoryMetrics)
		api.GET("/stats/:event_type", h.GetEventTypeStats)
	}
}
```

- [ ] **Step 2: Write health handler**

Create `services/analytics-service/internal/handler/health.go`:

```go
package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type HealthStatus struct {
	Status  string            `json:"status"`
	Service string            `json:"service"`
	Checks  map[string]string `json:"checks,omitempty"`
}

func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, HealthStatus{
		Status:  "ok",
		Service: "analytics-service",
	})
}

func (h *Handler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	checks := make(map[string]string)
	allOk := true

	if h.store.Health(ctx) {
		checks["timescaledb"] = "ok"
	} else {
		checks["timescaledb"] = "error"
		allOk = false
	}

	if h.cache.Health(ctx) {
		checks["redis"] = "ok"
	} else {
		checks["redis"] = "error"
		allOk = false
	}

	status := HealthStatus{
		Service: "analytics-service",
		Checks:  checks,
	}

	if !allOk {
		status.Status = "error"
		c.JSON(http.StatusServiceUnavailable, status)
		return
	}

	status.Status = "ok"
	c.JSON(http.StatusOK, status)
}
```

- [ ] **Step 3: Write metrics handler**

Create `services/analytics-service/internal/handler/metrics_handler.go`:

```go
package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func (h *Handler) GetRealtimeMetrics(c *gin.Context) {
	metrics, err := h.cache.GetRealtimeMetrics(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get realtime metrics"})
		return
	}
	c.JSON(http.StatusOK, metrics)
}

func (h *Handler) GetHistoryMetrics(c *gin.Context) {
	fromStr := c.Query("from")
	toStr := c.Query("to")

	if fromStr == "" || toStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "from and to query params are required"})
		return
	}

	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from timestamp, use RFC3339 format"})
		return
	}

	to, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to timestamp, use RFC3339 format"})
		return
	}

	query := model.HistoryQuery{
		From:      from,
		To:        to,
		Interval:  c.Query("interval"),
		EventType: c.Query("event_type"),
	}

	metrics, err := h.store.QueryHistory(c.Request.Context(), query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query history"})
		return
	}
	c.JSON(http.StatusOK, metrics)
}

func (h *Handler) GetEventTypeStats(c *gin.Context) {
	eventType := c.Param("event_type")

	// Realtime stats from Redis
	realtimeStats, err := h.cache.GetEventTypeStats(c.Request.Context(), eventType)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get realtime stats"})
		return
	}

	// Today stats from TimescaleDB
	todayStats, err := h.store.QueryTodayStats(c.Request.Context(), eventType)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get today stats"})
		return
	}

	c.JSON(http.StatusOK, model.EventDetailStats{
		EventType: eventType,
		Realtime:  *realtimeStats,
		Today:     *todayStats,
	})
}
```

- [ ] **Step 4: Verify build**

Run: `cd services/analytics-service && go build ./internal/handler/`
Expected: Build succeeds

- [ ] **Step 5: Commit**

```bash
git add services/analytics-service/internal/handler/
git commit -m "feat(analytics): add HTTP handlers for health and metrics endpoints"
```

---

### Task 14: Main Entry Point — Wire Everything Together

**Files:**
- Modify: `services/analytics-service/cmd/server/main.go`

- [ ] **Step 1: Write the main.go**

Replace `services/analytics-service/cmd/server/main.go` with:

```go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/aggregator"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/cache"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/config"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/consumer"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/handler"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/pipeline"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/publisher"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/store"
)

func main() {
	// 1. Setup logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// 2. Load config
	cfg := config.Load()
	logger.Info("Starting analytics-service",
		"port", cfg.Port,
		"env", cfg.AppEnv,
		"kafka_brokers", cfg.KafkaBrokers,
		"consume_topic", cfg.ConsumeTopic,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Initialize TimescaleDB store
	db, err := store.New(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("Failed to connect to TimescaleDB", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// 4. Initialize Redis cache
	redisCache, err := cache.New(cfg.RedisAddr, cfg.RedisPassword, logger)
	if err != nil {
		logger.Error("Failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	defer redisCache.Close()

	// 5. Initialize Kafka publisher
	pub := publisher.New(publisher.Config{
		Brokers:        cfg.KafkaBrokers,
		ProcessedTopic: "events.processed",
		AnalyticsTopic: "events.analytics",
	}, logger)
	defer pub.Close()

	// 6. Initialize pipeline
	pipe := pipeline.New(pipeline.Config{
		ChannelBuffer: cfg.ChannelBuffer,
		MaxEventAge:   cfg.MaxEventAge,
	}, logger)

	storeCh, aggregateCh, publishCh := pipe.Start(ctx)

	// 7. Initialize aggregator + flusher
	window := aggregator.NewWindowAggregator()
	flusher := aggregator.NewFlusher(window, redisCache, cfg.FlushInterval, logger)

	// 8. Start background workers

	// Store worker: batch insert to TimescaleDB
	go func() {
		var batch []model.ProcessedEvent
		ticker := time.NewTicker(cfg.BatchFlushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				if len(batch) > 0 {
					_ = db.InsertBatch(context.Background(), batch)
				}
				return
			case event, ok := <-storeCh:
				if !ok {
					return
				}
				batch = append(batch, event)
				if len(batch) >= cfg.BatchSize {
					if err := db.InsertBatch(ctx, batch); err != nil {
						logger.Error("Batch insert failed", "error", err)
					}
					batch = batch[:0]
				}
			case <-ticker.C:
				if len(batch) > 0 {
					if err := db.InsertBatch(ctx, batch); err != nil {
						logger.Error("Batch flush failed", "error", err)
					}
					batch = batch[:0]
				}
			}
		}
	}()

	// Aggregate worker: feed in-memory aggregator
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-aggregateCh:
				if !ok {
					return
				}
				value := 0.0
				if v, ok := event.Payload["value"]; ok {
					switch n := v.(type) {
					case float64:
						value = n
					case int:
						value = float64(n)
					}
				}
				window.Add(event.Type, value)
			}
		}
	}()

	// Flusher: periodic Redis flush
	go flusher.Run(ctx)

	// Publish worker: send to Kafka
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-publishCh:
				if !ok {
					return
				}
				if err := pub.PublishProcessed(ctx, &event); err != nil {
					logger.Error("Failed to publish processed event", "error", err)
				}
			}
		}
	}()

	// 9. Initialize Kafka consumer
	cons := consumer.New(consumer.Config{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.ConsumeTopic,
		GroupID: cfg.KafkaGroupID,
	}, pipe, logger)

	go func() {
		if err := cons.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("Consumer error", "error", err)
		}
	}()

	// 10. Setup HTTP server
	if !cfg.IsDevelopment() {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(gin.LoggerWithConfig(gin.LoggerConfig{
		SkipPaths: []string{"/health"},
	}))

	h := handler.New(db, redisCache)
	h.SetupRoutes(router)

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	go func() {
		logger.Info("HTTP server starting", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// 11. Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down analytics-service...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server forced to shutdown", "error", err)
	}

	if err := cons.Close(); err != nil {
		logger.Error("Error closing consumer", "error", err)
	}

	logger.Info("analytics-service exited")
}
```

- [ ] **Step 2: Verify build**

Run: `cd services/analytics-service && go build ./cmd/server/`
Expected: Build succeeds, binary produced

- [ ] **Step 3: Commit**

```bash
git add services/analytics-service/cmd/server/main.go
git commit -m "feat(analytics): wire all components in main entry point"
```

---

### Task 15: Dockerfile

**Files:**
- Create: `services/analytics-service/Dockerfile`

- [ ] **Step 1: Write Dockerfile**

Create `services/analytics-service/Dockerfile`:

```dockerfile
# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copy go workspace files
COPY go.work go.work.sum* ./
COPY services/analytics-service/go.mod services/analytics-service/go.sum ./services/analytics-service/
COPY shared/ ./shared/

RUN cd services/analytics-service && go mod download

COPY services/analytics-service/ ./services/analytics-service/

RUN cd services/analytics-service && CGO_ENABLED=0 GOOS=linux go build -o /analytics-service ./cmd/server/

# Run stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app
COPY --from=builder /analytics-service .

EXPOSE 8080

CMD ["./analytics-service"]
```

- [ ] **Step 2: Commit**

```bash
git add services/analytics-service/Dockerfile
git commit -m "feat(analytics): add multi-stage Dockerfile"
```

---

### Task 16: Update Docker Compose

**Files:**
- Modify: `infra/docker/docker-compose.yml`

- [ ] **Step 1: Update analytics-service in docker-compose**

In `infra/docker/docker-compose.yml`, replace the `analytics-service` service block with the correct environment variables. Find the existing `analytics-service` block and update the `environment` section to include:

```yaml
  analytics-service:
    build:
      context: ../../
      dockerfile: services/analytics-service/Dockerfile
    ports:
      - "8082:8080"
    environment:
      - APP_ENV=development
      - PORT=8080
      - KAFKA_BROKERS=kafka:29092
      - KAFKA_GROUP_ID=analytics-service
      - KAFKA_CONSUME_TOPIC=events.raw
      - DATABASE_URL=postgres://postgres:postgres@postgres:5432/eventdb?sslmode=disable
      - REDIS_ADDR=redis:6379
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    networks:
      - event-platform-network
    restart: unless-stopped
```

- [ ] **Step 2: Verify docker compose config**

Run: `cd infra/docker && docker compose config --services`
Expected: Lists all services including `analytics-service`

- [ ] **Step 3: Commit**

```bash
git add infra/docker/docker-compose.yml
git commit -m "feat(analytics): update docker-compose with analytics-service config"
```

---

### Task 17: Full Build Verification

- [ ] **Step 1: Run all tests**

Run: `cd services/analytics-service && go test ./... -v`
Expected: All tests pass

- [ ] **Step 2: Build binary**

Run: `cd services/analytics-service && go build -o bin/analytics-service ./cmd/server/`
Expected: Binary produced at `bin/analytics-service`

- [ ] **Step 3: Run go vet**

Run: `cd services/analytics-service && go vet ./...`
Expected: No issues

- [ ] **Step 4: Final commit if any fixes needed**

```bash
git add -A services/analytics-service/
git commit -m "fix(analytics): address build/test issues"
```
