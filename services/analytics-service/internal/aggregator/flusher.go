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

// BuildSnapshots creates aggregation snapshots for publishing to Kafka.
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
