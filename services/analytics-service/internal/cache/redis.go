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
