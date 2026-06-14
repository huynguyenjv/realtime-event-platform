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
	EventType string            `json:"event_type"`
	Realtime  RealtimeTypeStats `json:"realtime"`
	Today     TodayStats        `json:"today"`
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

// WindowStats holds in-memory aggregation values per event type per window.
type WindowStats struct {
	Count int64   `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}
