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
