package contracts

import "time"

type Event struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Source    string            `json:"source"`
	Payload   map[string]any    `json:"payload"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

type EventBatch struct {
	Events []Event `json:"events"`
}

type KafkaTopics struct {
	RawEvents       string
	ProcessedEvents string
	Predictions     string
	Alerts          string
	Analytics       string
}

var DefaultTopics = KafkaTopics{
	RawEvents:       "events.raw",
	ProcessedEvents: "events.processed",
	Predictions:     "events.predictions",
	Alerts:          "events.alerts",
	Analytics:       "events.analytics",
}
