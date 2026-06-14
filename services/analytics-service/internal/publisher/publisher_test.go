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
