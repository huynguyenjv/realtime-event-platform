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
