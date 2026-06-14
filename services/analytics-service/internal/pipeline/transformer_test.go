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
