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
