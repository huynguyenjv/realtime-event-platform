package validator

import (
	"errors"
	"strings"

	"github.com/huynguyenjv/realtime-event-platform/collector-service/internal/model"
)

var (
	ErrEmptyType    = errors.New("event type is required")
	ErrEmptySource  = errors.New("event source is required")
	ErrEmptyPayload = errors.New("event payload is required")
	ErrInvalidType  = errors.New("invalid event type")
)

// AllowedEventTypes - whitelist of valid event types
var AllowedEventTypes = map[string]bool{
	"game_result":  true,
	"user_action":  true,
	"system_event": true,
	"transaction":  true,
	"notification": true,
	"custom":       true,
}

type Validator struct{}

func New() *Validator {
	return &Validator{}
}

func (v *Validator) ValidateEvent(event *model.RawEvent) error {
	// Check required fields
	if strings.TrimSpace(event.Type) == "" {
		return ErrEmptyType
	}

	if strings.TrimSpace(event.Source) == "" {
		return ErrEmptySource
	}

	if event.Payload == nil || len(event.Payload) == 0 {
		return ErrEmptyPayload
	}

	// Validate event type (optional - có thể bỏ nếu muốn flexible)
	// if !AllowedEventTypes[event.Type] {
	// 	return ErrInvalidType
	// }

	return nil
}
