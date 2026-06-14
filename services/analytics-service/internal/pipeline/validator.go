package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func validate(e model.ProcessedEvent, maxAge time.Duration) error {
	if e.ID == "" {
		return fmt.Errorf("missing event ID")
	}
	if e.Type == "" {
		return fmt.Errorf("missing event type")
	}
	if e.Source == "" {
		return fmt.Errorf("missing event source")
	}
	if time.Since(e.Timestamp) > maxAge {
		return fmt.Errorf("event too old: %v", e.Timestamp)
	}
	return nil
}

// runValidator reads from input, validates, and sends valid events to output.
func runValidator(ctx context.Context, input <-chan model.ProcessedEvent, output chan<- model.ProcessedEvent, maxAge time.Duration, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-input:
			if !ok {
				return
			}
			if err := validate(event, maxAge); err != nil {
				logger.Warn("Event validation failed",
					"event_id", event.ID,
					"error", err,
				)
				continue
			}
			select {
			case output <- event:
			case <-ctx.Done():
				return
			}
		}
	}
}
