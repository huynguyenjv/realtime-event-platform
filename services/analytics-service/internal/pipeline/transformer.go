package pipeline

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func transform(e model.ProcessedEvent) model.ProcessedEvent {
	e.Type = strings.ToLower(e.Type)
	e.ProcessedAt = time.Now()

	// Compute serialized payload size
	if data, err := json.Marshal(e.Payload); err == nil {
		e.EventSizeBytes = len(data)
	}

	return e
}

// runTransformer reads from input, transforms, and sends to output.
func runTransformer(ctx context.Context, input <-chan model.ProcessedEvent, output chan<- model.ProcessedEvent, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-input:
			if !ok {
				return
			}
			transformed := transform(event)
			logger.Debug("Event transformed",
				"event_id", transformed.ID,
				"type", transformed.Type,
				"size_bytes", transformed.EventSizeBytes,
			)
			select {
			case output <- transformed:
			case <-ctx.Done():
				return
			}
		}
	}
}
