package pipeline

import (
	"context"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

// runFanout reads from input and dispatches to all three output channels.
// Non-blocking sends: if a channel is full, the event is skipped for that channel.
func runFanout(ctx context.Context, input <-chan model.ProcessedEvent, storeCh, aggregateCh, publishCh chan<- model.ProcessedEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-input:
			if !ok {
				return
			}

			// Non-blocking send to store
			select {
			case storeCh <- event:
			default:
			}

			// Non-blocking send to aggregate
			select {
			case aggregateCh <- event:
			default:
			}

			// Non-blocking send to publish
			select {
			case publishCh <- event:
			default:
			}
		}
	}
}
