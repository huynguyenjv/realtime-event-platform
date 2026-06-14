package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func TestRunFanout_DispatchesToAllChannels(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	input := make(chan model.ProcessedEvent, 1)
	storeCh := make(chan model.ProcessedEvent, 1)
	aggregateCh := make(chan model.ProcessedEvent, 1)
	publishCh := make(chan model.ProcessedEvent, 1)

	go runFanout(ctx, input, storeCh, aggregateCh, publishCh)

	event := model.ProcessedEvent{
		ID:   "test-1",
		Type: "user.click",
	}

	input <- event
	close(input)

	// All three channels should receive the event
	select {
	case e := <-storeCh:
		if e.ID != "test-1" {
			t.Errorf("storeCh got ID = %q", e.ID)
		}
	case <-time.After(time.Second):
		t.Error("storeCh timed out")
	}

	select {
	case e := <-aggregateCh:
		if e.ID != "test-1" {
			t.Errorf("aggregateCh got ID = %q", e.ID)
		}
	case <-time.After(time.Second):
		t.Error("aggregateCh timed out")
	}

	select {
	case e := <-publishCh:
		if e.ID != "test-1" {
			t.Errorf("publishCh got ID = %q", e.ID)
		}
	case <-time.After(time.Second):
		t.Error("publishCh timed out")
	}
}

func TestRunFanout_SkipsFullChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	input := make(chan model.ProcessedEvent, 1)
	storeCh := make(chan model.ProcessedEvent, 1)
	aggregateCh := make(chan model.ProcessedEvent) // unbuffered = will block
	publishCh := make(chan model.ProcessedEvent, 1)

	go runFanout(ctx, input, storeCh, aggregateCh, publishCh)

	event := model.ProcessedEvent{ID: "test-1", Type: "user.click"}
	input <- event
	close(input)

	// storeCh and publishCh should still receive despite aggregateCh being full
	select {
	case <-storeCh:
	case <-time.After(time.Second):
		t.Error("storeCh timed out — fanout should not block")
	}

	select {
	case <-publishCh:
	case <-time.After(time.Second):
		t.Error("publishCh timed out — fanout should not block")
	}
}
