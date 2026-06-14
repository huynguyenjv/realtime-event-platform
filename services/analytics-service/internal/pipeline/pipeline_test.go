package pipeline

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func TestPipeline_EndToEnd(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := Config{
		ChannelBuffer: 10,
		MaxEventAge:   24 * time.Hour,
	}

	p := New(cfg, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	storeCh, aggregateCh, publishCh := p.Start(ctx)

	// Send a valid event
	p.Send(model.ProcessedEvent{
		ID:        "test-1",
		Type:      "User.Click",
		Source:    "web",
		Payload:   map[string]any{"value": 42},
		Timestamp: time.Now(),
	})

	// All three output channels should receive the transformed event
	select {
	case e := <-storeCh:
		if e.Type != "user.click" {
			t.Errorf("storeCh Type = %q, want %q", e.Type, "user.click")
		}
		if e.ProcessedAt.IsZero() {
			t.Error("ProcessedAt should be set")
		}
	case <-time.After(2 * time.Second):
		t.Error("storeCh timed out")
	}

	select {
	case <-aggregateCh:
	case <-time.After(2 * time.Second):
		t.Error("aggregateCh timed out")
	}

	select {
	case <-publishCh:
	case <-time.After(2 * time.Second):
		t.Error("publishCh timed out")
	}
}

func TestPipeline_DropsInvalidEvent(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := Config{
		ChannelBuffer: 10,
		MaxEventAge:   24 * time.Hour,
	}

	p := New(cfg, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	storeCh, _, _ := p.Start(ctx)

	// Send invalid event (missing ID)
	p.Send(model.ProcessedEvent{
		Type:      "user.click",
		Source:    "web",
		Timestamp: time.Now(),
	})

	select {
	case <-storeCh:
		t.Error("invalid event should not reach storeCh")
	case <-time.After(500 * time.Millisecond):
		// Expected — no event received
	}
}
