package pipeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

type Config struct {
	ChannelBuffer int
	MaxEventAge   time.Duration
}

type Pipeline struct {
	cfg    Config
	logger *slog.Logger
	input  chan model.ProcessedEvent
}

func New(cfg Config, logger *slog.Logger) *Pipeline {
	return &Pipeline{
		cfg:    cfg,
		logger: logger,
		input:  make(chan model.ProcessedEvent, cfg.ChannelBuffer),
	}
}

// Start launches all pipeline stages and returns the three output channels.
func (p *Pipeline) Start(ctx context.Context) (storeCh, aggregateCh, publishCh <-chan model.ProcessedEvent) {
	validCh := make(chan model.ProcessedEvent, p.cfg.ChannelBuffer)
	transformedCh := make(chan model.ProcessedEvent, p.cfg.ChannelBuffer)
	store := make(chan model.ProcessedEvent, p.cfg.ChannelBuffer)
	aggregate := make(chan model.ProcessedEvent, p.cfg.ChannelBuffer)
	publish := make(chan model.ProcessedEvent, p.cfg.ChannelBuffer)

	// Stage 1: Validate
	go runValidator(ctx, p.input, validCh, p.cfg.MaxEventAge, p.logger)

	// Stage 2: Transform
	go runTransformer(ctx, validCh, transformedCh, p.logger)

	// Stage 3: Fan-out
	go runFanout(ctx, transformedCh, store, aggregate, publish)

	p.logger.Info("Pipeline started",
		"buffer", p.cfg.ChannelBuffer,
		"max_event_age", p.cfg.MaxEventAge,
	)

	return store, aggregate, publish
}

// Send pushes an event into the pipeline.
func (p *Pipeline) Send(event model.ProcessedEvent) {
	select {
	case p.input <- event:
	default:
		p.logger.Warn("Pipeline input channel full, dropping event",
			"event_id", event.ID,
		)
	}
}
