package consumer

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/segmentio/kafka-go"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/pipeline"
)

type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

type Consumer struct {
	reader   *kafka.Reader
	pipeline *pipeline.Pipeline
	logger   *slog.Logger
}

func New(cfg Config, p *pipeline.Pipeline, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	logger.Info("Kafka consumer initialized",
		"topic", cfg.Topic,
		"group_id", cfg.GroupID,
	)

	return &Consumer{
		reader:   reader,
		pipeline: p,
		logger:   logger,
	}
}

// Run starts consuming messages and feeding them into the pipeline.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				c.logger.Error("Failed to fetch message", "error", err)
				continue
			}

			var event model.ProcessedEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				c.logger.Error("Failed to unmarshal event",
					"offset", msg.Offset,
					"error", err,
				)
				// Commit bad message to avoid reprocessing
				_ = c.reader.CommitMessages(ctx, msg)
				continue
			}

			c.pipeline.Send(event)

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error("Failed to commit message",
					"offset", msg.Offset,
					"error", err,
				)
			}
		}
	}
}

func (c *Consumer) Close() error {
	c.logger.Info("Closing Kafka consumer")
	return c.reader.Close()
}
