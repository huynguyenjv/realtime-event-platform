package kafka

import (
	"context"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	logger *slog.Logger
}

type ConsumerConfig struct {
	Brokers []string
	Topic   string
	GroupID string
}

type MessageHandler func(ctx context.Context, msg kafka.Message) error

func NewConsumer(cfg ConsumerConfig, logger *slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	return &Consumer{reader: reader, logger: logger}
}

func (c *Consumer) Consume(ctx context.Context, handler MessageHandler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				c.logger.Error("failed to fetch message", "error", err)
				continue
			}

			if err := handler(ctx, msg); err != nil {
				c.logger.Error("failed to handle message", "error", err, "offset", msg.Offset)
				continue
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error("failed to commit message", "error", err)
			}
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
