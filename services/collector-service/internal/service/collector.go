package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/collector-service/internal/config"
	"github.com/huynguyenjv/realtime-event-platform/collector-service/internal/kafka"
	"github.com/huynguyenjv/realtime-event-platform/collector-service/internal/model"
)

type Collector struct {
	cfg      *config.Config
	logger   *slog.Logger
	producer *kafka.Producer
}

func NewCollector(cfg *config.Config, logger *slog.Logger) (*Collector, error) {
	producer := kafka.NewProducer(kafka.ProducerConfig{
		Broker: cfg.KafkaBrokers,
		Topic:  cfg.KafkaTopic,
	}, logger)

	return &Collector{
		cfg:      cfg,
		logger:   logger,
		producer: producer,
	}, nil
}

func (c *Collector) ProcessEvent(ctx context.Context, event *model.RawEvent) (*model.Event, error) {
	normalized := c.normalizeEvent(event)

	err := c.producer.Publish(ctx, normalized.ID, normalized)
	if err != nil {
		c.logger.Error(
			"Failed to publish event",
			"event_id", normalized.ID,
			"error", err,
		)
		return nil, err
	}

	c.logger.Info("Event processed",
		"event_id", normalized.ID,
		"error", err,
	)
	return normalized, nil
}

func (c *Collector) normalizeEvent(raw *model.RawEvent) *model.Event {
	return &model.Event{
		ID:        raw.ID,
		Type:      raw.Type,
		Source:    raw.Source,
		Payload:   raw.Payload,
		Metadata:  raw.Metadata,
		Timestamp: time.Now(),
	}
}

func (c *Collector) IsKafkaHealthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.producer.Health(ctx)
}

func (c *Collector) Close() error {
	c.logger.Info("Collector service shutting down")
	return nil
}
