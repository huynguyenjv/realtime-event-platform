package publisher

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

type Config struct {
	Brokers        []string
	ProcessedTopic string
	AnalyticsTopic string
}

type Publisher struct {
	processed *kafka.Writer
	analytics *kafka.Writer
	logger    *slog.Logger
}

func New(cfg Config, logger *slog.Logger) *Publisher {
	mkWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:                   kafka.TCP(cfg.Brokers...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			BatchSize:              100,
			BatchTimeout:           10 * time.Millisecond,
			RequiredAcks:           kafka.RequireOne,
			WriteTimeout:           5 * time.Second,
			AllowAutoTopicCreation: true,
		}
	}

	logger.Info("Kafka publishers initialized",
		"processed_topic", cfg.ProcessedTopic,
		"analytics_topic", cfg.AnalyticsTopic,
	)

	return &Publisher{
		processed: mkWriter(cfg.ProcessedTopic),
		analytics: mkWriter(cfg.AnalyticsTopic),
		logger:    logger,
	}
}

// PublishProcessed sends a normalized event to events.processed.
func (p *Publisher) PublishProcessed(ctx context.Context, event *model.ProcessedEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = p.processed.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(event.ID),
		Value: data,
		Time:  time.Now(),
	})
	if err != nil {
		p.logger.Error("Failed to publish processed event", "event_id", event.ID, "error", err)
	}
	return err
}

// PublishAnalytics sends an aggregation snapshot to events.analytics.
func (p *Publisher) PublishAnalytics(ctx context.Context, snapshot *model.AggregationSnapshot) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err = p.analytics.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(snapshot.EventType),
		Value: data,
		Time:  time.Now(),
	})
	if err != nil {
		p.logger.Error("Failed to publish analytics snapshot", "event_type", snapshot.EventType, "error", err)
	}
	return err
}

func (p *Publisher) Close() error {
	errP := p.processed.Close()
	errA := p.analytics.Close()
	if errP != nil {
		return errP
	}
	return errA
}
