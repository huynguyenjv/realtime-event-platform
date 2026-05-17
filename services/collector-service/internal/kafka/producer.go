package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	logger *slog.Logger
}

type ProducerConfig struct {
	Broker []string
	Topic  string
}

func NewProducer(cfg ProducerConfig, logger *slog.Logger) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Broker...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	logger.Info("Kafka producer initialized",
		"broker", cfg.Broker,
		"topic", cfg.Topic,
	)

	return &Producer{
		writer: writer,
		logger: logger,
	}
}

func (p *Producer) Publish(ctx context.Context, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}

	msg := &kafka.Message{
		Key:   []byte(key),
		Value: data,
		Time:  time.Now(),
	}

	err = p.writer.WriteMessages(ctx, *msg)

	if err != nil {
		p.logger.Error("Failed to publish message",
			"key", key,
			"error", err)
		return err
	}

	p.logger.Debug(" Message published",
		"key", key,
		"size", len(data))

	return nil
}

func (p *Producer) PublishBatch(ctx context.Context, message []kafka.Message) error {
	return p.writer.WriteMessages(ctx, message...)
}

func (p *Producer) Health(ctx context.Context) bool {
	conn, err := kafka.DialContext(ctx, "tcp", p.writer.Addr.String())
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

func (p *Producer) Close() error {
	p.logger.Info("Closing Kafka producer")
	return p.writer.Close()
}
