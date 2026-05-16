package service

import (
	"log/slog"

	"github.com/huynguyenjv/realtime-event-platform/collector-service/internal/config"
)

type Collector struct {
	cfg    *config.Config
	logger *slog.Logger
}

func NewCollector(cfg *config.Config, logger *slog.Logger) *Collector {
	return &Collector{
		cfg:    cfg,
		logger: logger,
	}
}

func (c *Collector) IsKafkaHealthy() bool {
	return true
}

func (c *Collector) Close() error {
	c.logger.Info("Collector service shutting down")
	return nil
}
