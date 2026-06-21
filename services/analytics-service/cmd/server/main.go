package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/aggregator"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/cache"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/config"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/consumer"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/handler"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/pipeline"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/publisher"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/store"
)

func main() {
	// 1. Setup logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// 2. Load config
	cfg := config.Load()
	logger.Info("Starting analytics-service",
		"port", cfg.Port,
		"env", cfg.AppEnv,
		"kafka_brokers", cfg.KafkaBrokers,
		"consume_topic", cfg.ConsumeTopic,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Initialize TimescaleDB store
	db, err := store.New(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("Failed to connect to TimescaleDB", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// 4. Initialize Redis cache
	redisCache, err := cache.New(cfg.RedisAddr, cfg.RedisPassword, logger)
	if err != nil {
		logger.Error("Failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	defer redisCache.Close()

	// 5. Initialize Kafka publisher
	pub := publisher.New(publisher.Config{
		Brokers:        cfg.KafkaBrokers,
		ProcessedTopic: "events.processed",
		AnalyticsTopic: "events.analytics",
	}, logger)
	defer pub.Close()

	// 6. Initialize pipeline
	pipe := pipeline.New(pipeline.Config{
		ChannelBuffer: cfg.ChannelBuffer,
		MaxEventAge:   cfg.MaxEventAge,
	}, logger)

	storeCh, aggregateCh, publishCh := pipe.Start(ctx)

	// 7. Initialize aggregator + flusher
	window := aggregator.NewWindowAggregator()
	flusher := aggregator.NewFlusher(window, redisCache, cfg.FlushInterval, logger)

	// 8. Start background workers

	// Store worker: batch insert to TimescaleDB
	go func() {
		var batch []model.ProcessedEvent
		ticker := time.NewTicker(cfg.BatchFlushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				if len(batch) > 0 {
					_ = db.InsertBatch(context.Background(), batch)
				}
				return
			case event, ok := <-storeCh:
				if !ok {
					return
				}
				batch = append(batch, event)
				if len(batch) >= cfg.BatchSize {
					if err := db.InsertBatch(ctx, batch); err != nil {
						logger.Error("Batch insert failed", "error", err)
					}
					batch = batch[:0]
				}
			case <-ticker.C:
				if len(batch) > 0 {
					if err := db.InsertBatch(ctx, batch); err != nil {
						logger.Error("Batch flush failed", "error", err)
					}
					batch = batch[:0]
				}
			}
		}
	}()

	// Aggregate worker: feed in-memory aggregator
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-aggregateCh:
				if !ok {
					return
				}
				value := 0.0
				if v, ok := event.Payload["value"]; ok {
					switch n := v.(type) {
					case float64:
						value = n
					case int:
						value = float64(n)
					}
				}
				window.Add(event.Type, value)
			}
		}
	}()

	// Flusher: periodic Redis flush
	go flusher.Run(ctx)

	// Publish worker: send to Kafka
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-publishCh:
				if !ok {
					return
				}
				if err := pub.PublishProcessed(ctx, &event); err != nil {
					logger.Error("Failed to publish processed event", "error", err)
				}
			}
		}
	}()

	// 9. Initialize Kafka consumer
	cons := consumer.New(consumer.Config{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.ConsumeTopic,
		GroupID: cfg.KafkaGroupID,
	}, pipe, logger)

	go func() {
		if err := cons.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("Consumer error", "error", err)
		}
	}()

	// 10. Setup HTTP server
	if !cfg.IsDevelopment() {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(gin.LoggerWithConfig(gin.LoggerConfig{
		SkipPaths: []string{"/health"},
	}))

	h := handler.New(db, redisCache)
	h.SetupRoutes(router)

	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	go func() {
		logger.Info("HTTP server starting", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// 11. Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down analytics-service...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server forced to shutdown", "error", err)
	}

	if err := cons.Close(); err != nil {
		logger.Error("Error closing consumer", "error", err)
	}

	logger.Info("analytics-service exited")
}
