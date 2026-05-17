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
	"github.com/huynguyenjv/realtime-event-platform/collector-service/internal/config"
	"github.com/huynguyenjv/realtime-event-platform/collector-service/internal/handler"
	"github.com/huynguyenjv/realtime-event-platform/collector-service/internal/service"
)

func main() {
	// 1. Setup logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// 2. Load config
	cfg := config.Load()
	logger.Info("Starting collector-service",
		"port", cfg.Port,
		"env", cfg.AppEnv,
	)

	// 3. Initialize collector service
	collector, err := service.NewCollector(cfg, logger)
	if err != nil {
		logger.Error("Failed to create collector", "error", err)
		os.Exit(1)
	}

	// 4. Setup HTTP server
	if !cfg.IsDevelopment() {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	router.Use(gin.Recovery())

	// 5. Setup handlers
	h := handler.New(collector)
	h.SetupRoutes(router)

	// 6. Start server
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

	// 7. Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("Server forced to shutdown", "error", err)
	}

	if err := collector.Close(); err != nil {
		logger.Error("Error closing collector", "error", err)
	}

	logger.Info("Server exited")
}
