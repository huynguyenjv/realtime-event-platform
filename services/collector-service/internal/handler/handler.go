package handler

import (
	"github.com/gin-gonic/gin"
	"github.com/huynguyenjv/realtime-event-platform/collector-service/internal/service"
)

type Handler struct {
	collector *service.Collector
}

func New(collector *service.Collector) *Handler {
	return &Handler{
		collector: collector,
	}
}

func (h *Handler) SetupRoutes(r *gin.Engine) {
	// Health checks
	r.GET("/health", h.Health)
	r.GET("/ready", h.Ready)

	// API v1
	api := r.Group("/api/v1")
	{
		api.POST("/events", h.IngestEvent)
		api.POST("/events/batch", h.IngestBatch)
	}
}
