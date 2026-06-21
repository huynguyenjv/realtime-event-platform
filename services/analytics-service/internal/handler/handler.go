package handler

import (
	"github.com/gin-gonic/gin"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/cache"
	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/store"
)

type Handler struct {
	store *store.Store
	cache *cache.Cache
}

func New(store *store.Store, cache *cache.Cache) *Handler {
	return &Handler{store: store, cache: cache}
}

func (h *Handler) SetupRoutes(r *gin.Engine) {
	r.GET("/health", h.Health)
	r.GET("/ready", h.Ready)

	api := r.Group("/api/v1")
	{
		api.GET("/metrics/realtime", h.GetRealtimeMetrics)
		api.GET("/metrics/history", h.GetHistoryMetrics)
		api.GET("/stats/:event_type", h.GetEventTypeStats)
	}
}
