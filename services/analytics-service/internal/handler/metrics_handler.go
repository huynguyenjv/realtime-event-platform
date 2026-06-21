package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func (h *Handler) GetRealtimeMetrics(c *gin.Context) {
	metrics, err := h.cache.GetRealtimeMetrics(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get realtime metrics"})
		return
	}
	c.JSON(http.StatusOK, metrics)
}

func (h *Handler) GetHistoryMetrics(c *gin.Context) {
	fromStr := c.Query("from")
	toStr := c.Query("to")

	if fromStr == "" || toStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "from and to query params are required"})
		return
	}

	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from timestamp, use RFC3339 format"})
		return
	}

	to, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to timestamp, use RFC3339 format"})
		return
	}

	query := model.HistoryQuery{
		From:      from,
		To:        to,
		Interval:  c.Query("interval"),
		EventType: c.Query("event_type"),
	}

	metrics, err := h.store.QueryHistory(c.Request.Context(), query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query history"})
		return
	}
	c.JSON(http.StatusOK, metrics)
}

func (h *Handler) GetEventTypeStats(c *gin.Context) {
	eventType := c.Param("event_type")

	// Realtime stats from Redis
	realtimeStats, err := h.cache.GetEventTypeStats(c.Request.Context(), eventType)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get realtime stats"})
		return
	}

	// Today stats from TimescaleDB
	todayStats, err := h.store.QueryTodayStats(c.Request.Context(), eventType)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get today stats"})
		return
	}

	c.JSON(http.StatusOK, model.EventDetailStats{
		EventType: eventType,
		Realtime:  *realtimeStats,
		Today:     *todayStats,
	})
}
