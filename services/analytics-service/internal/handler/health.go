package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type HealthStatus struct {
	Status  string            `json:"status"`
	Service string            `json:"service"`
	Checks  map[string]string `json:"checks,omitempty"`
}

func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, HealthStatus{
		Status:  "ok",
		Service: "analytics-service",
	})
}

func (h *Handler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	checks := make(map[string]string)
	allOk := true

	if h.store.Health(ctx) {
		checks["timescaledb"] = "ok"
	} else {
		checks["timescaledb"] = "error"
		allOk = false
	}

	if h.cache.Health(ctx) {
		checks["redis"] = "ok"
	} else {
		checks["redis"] = "error"
		allOk = false
	}

	status := HealthStatus{
		Service: "analytics-service",
		Checks:  checks,
	}

	if !allOk {
		status.Status = "error"
		c.JSON(http.StatusServiceUnavailable, status)
		return
	}

	status.Status = "ok"
	c.JSON(http.StatusOK, status)
}
