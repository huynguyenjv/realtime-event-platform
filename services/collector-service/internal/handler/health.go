package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type HealthStatus struct {
	Status  string            `json:"status"`
	Service string            `json:"service"`
	Check   map[string]string `json:"check,omitempty"`
}

func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, HealthStatus{
		Status:  "ok",
		Service: "collector-service",
	})
}

func (h *Handler) Ready(c *gin.Context) {
	checks := make(map[string]string)

	if h.collector.IsKafkaHealthy() {
		checks["kafka"] = "ok"
	} else {
		checks["kafka"] = "error"
		c.JSON(http.StatusServiceUnavailable, HealthStatus{
			Status:  "error",
			Service: "collector-service",
			Check:   checks,
		})
	}

	c.JSON(http.StatusOK, HealthStatus{
		Status:  "ok",
		Service: "collector-service",
		Check:   checks,
	})
}
