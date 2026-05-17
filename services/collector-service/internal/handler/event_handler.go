package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/huynguyenjv/realtime-event-platform/collector-service/internal/model"
)

// IngestEvent handles single event ingestion
func (h *Handler) IngestEvent(c *gin.Context) {
	var event model.RawEvent

	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, model.EventResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	// Generate ID if not provided
	if event.ID == "" {
		event.ID = uuid.New().String()
	}

	// Process event
	processed, err := h.collector.ProcessEvent(c.Request.Context(), &event)
	if err != nil {
		c.JSON(http.StatusInternalServerError, model.EventResponse{
			Success: false,
			EventID: event.ID,
			Error:   err.Error(),
		})
		return
	}

	c.JSON(http.StatusAccepted, model.EventResponse{
		Success: true,
		EventID: processed.ID,
	})
}

// IngestBatch handles batch event ingestion
func (h *Handler) IngestBatch(c *gin.Context) {
	var batch model.BatchRequest

	if err := c.ShouldBindJSON(&batch); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	response := model.BatchResponse{
		Total:   len(batch.Events),
		Results: make([]model.EventResponse, 0, len(batch.Events)),
	}

	for i := range batch.Events {
		event := &batch.Events[i]

		if event.ID == "" {
			event.ID = uuid.New().String()
		}

		processed, err := h.collector.ProcessEvent(c.Request.Context(), event)
		if err != nil {
			response.Failed++
			response.Results = append(response.Results, model.EventResponse{
				Success: false,
				EventID: event.ID,
				Error:   err.Error(),
			})
		} else {
			response.Succeeded++
			response.Results = append(response.Results, model.EventResponse{
				Success: true,
				EventID: processed.ID,
			})
		}
	}

	c.JSON(http.StatusAccepted, response)
}
