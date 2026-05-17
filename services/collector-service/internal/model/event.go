package model

import (
	"time"
)

// RawEvent - event từ external source (chưa chuẩn hóa)
type RawEvent struct {
	ID       string            `json:"id,omitempty"`
	Type     string            `json:"type" binding:"required"`
	Source   string            `json:"source" binding:"required"`
	Payload  map[string]any    `json:"payload" binding:"required"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Event - normalized internal event format
type Event struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Source    string            `json:"source"`
	Payload   map[string]any    `json:"payload"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// BatchRequest - for batch ingestion
type BatchRequest struct {
	Events []RawEvent `json:"events" binding:"required,min=1,max=1000"`
}

// EventResponse - API response
type EventResponse struct {
	Success bool   `json:"success"`
	EventID string `json:"event_id,omitempty"`
	Error   string `json:"error,omitempty"`
}

// BatchResponse - batch API response
type BatchResponse struct {
	Total     int             `json:"total"`
	Succeeded int             `json:"succeeded"`
	Failed    int             `json:"failed"`
	Results   []EventResponse `json:"results"`
}
