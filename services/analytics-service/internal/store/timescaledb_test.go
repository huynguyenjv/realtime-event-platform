package store

import (
	"testing"
	"time"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

func TestAutoSelectInterval(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{"under 1h uses 1m", 30 * time.Minute, "1m"},
		{"under 24h uses 5m", 12 * time.Hour, "5m"},
		{"over 24h uses 1h", 48 * time.Hour, "1h"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := model.HistoryQuery{
				From: time.Now().Add(-tt.duration),
				To:   time.Now(),
			}
			got := autoSelectInterval(q)
			if got != tt.want {
				t.Errorf("autoSelectInterval() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIntervalToView(t *testing.T) {
	tests := []struct {
		interval string
		want     string
	}{
		{"1m", "event_metrics_1m"},
		{"5m", "event_metrics_5m"},
		{"1h", "event_metrics_1h"},
		{"unknown", "event_metrics_1m"},
	}

	for _, tt := range tests {
		t.Run(tt.interval, func(t *testing.T) {
			got := intervalToView(tt.interval)
			if got != tt.want {
				t.Errorf("intervalToView(%q) = %q, want %q", tt.interval, got, tt.want)
			}
		})
	}
}
