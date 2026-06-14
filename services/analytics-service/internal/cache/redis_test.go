package cache

import (
	"testing"
)

func TestMetricsKey(t *testing.T) {
	tests := []struct {
		eventType string
		suffix    string
		want      string
	}{
		{"user.click", "count", "metrics:realtime:user.click:count"},
		{"order.created", "rate", "metrics:realtime:order.created:rate"},
	}

	for _, tt := range tests {
		got := metricsKey(tt.eventType, tt.suffix)
		if got != tt.want {
			t.Errorf("metricsKey(%q, %q) = %q, want %q", tt.eventType, tt.suffix, got, tt.want)
		}
	}
}

func TestWindowKey(t *testing.T) {
	got := windowKey("1m", "user.click")
	want := "metrics:window:1m:user.click"
	if got != want {
		t.Errorf("windowKey() = %q, want %q", got, want)
	}
}
