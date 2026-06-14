package aggregator

import (
	"testing"
)

func TestWindowAggregator_Add(t *testing.T) {
	w := NewWindowAggregator()

	w.Add("user.click", 10.0)
	w.Add("user.click", 20.0)
	w.Add("order.created", 5.0)

	counters, total := w.SnapshotAndReset()

	if counters["user.click"] != 2 {
		t.Errorf("user.click count = %d, want 2", counters["user.click"])
	}
	if counters["order.created"] != 1 {
		t.Errorf("order.created count = %d, want 1", counters["order.created"])
	}
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
}

func TestWindowAggregator_SnapshotAndReset_Clears(t *testing.T) {
	w := NewWindowAggregator()
	w.Add("user.click", 10.0)

	w.SnapshotAndReset()

	counters, total := w.SnapshotAndReset()
	if total != 0 {
		t.Errorf("total after reset = %d, want 0", total)
	}
	if len(counters) != 0 {
		t.Errorf("counters after reset = %v, want empty", counters)
	}
}

func TestWindowAggregator_WindowStats(t *testing.T) {
	w := NewWindowAggregator()
	w.Add("user.click", 10.0)
	w.Add("user.click", 20.0)
	w.Add("user.click", 5.0)

	stats := w.WindowStatsSnapshot()

	s, ok := stats["user.click"]
	if !ok {
		t.Fatal("user.click stats not found")
	}
	if s.Count != 3 {
		t.Errorf("Count = %d, want 3", s.Count)
	}
	if s.Min != 5.0 {
		t.Errorf("Min = %f, want 5.0", s.Min)
	}
	if s.Max != 20.0 {
		t.Errorf("Max = %f, want 20.0", s.Max)
	}
	if s.Sum != 35.0 {
		t.Errorf("Sum = %f, want 35.0", s.Sum)
	}
}
