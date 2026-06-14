package aggregator

import (
	"math"
	"sync"

	"github.com/huynguyenjv/realtime-event-platform/analytics-service/internal/model"
)

type WindowAggregator struct {
	mu       sync.Mutex
	counters map[string]int64
	stats    map[string]*runningStats
	total    int64
}

type runningStats struct {
	count int64
	sum   float64
	min   float64
	max   float64
}

func NewWindowAggregator() *WindowAggregator {
	return &WindowAggregator{
		counters: make(map[string]int64),
		stats:    make(map[string]*runningStats),
	}
}

// Add records an event with an optional numeric value.
func (w *WindowAggregator) Add(eventType string, value float64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.counters[eventType]++
	w.total++

	s, ok := w.stats[eventType]
	if !ok {
		s = &runningStats{min: math.MaxFloat64, max: -math.MaxFloat64}
		w.stats[eventType] = s
	}
	s.count++
	s.sum += value
	if value < s.min {
		s.min = value
	}
	if value > s.max {
		s.max = value
	}
}

// SnapshotAndReset returns current counters and total, then resets.
func (w *WindowAggregator) SnapshotAndReset() (map[string]int64, int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	counters := w.counters
	total := w.total

	w.counters = make(map[string]int64)
	w.stats = make(map[string]*runningStats)
	w.total = 0

	return counters, total
}

// WindowStatsSnapshot returns per-type stats without resetting.
func (w *WindowAggregator) WindowStatsSnapshot() map[string]model.WindowStats {
	w.mu.Lock()
	defer w.mu.Unlock()

	result := make(map[string]model.WindowStats, len(w.stats))
	for k, s := range w.stats {
		minVal := s.min
		if minVal == math.MaxFloat64 {
			minVal = 0
		}
		maxVal := s.max
		if maxVal == -math.MaxFloat64 {
			maxVal = 0
		}
		result[k] = model.WindowStats{
			Count: s.count,
			Sum:   s.sum,
			Min:   minVal,
			Max:   maxVal,
		}
	}
	return result
}
