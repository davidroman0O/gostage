package gostage

import (
	"container/heap"
	"sync"
	"time"
)

// timerScheduler manages sleeping workflows using a min-heap + single timer.
// A single goroutine manages all sleeping workflows efficiently.
// A map index (byRunID) provides O(1) lookup for Schedule and Cancel.
type timerScheduler struct {
	mu      sync.Mutex
	entries timerHeap
	byRunID map[RunID]*timerEntry // O(1) lookup index
	timer   *time.Timer
	wakeFn  func(RunID) // called when a timer fires
	stopCh  chan struct{}
	stopped bool
}

// timerEntry represents a scheduled wake for a sleeping workflow.
type timerEntry struct {
	runID  RunID
	wakeAt time.Time
	index  int // heap index
}

// timerHeap implements heap.Interface for timerEntry.
type timerHeap []*timerEntry

// Len returns the number of entries in the heap. Implements heap.Interface.
func (h timerHeap) Len() int { return len(h) }

// Less reports whether the entry at index i has an earlier wake time than index j. Implements heap.Interface.
func (h timerHeap) Less(i, j int) bool { return h[i].wakeAt.Before(h[j].wakeAt) }

// Swap exchanges the entries at indices i and j and updates their stored heap indices. Implements heap.Interface.
func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

// Push appends a timerEntry to the heap and sets its index. Implements heap.Interface.
func (h *timerHeap) Push(x any) {
	entry := x.(*timerEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}

// Pop removes and returns the entry with the earliest wake time. Implements heap.Interface.
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil // avoid memory leak
	entry.index = -1
	*h = old[:n-1]
	return entry
}

// newTimerScheduler creates a scheduler that calls wakeFn when a timer fires.
func newTimerScheduler(wakeFn func(RunID)) *timerScheduler {
	ts := &timerScheduler{
		wakeFn:  wakeFn,
		byRunID: make(map[RunID]*timerEntry),
		stopCh:  make(chan struct{}),
	}
	heap.Init(&ts.entries)
	return ts
}

// Schedule adds or updates a wake timer for a run.
func (ts *timerScheduler) Schedule(runID RunID, wakeAt time.Time) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.stopped {
		return
	}

	// O(1) lookup via map
	if e, ok := ts.byRunID[runID]; ok {
		e.wakeAt = wakeAt
		heap.Fix(&ts.entries, e.index)
		ts.resetTimerLocked()
		return
	}

	// New entry
	entry := &timerEntry{runID: runID, wakeAt: wakeAt}
	heap.Push(&ts.entries, entry)
	ts.byRunID[runID] = entry
	ts.resetTimerLocked()
}

// Cancel removes a run's wake timer.
func (ts *timerScheduler) Cancel(runID RunID) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if e, ok := ts.byRunID[runID]; ok {
		heap.Remove(&ts.entries, e.index)
		delete(ts.byRunID, runID)
		ts.resetTimerLocked()
	}
}

// Stop shuts down the scheduler.
func (ts *timerScheduler) Stop() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.stopped {
		return
	}
	ts.stopped = true
	close(ts.stopCh)

	if ts.timer != nil {
		ts.timer.Stop()
	}
}

// resetTimerLocked recalculates the next timer fire time. Must hold ts.mu.
func (ts *timerScheduler) resetTimerLocked() {
	if ts.timer != nil {
		ts.timer.Stop()
	}

	if ts.entries.Len() == 0 {
		ts.timer = nil
		return
	}

	next := ts.entries[0]
	delay := time.Until(next.wakeAt)
	if delay < 0 {
		delay = 0
	}

	ts.timer = time.AfterFunc(delay, func() {
		ts.fire()
	})
}

// fire is called when the timer fires. It pops all due entries and dispatches wakes.
func (ts *timerScheduler) fire() {
	ts.mu.Lock()
	if ts.stopped {
		ts.mu.Unlock()
		return
	}

	now := time.Now()
	var toWake []RunID

	for ts.entries.Len() > 0 && !ts.entries[0].wakeAt.After(now) {
		entry := heap.Pop(&ts.entries).(*timerEntry)
		delete(ts.byRunID, entry.runID)
		toWake = append(toWake, entry.runID)
	}

	// Reset timer for next entry
	ts.resetTimerLocked()
	ts.mu.Unlock()

	// Dispatch wakes outside the lock
	for _, runID := range toWake {
		rid := runID
		ts.wakeFn(rid)
	}
}

// Pending returns the number of scheduled wakes.
func (ts *timerScheduler) Pending() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.entries.Len()
}
