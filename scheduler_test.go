package gostage

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// === Timer Scheduler ===

func TestTimerScheduler(t *testing.T) {
	var woken sync.Map

	ts := newTimerScheduler(func(runID RunID) {
		woken.Store(runID, true)
	})
	defer ts.Stop()

	ts.Schedule("run-1", time.Now().Add(50*time.Millisecond))
	ts.Schedule("run-2", time.Now().Add(100*time.Millisecond))

	if ts.Pending() != 2 {
		t.Fatalf("expected 2 pending, got %d", ts.Pending())
	}

	time.Sleep(200 * time.Millisecond)

	if _, ok := woken.Load(RunID("run-1")); !ok {
		t.Fatal("run-1 should have been woken")
	}
	if _, ok := woken.Load(RunID("run-2")); !ok {
		t.Fatal("run-2 should have been woken")
	}
}

func TestTimerCancel(t *testing.T) {
	var woken int32

	ts := newTimerScheduler(func(runID RunID) {
		atomic.AddInt32(&woken, 1)
	})
	defer ts.Stop()

	ts.Schedule("run-cancel", time.Now().Add(100*time.Millisecond))
	ts.Cancel("run-cancel")

	time.Sleep(200 * time.Millisecond)

	if atomic.LoadInt32(&woken) != 0 {
		t.Fatal("cancelled timer should not fire")
	}
}
