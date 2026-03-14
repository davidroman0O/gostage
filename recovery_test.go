package gostage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// === Startup Recovery ===

func TestRecoverCrashedRuns(t *testing.T) {
	ResetTaskRegistry()

	Task("recover.noop", func(ctx *Ctx) error { return nil })

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Simulate a crashed run (status = Running)
	crashedRun := &RunState{
		RunID:      "crashed-run",
		WorkflowID: "test-wf",
		Status:     Running,
		StepStates: make(map[string]Status),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	engine.persistence.SaveRun(ctx, crashedRun)

	// Run recovery
	if err := engine.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	// Check it was marked as Failed
	run, err := engine.persistence.LoadRun(ctx, "crashed-run")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != Failed {
		t.Fatalf("expected Failed, got %s", run.Status)
	}

	engine.Close()
}

func TestRecoverSleepingPastDue(t *testing.T) {
	ResetTaskRegistry()

	var woken int32
	Task("recover.wake", func(ctx *Ctx) error {
		atomic.StoreInt32(&woken, 1)
		Set(ctx, "woken", true)
		return nil
	})

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Cache a workflow for the wake function to find
	wf, err := NewWorkflow("recover-wake").Step("recover.wake").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine.cacheWorkflow(wf)

	// Simulate a sleeping run whose wake time has passed
	sleepingRun := &RunState{
		RunID:      "sleep-past",
		WorkflowID: "recover-wake",
		Status:     Sleeping,
		StepStates: make(map[string]Status),
		WakeAt:     time.Now().Add(-1 * time.Hour), // past due
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	engine.persistence.SaveRun(ctx, sleepingRun)

	// Save empty state for the sleeping run (so LoadFromPersistence works)
	engine.persistence.SaveState(ctx, "sleep-past", map[string]StateEntry{})

	// Run recovery
	engine.Recover(ctx)

	// Poll until the wake completes (max 2 seconds)
	deadline := time.After(2 * time.Second)
	for {
		if atomic.LoadInt32(&woken) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for sleeping run to be woken")
		case <-time.After(10 * time.Millisecond):
		}
	}

	// Give a moment for status to be persisted
	time.Sleep(50 * time.Millisecond)

	// Verify the run was processed
	run, err := engine.persistence.LoadRun(ctx, "sleep-past")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status == Sleeping {
		t.Fatal("sleeping run should have been woken")
	}

	engine.Close()
}
