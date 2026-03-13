package gostage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// === Task 14: Sleep Timer Wake Primary Path ===

func TestSleepTimerWakePrimaryPath(t *testing.T) {
	ResetTaskRegistry()

	Task("stw.after_sleep", func(ctx *Ctx) error {
		Set(ctx, "woken", true)
		return nil
	})

	wf, err := NewWorkflow("sleep-wake").
		Sleep(100 * time.Millisecond).
		Step("stw.after_sleep").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Memory persistence uses blocking sleep (inline time.Sleep),
	// which tests the primary sleep-step-then-continue path.
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	start := time.Now()
	result, err := engine.RunSync(context.Background(), wf, nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
	if result.Store["woken"] != true {
		t.Fatal("expected woken=true in store")
	}
	// Verify that the sleep actually blocked (not skipped)
	if elapsed < 80*time.Millisecond {
		t.Fatalf("expected sleep to block ~100ms, but only took %v", elapsed)
	}
}

func TestSleepPinProtectsCache(t *testing.T) {
	ResetTaskRegistry()

	Task("d9.step_a", func(ctx *Ctx) error {
		Set(ctx, "step_a", true)
		return nil
	})
	Task("d9.step_b", func(ctx *Ctx) error {
		Set(ctx, "step_b", true)
		return nil
	})
	Task("d9.step_c", func(ctx *Ctx) error {
		Set(ctx, "step_c", true)
		return nil
	})

	// wf1 sleeps -- with cacheSize=1, running wf2 would evict wf1 without pinning
	wf1, err := NewWorkflow("pin-wf1").
		Step("d9.step_a").
		Sleep(200 * time.Millisecond).
		Step("d9.step_b").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	wf2, err := NewWorkflow("pin-wf2").
		Step("d9.step_c").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := t.TempDir() + "/pin-test.db"
	engine, err := New(
		WithSQLite(dbPath),
		WithCacheSize(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// RunSync wf1 -- returns Sleeping after step_a (timer-based sleep with SQLite)
	result1, err := engine.RunSync(ctx, wf1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result1.Status != Sleeping {
		t.Fatalf("expected Sleeping, got %s (error: %v)", result1.Status, result1.Error)
	}

	// RunSync wf2 -- without pinning, this would evict wf1 from the size-1 cache
	result2, err := engine.RunSync(ctx, wf2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result2.Status != Completed {
		t.Fatalf("wf2: expected Completed, got %s", result2.Status)
	}

	// Wait for wf1's timer to fire and complete execution
	deadline := time.After(3 * time.Second)
	for {
		run, loadErr := engine.persistence.LoadRun(ctx, result1.RunID)
		if loadErr == nil && (run.Status == Completed || run.Status == Failed) {
			if run.Status != Completed {
				t.Fatalf("wf1 after wake: expected Completed, got %s", run.Status)
			}
			return // success
		}
		select {
		case <-deadline:
			// Check final status
			run, loadErr := engine.persistence.LoadRun(ctx, result1.RunID)
			if loadErr != nil {
				t.Fatalf("timeout: could not load run: %v", loadErr)
			}
			t.Fatalf("timeout: wf1 status is %s (expected Completed)", run.Status)
		case <-time.After(50 * time.Millisecond):
			// poll again
		}
	}
}

// === Decision 010: Final Quality Pass ===

// TestSleepRecoverAcrossRestart verifies that a sleeping workflow can be recovered
// by a NEW engine instance using the persisted workflow definition (no in-memory cache).
func TestSleepRecoverAcrossRestart(t *testing.T) {
	ResetTaskRegistry()

	var postSleepRan int32
	Task("d10.post_sleep", func(ctx *Ctx) error {
		atomic.AddInt32(&postSleepRan, 1)
		Set(ctx, "recovered", true)
		return nil
	})

	wf, err := NewWorkflow("sleep-recover-restart").
		Sleep(100 * time.Millisecond).
		Step("d10.post_sleep").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := t.TempDir() + "/recover-restart.db"
	ctx := context.Background()

	// Engine 1: start the workflow, it enters Sleeping
	engine1, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}

	result1, err := engine1.RunSync(ctx, wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result1.Status != Sleeping {
		t.Fatalf("expected Sleeping, got %s (error: %v)", result1.Status, result1.Error)
	}
	runID := result1.RunID

	// Close engine 1 -- cache is gone
	engine1.Close()

	// Wait for sleep duration to pass
	time.Sleep(150 * time.Millisecond)

	// Engine 2: new instance with same DB, no in-memory cache
	engine2, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()

	// Recover should rebuild workflow from persisted definition and wake it
	if err := engine2.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	// Poll until the post-sleep step completes
	deadline := time.After(3 * time.Second)
	for {
		if atomic.LoadInt32(&postSleepRan) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for recovered sleeping run to complete")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Verify run completed
	run, err := engine2.persistence.LoadRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != Completed {
		t.Fatalf("expected Completed, got %s", run.Status)
	}
}
