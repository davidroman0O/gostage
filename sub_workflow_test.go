package gostage

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// === Sub-workflow per-step resume tracking (Issue 3) ===

func TestSubWorkflow_PerStepResume(t *testing.T) {
	ResetTaskRegistry()

	var step1Count, step2Count, step3Count int32

	Task("sub3.step1", func(ctx *Ctx) error {
		atomic.AddInt32(&step1Count, 1)
		Set(ctx, "sub_step1", true)
		return nil
	})
	Task("sub3.step2", func(ctx *Ctx) error {
		atomic.AddInt32(&step2Count, 1)
		if !IsResuming(ctx) {
			return Suspend(ctx, Params{"at": "sub_step2"})
		}
		Set(ctx, "sub_step2", true)
		return nil
	})
	Task("sub3.step3", func(ctx *Ctx) error {
		atomic.AddInt32(&step3Count, 1)
		Set(ctx, "sub_step3", true)
		return nil
	})

	inner, err := NewWorkflow("sub3-inner").
		Step("sub3.step1").
		Step("sub3.step2").
		Step("sub3.step3").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	outer, err := NewWorkflow("sub3-outer").
		Sub(inner).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithSQLite(t.TempDir() + "/sub3.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// First run: step1 completes, step2 suspends.
	result, err := engine.RunSync(ctx, outer, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s (error: %v)", result.Status, result.Error)
	}

	// On resume, step1 must be skipped (still at count 1).
	outer2, err := NewWorkflow("sub3-outer").
		Sub(inner).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	result, err = engine.Resume(ctx, outer2, result.RunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}

	// step1 must have run exactly once -- skipped on resume.
	if atomic.LoadInt32(&step1Count) != 1 {
		t.Fatalf("step1 should run once (skipped on resume), ran %d times", step1Count)
	}
	// step2 runs twice (first run + resume).
	if atomic.LoadInt32(&step2Count) != 2 {
		t.Fatalf("step2 should run twice, ran %d times", step2Count)
	}
	// step3 runs once (only on resume).
	if atomic.LoadInt32(&step3Count) != 1 {
		t.Fatalf("step3 should run once, ran %d times", step3Count)
	}
}

func TestSubWorkflow_PerStepFlush(t *testing.T) {
	ResetTaskRegistry()

	Task("subf.step1", func(ctx *Ctx) error {
		return Set(ctx, "subf_key", "from_step1")
	})
	Task("subf.step2", func(ctx *Ctx) error {
		// This step reads the value written by step1.
		// If step1's state was not flushed, it would still be in memory.
		// We verify the key exists here; the real flush guarantee is verified
		// by the resume test above (step1's state survives a crash).
		val := Get[string](ctx, "subf_key")
		if val != "from_step1" {
			return fmt.Errorf("expected subf_key='from_step1', got %q", val)
		}
		return Set(ctx, "subf_step2_ran", true)
	})

	inner, err := NewWorkflow("subf-inner").
		Step("subf.step1").
		Step("subf.step2").
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	outer, err := NewWorkflow("subf-outer").Sub(inner).Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithSQLite(t.TempDir() + "/subf.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), outer, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
	if result.Store["subf_step2_ran"] != true {
		t.Fatal("step2 should have run and seen step1's state")
	}
}

// TestSubWorkflow_ForEachResumeNoCollision verifies that per-sub-step tracking
// IDs are scoped to the ForEach item, so item 0 completing its sub-steps does
// not cause item 1's sub-steps to appear already-completed on resume.
func TestSubWorkflow_ForEachResumeNoCollision(t *testing.T) {
	ResetTaskRegistry()

	var step1Runs, step2Runs atomic.Int32

	Task("nocol.step1", func(ctx *Ctx) error {
		step1Runs.Add(1)
		return Set(ctx, "step1_done", true)
	})
	Task("nocol.step2", func(ctx *Ctx) error {
		step2Runs.Add(1)
		if !IsResuming(ctx) {
			return Suspend(ctx, Params{"at": "step2"})
		}
		return Set(ctx, "step2_done", true)
	})

	inner, err := NewWorkflow("nocol-inner").
		Step("nocol.step1").
		Step("nocol.step2").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	// ForEach with 2 items, each item runs the same inner sub-workflow.
	// Sequential (concurrency=1): item 0 runs first, item 1 runs second.
	outer, err := NewWorkflow("nocol-outer").
		ForEach("items", Sub(inner)).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithSQLite(t.TempDir() + "/nocol.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Run 1: item 0 processes step1 + step2 (step2 suspends).
	// Because ForEach is sequential, item 0 runs before item 1 starts.
	result, err := engine.RunSync(ctx, outer, Params{"items": []string{"A", "B"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s (error: %v)", result.Status, result.Error)
	}

	// step1 ran once (for item 0 only), step2 ran once (for item 0, suspended).
	if step1Runs.Load() != 1 {
		t.Fatalf("step1 should have run 1 time before suspension, got %d", step1Runs.Load())
	}
	if step2Runs.Load() != 1 {
		t.Fatalf("step2 should have run 1 time before suspension, got %d", step2Runs.Load())
	}

	// Resume: item 0 re-runs from step2 (step1 skipped -- already completed).
	// Then item 1 runs step1 from scratch -- step1 must NOT be skipped even
	// though item 0's step1 was already written to step_statuses.
	// This is the collision test: if sub-step IDs were not item-scoped,
	// item 1's step1 would be incorrectly skipped.
	outer2, err := NewWorkflow("nocol-outer").
		ForEach("items", Sub(inner)).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	result, err = engine.Resume(ctx, outer2, result.RunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}

	// step1 should have run exactly twice total:
	//   - once for item 0 on first run (count=1 at start of resume = still 1)
	//   - once for item 1 on resume (count goes to 2)
	// If step IDs were not item-scoped, item 1's step1 would see item 0's
	// completed record and skip, leaving step1Runs at 1. That would be the bug.
	if step1Runs.Load() != 2 {
		t.Fatalf("step1 should have run twice total (once per item), got %d -- "+
			"item 1's step1 was incorrectly skipped due to step ID collision", step1Runs.Load())
	}
	// step2 runs twice: item 0 resumes + item 1 runs fresh.
	if step2Runs.Load() != 3 {
		t.Fatalf("step2 should have run 3 times total (item0 initial + item0 resume + item1), got %d", step2Runs.Load())
	}
}
