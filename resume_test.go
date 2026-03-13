package gostage

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// === Step-Level Resume ===

func TestStepLevelResume(t *testing.T) {
	ResetTaskRegistry()

	var step1Count, step2Count, step3Count int32

	Task("resume.step1", func(ctx *Ctx) error {
		atomic.AddInt32(&step1Count, 1)
		Set(ctx, "step1_done", true)
		return nil
	})
	Task("resume.step2", func(ctx *Ctx) error {
		atomic.AddInt32(&step2Count, 1)
		if !IsResuming(ctx) {
			return Suspend(ctx, Params{"reason": "need approval"})
		}
		Set(ctx, "step2_done", true)
		return nil
	})
	Task("resume.step3", func(ctx *Ctx) error {
		atomic.AddInt32(&step3Count, 1)
		Set(ctx, "step3_done", true)
		return nil
	})

	wf, err := NewWorkflow("step-resume").
		Step("resume.step1").
		Step("resume.step2").
		Step("resume.step3").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// First run: suspends at step2
	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}

	// Resume: step1 should be skipped (already completed), step2 re-executes with resume path
	result, err = engine.Resume(context.Background(), wf, result.RunID, Params{"approved": true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (err: %v)", result.Status, result.Error)
	}

	// step1 should have run exactly once (skipped on resume)
	if atomic.LoadInt32(&step1Count) != 1 {
		t.Fatalf("step1 should run once, ran %d times", step1Count)
	}
	// step2 runs twice (initial + resume)
	if atomic.LoadInt32(&step2Count) != 2 {
		t.Fatalf("step2 should run twice, ran %d times", step2Count)
	}
	// step3 runs once (only on resume)
	if atomic.LoadInt32(&step3Count) != 1 {
		t.Fatalf("step3 should run once, ran %d times", step3Count)
	}
}

func TestForEachPerItemResume(t *testing.T) {
	ResetTaskRegistry()

	var itemRunCount int32

	Task("resume.prepare", func(ctx *Ctx) error {
		Set(ctx, "items", []string{"a", "b", "c"})
		return nil
	})

	Task("resume.process", func(ctx *Ctx) error {
		item := Item[string](ctx)
		idx := ItemIndex(ctx)
		count := atomic.AddInt32(&itemRunCount, 1)
		// Suspend at item "b" on first run (when count is 2, meaning a=1, b=2)
		if item == "b" && !IsResuming(ctx) {
			return Suspend(ctx, Params{"waiting": item})
		}
		Set(ctx, fmt.Sprintf("processed_%d", idx), item)
		_ = count
		return nil
	})

	Task("resume.done", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})

	wf, err := NewWorkflow("foreach-resume").
		Step("resume.prepare").
		ForEach("items", Step("resume.process")).
		Step("resume.done").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithSQLite(":memory:"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// First run: processes "a", suspends at "b"
	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s (err: %v)", result.Status, result.Error)
	}

	// Reset counter to track resume-only executions
	atomic.StoreInt32(&itemRunCount, 0)

	// Resume: "a" should be skipped (completed), "b" re-runs (with resume), "c" runs fresh
	result, err = engine.Resume(context.Background(), wf, result.RunID, Params{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (err: %v)", result.Status, result.Error)
	}

	// On resume: "a" skipped, "b" runs, "c" runs = 2 items processed
	resumeCount := atomic.LoadInt32(&itemRunCount)
	if resumeCount != 2 {
		t.Fatalf("expected 2 items on resume (b,c), got %d", resumeCount)
	}
}

// TestParallelPerRefResume verifies that individual branches within a Parallel
// step that completed before a crash are skipped on resume.
func TestParallelPerRefResume(t *testing.T) {
	ResetTaskRegistry()

	var taskACount, taskBCount, taskCCount int32

	Task("pref.a", func(ctx *Ctx) error {
		atomic.AddInt32(&taskACount, 1)
		Set(ctx, "a_done", true)
		return nil
	})
	Task("pref.b", func(ctx *Ctx) error {
		atomic.AddInt32(&taskBCount, 1)
		if !IsResuming(ctx) {
			return Suspend(ctx, Params{"reason": "wait"})
		}
		Set(ctx, "b_done", true)
		return nil
	})
	Task("pref.c", func(ctx *Ctx) error {
		atomic.AddInt32(&taskCCount, 1)
		Set(ctx, "c_done", true)
		return nil
	})

	// Use in-memory persistence (sufficient — per-ref tracking uses UpdateStepStatus)
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Workflow: parallel with 3 refs, middle one suspends
	// Note: Parallel runs all concurrently, so suspend from one branch propagates
	// We need to test with a Stage (sequential) to demonstrate per-ref skip
	wf, err := NewWorkflow("pref-test").
		Stage("group", Step("pref.a"), Step("pref.b"), Step("pref.c")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	result1, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result1.Status != Suspended {
		t.Fatalf("expected Suspended, got %s (error: %v)", result1.Status, result1.Error)
	}

	// pref.a should have run once, pref.b ran and suspended, pref.c should not run yet
	if atomic.LoadInt32(&taskACount) != 1 {
		t.Fatalf("expected taskA to run once before suspend, got %d", atomic.LoadInt32(&taskACount))
	}
	if atomic.LoadInt32(&taskBCount) != 1 {
		t.Fatalf("expected taskB to run once before suspend, got %d", atomic.LoadInt32(&taskBCount))
	}
	if atomic.LoadInt32(&taskCCount) != 0 {
		t.Fatalf("expected taskC not to run before suspend, got %d", atomic.LoadInt32(&taskCCount))
	}

	// Resume: pref.a should be SKIPPED (per-ref completion), pref.b resumes, pref.c runs
	wf2, err := NewWorkflow("pref-test").
		Stage("group", Step("pref.a"), Step("pref.b"), Step("pref.c")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	result2, err := engine.Resume(context.Background(), wf2, result1.RunID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result2.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result2.Status, result2.Error)
	}

	// pref.a should NOT have run again (per-ref tracking skipped it)
	if atomic.LoadInt32(&taskACount) != 1 {
		t.Fatalf("expected taskA to still be 1 after resume (skipped), got %d", atomic.LoadInt32(&taskACount))
	}
	// pref.b should have run again (resume path)
	if atomic.LoadInt32(&taskBCount) != 2 {
		t.Fatalf("expected taskB to be 2 after resume, got %d", atomic.LoadInt32(&taskBCount))
	}
	// pref.c should have run once
	if atomic.LoadInt32(&taskCCount) != 1 {
		t.Fatalf("expected taskC to be 1 after resume, got %d", atomic.LoadInt32(&taskCCount))
	}
}

// === Task 10: Multiple Suspend-Resume Cycles ===

func TestMultipleSuspendResumeCycles(t *testing.T) {
	ResetTaskRegistry()

	var step1Count, step2Count, step3Count, step4Count int32

	Task("msr.step1", func(ctx *Ctx) error {
		atomic.AddInt32(&step1Count, 1)
		return nil
	})
	// step2 suspends unless resume data has "step2_approved"
	Task("msr.step2", func(ctx *Ctx) error {
		atomic.AddInt32(&step2Count, 1)
		if !ResumeData[bool](ctx, "step2_approved") {
			return Suspend(ctx, Params{"at": "step2"})
		}
		return nil
	})
	// step3 suspends unless resume data has "step3_approved"
	Task("msr.step3", func(ctx *Ctx) error {
		atomic.AddInt32(&step3Count, 1)
		if !ResumeData[bool](ctx, "step3_approved") {
			return Suspend(ctx, Params{"at": "step3"})
		}
		return nil
	})
	Task("msr.step4", func(ctx *Ctx) error {
		atomic.AddInt32(&step4Count, 1)
		Set(ctx, "final", true)
		return nil
	})

	buildWf := func() *Workflow {
		wf, err := NewWorkflow("multi-suspend").
			Step("msr.step1").
			Step("msr.step2").
			Step("msr.step3").
			Step("msr.step4").
			Commit()
		if err != nil {
			t.Fatal(err)
		}
		return wf
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Run 1: suspends at step2 (step2_approved not set)
	result, err := engine.RunSync(context.Background(), buildWf(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("run1: expected Suspended, got %s", result.Status)
	}

	// Resume 1: pass step2_approved=true → step2 passes, step3 suspends
	result, err = engine.Resume(context.Background(), buildWf(), result.RunID, Params{"step2_approved": true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("resume1: expected Suspended, got %s (error: %v)", result.Status, result.Error)
	}

	// Resume 2: pass step3_approved=true → step3 passes, step4 runs, workflow completes
	result, err = engine.Resume(context.Background(), buildWf(), result.RunID, Params{"step3_approved": true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("resume2: expected Completed, got %s (error: %v)", result.Status, result.Error)
	}

	// step1: ran once (skipped on resumes)
	if c := atomic.LoadInt32(&step1Count); c != 1 {
		t.Fatalf("step1 expected 1 run, got %d", c)
	}
	// step2: ran twice (initial suspend + resume1 pass)
	if c := atomic.LoadInt32(&step2Count); c != 2 {
		t.Fatalf("step2 expected 2 runs, got %d", c)
	}
	// step3: ran twice (resume1 suspend + resume2 pass)
	if c := atomic.LoadInt32(&step3Count); c != 2 {
		t.Fatalf("step3 expected 2 runs, got %d", c)
	}
	// step4: ran once (only in resume2)
	if c := atomic.LoadInt32(&step4Count); c != 1 {
		t.Fatalf("step4 expected 1 run, got %d", c)
	}
	if result.Store["final"] != true {
		t.Fatal("expected final=true in store")
	}
}

// TestResumeKeyCleanup verifies that internal __resuming and __resume:* keys
// do not appear in the final Result.Store after a suspend-and-resume cycle.
func TestResumeKeyCleanup(t *testing.T) {
	ResetTaskRegistry()

	Task("rkc.task", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			approved := ResumeData[bool](ctx, "approved")
			Set(ctx, "approved", approved)
			return nil
		}
		Set(ctx, "data", "value")
		return Suspend(ctx, Params{"reason": "need approval"})
	})

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	wf, err := NewWorkflow("resume-key-cleanup").Step("rkc.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	// First run: suspends
	result1, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result1.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result1.Status)
	}

	// Resume with data
	wf2, err := NewWorkflow("resume-key-cleanup").Step("rkc.task").Commit()
	if err != nil {
		t.Fatal(err)
	}
	result2, err := engine.Resume(context.Background(), wf2, result1.RunID, Params{"approved": true})
	if err != nil {
		t.Fatal(err)
	}
	if result2.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result2.Status, result2.Error)
	}

	// Verify no internal keys leak into the result
	for k := range result2.Store {
		if len(k) >= 2 && k[:2] == "__" {
			t.Fatalf("internal key %q leaked into Result.Store", k)
		}
	}

	// Verify user data is preserved
	if result2.Store["data"] != "value" {
		t.Fatalf("expected data=value, got %v", result2.Store["data"])
	}
	if result2.Store["approved"] != true {
		t.Fatalf("expected approved=true, got %v", result2.Store["approved"])
	}
}

func TestConcurrentResumeRace(t *testing.T) {
	ResetTaskRegistry()

	ch := make(chan struct{})
	Task("d8.block_resume", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			<-ch // block until released
			return nil
		}
		return Suspend(ctx, Params{"reason": "wait"})
	})

	wf, _ := NewWorkflow("conc-resume").Step("d8.block_resume").Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// First run → Suspended
	result, _ := engine.RunSync(ctx, wf, nil)
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}

	wf2, _ := NewWorkflow("conc-resume").Step("d8.block_resume").Commit()
	wf3, _ := NewWorkflow("conc-resume").Step("d8.block_resume").Commit()

	// Start first Resume in a goroutine (will block on ch)
	errCh := make(chan error, 1)
	go func() {
		_, err := engine.Resume(ctx, wf2, result.RunID, nil)
		errCh <- err
	}()

	// Give the first Resume a moment to acquire the slot
	time.Sleep(50 * time.Millisecond)

	// Second Resume should get ErrRunAlreadyActive
	_, err = engine.Resume(ctx, wf3, result.RunID, nil)
	if !errors.Is(err, ErrRunAlreadyActive) {
		t.Fatalf("expected ErrRunAlreadyActive, got %v", err)
	}

	// Unblock the first Resume
	close(ch)
	if err := <-errCh; err != nil {
		t.Fatalf("first Resume failed: %v", err)
	}
}

func TestResumeStepCountMismatch(t *testing.T) {
	ResetTaskRegistry()

	callCount := 0
	Task("d8.step_a2", func(ctx *Ctx) error {
		callCount++
		Set(ctx, "a", true)
		return nil
	})
	Task("d8.step_b2", func(ctx *Ctx) error {
		Set(ctx, "b", true)
		return nil
	})
	Task("d8.step_c2", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			return nil
		}
		return Suspend(ctx, Params{"reason": "wait"})
	})

	// 3-step workflow: a → b → c (suspends)
	// After RunSync, steps a and b are completed (2 completed steps)
	wf, _ := NewWorkflow("step-mismatch").
		Step("d8.step_a2").
		Step("d8.step_b2").
		Step("d8.step_c2").
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	result, _ := engine.RunSync(ctx, wf, nil)
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}

	// Resume with a 1-step workflow (fewer steps than the 2 completed) → should fail
	shortWf, _ := NewWorkflow("step-mismatch").Step("d8.step_c2").Commit()
	_, err = engine.Resume(ctx, shortWf, result.RunID, nil)
	if !errors.Is(err, ErrWorkflowMismatch) {
		t.Fatalf("expected ErrWorkflowMismatch, got %v", err)
	}

	// Resume with the full 3-step workflow → should succeed
	wf3, _ := NewWorkflow("step-mismatch").
		Step("d8.step_a2").
		Step("d8.step_b2").
		Step("d8.step_c2").
		Commit()
	resumed, err := engine.Resume(ctx, wf3, result.RunID, nil)
	if err != nil {
		t.Fatalf("Resume with matching workflow failed: %v", err)
	}
	if resumed.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", resumed.Status, resumed.Error)
	}
}
