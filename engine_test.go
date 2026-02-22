package gostage

import (
	"context"
	"testing"
	"time"
)

func TestEngine_RunSync_Basic(t *testing.T) {
	ResetTaskRegistry()

	Task("greet", func(ctx *Ctx) error {
		name := GetOr[string](ctx, "user.name", "World")
		Set(ctx, "greeting", "Hello, "+name+"!")
		return nil
	})

	wf := NewWorkflowBuilder("hello").
		Step("greet").
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, P{"user.name": "David"})
	if err != nil {
		t.Fatalf("RunSync failed: %v", err)
	}

	if result.Status != StatusCompletedRun {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Error != nil {
		t.Fatalf("expected no error, got: %v", result.Error)
	}
}

func TestEngine_RunSync_Bail(t *testing.T) {
	ResetTaskRegistry()

	Task("check.age", func(ctx *Ctx) error {
		if Get[int](ctx, "age") < 18 {
			return Bail(ctx, "Must be 18+")
		}
		return nil
	})

	wf := NewWorkflowBuilder("age-check").
		Step("check.age").
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, P{"age": 16})
	if err != nil {
		t.Fatalf("RunSync failed: %v", err)
	}

	if result.Status != StatusBailed {
		t.Fatalf("expected Bailed, got %s", result.Status)
	}
	if result.BailReason != "Must be 18+" {
		t.Fatalf("expected 'Must be 18+', got %q", result.BailReason)
	}
}

func TestEngine_RunSync_Failed(t *testing.T) {
	ResetTaskRegistry()

	Task("fail", func(ctx *Ctx) error {
		return context.DeadlineExceeded
	})

	wf := NewWorkflowBuilder("fail-test").
		Step("fail").
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("RunSync failed: %v", err)
	}

	if result.Status != StatusFailedRun {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
}

func TestEngine_RunAsync_Wait(t *testing.T) {
	ResetTaskRegistry()

	Task("slow.task", func(ctx *Ctx) error {
		time.Sleep(100 * time.Millisecond)
		Set(ctx, "done", true)
		return nil
	})

	wf := NewWorkflowBuilder("async-test").
		Step("slow.task").
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()
	runID, err := engine.Run(ctx, wf, nil)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if runID == "" {
		t.Fatal("expected non-empty RunID")
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	result, err := engine.Wait(waitCtx, runID)
	if err != nil {
		t.Fatalf("Wait failed: %v", err)
	}

	if result.Status != StatusCompletedRun {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
}

func TestEngine_RunSync_MultiStep(t *testing.T) {
	ResetTaskRegistry()

	Task("step.a", func(ctx *Ctx) error {
		Set(ctx, "a", true)
		return nil
	})

	Task("step.b", func(ctx *Ctx) error {
		Set(ctx, "b", true)
		return nil
	})

	Task("step.c", func(ctx *Ctx) error {
		a := Get[bool](ctx, "a")
		b := Get[bool](ctx, "b")
		Set(ctx, "all_done", a && b)
		return nil
	})

	wf := NewWorkflowBuilder("multi-step").
		Step("step.a").
		Step("step.b").
		Step("step.c").
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("RunSync failed: %v", err)
	}

	if result.Status != StatusCompletedRun {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
}

func TestEngine_Persistence_Memory(t *testing.T) {
	ResetTaskRegistry()

	Task("noop", func(ctx *Ctx) error {
		return nil
	})

	wf := NewWorkflowBuilder("persist-test").
		Step("noop").
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	ctx := context.Background()
	result, err := engine.RunSync(ctx, wf, nil)
	if err != nil {
		t.Fatalf("RunSync failed: %v", err)
	}

	// Verify the run is in persistence
	run, err := engine.persistence.LoadRun(ctx, result.RunID)
	if err != nil {
		t.Fatalf("LoadRun failed: %v", err)
	}

	if run.Status != StatusCompletedRun {
		t.Fatalf("expected Completed in persistence, got %s", run.Status)
	}
}

func TestEngine_Suspend(t *testing.T) {
	ResetTaskRegistry()

	Task("approve", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			return nil
		}
		return Suspend(ctx, P{"reason": "needs approval"})
	})

	wf := NewWorkflowBuilder("suspend-test").
		Step("approve").
		Commit()

	engine, err := New()
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("RunSync failed: %v", err)
	}

	if result.Status != StatusSuspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}
}
