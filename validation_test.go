package gostage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestTypedErrors verifies that engine methods return typed sentinel errors
// that callers can check programmatically with errors.Is.
func TestTypedErrors(t *testing.T) {
	ResetTaskRegistry()

	Task("te.task", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			return nil
		}
		return Suspend(ctx, Params{"need": "input"})
	})

	wf, err := NewWorkflow("typed-errors").
		Step("te.task").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Resume a non-existent run -> ErrRunNotFound
	_, err = engine.Resume(context.Background(), wf, "nonexistent-run-id", nil)
	if !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("expected ErrRunNotFound, got: %v", err)
	}

	// Run a workflow that suspends, then test resume errors
	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}

	// Resume with wrong workflow -> ErrWorkflowMismatch
	wrongWf, err := NewWorkflow("wrong-workflow").
		Step("te.task").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	_, err = engine.Resume(context.Background(), wrongWf, result.RunID, nil)
	if !errors.Is(err, ErrWorkflowMismatch) {
		t.Fatalf("expected ErrWorkflowMismatch, got: %v", err)
	}

	// Resume correctly so the run completes
	_, err = engine.Resume(context.Background(), wf, result.RunID, Params{"approved": true})
	if err != nil {
		t.Fatal(err)
	}

	// Resume a completed run -> ErrRunNotSuspended
	_, err = engine.Resume(context.Background(), wf, result.RunID, nil)
	if !errors.Is(err, ErrRunNotSuspended) {
		t.Fatalf("expected ErrRunNotSuspended, got: %v", err)
	}
}

// TestInputValidation verifies that nil workflows are rejected by all engine methods.
func TestInputValidation(t *testing.T) {
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// RunSync with nil workflow
	_, err = engine.RunSync(context.Background(), nil, nil)
	if !errors.Is(err, ErrNilWorkflow) {
		t.Fatalf("RunSync: expected ErrNilWorkflow, got: %v", err)
	}

	// Run with nil workflow
	_, err = engine.Run(context.Background(), nil, nil)
	if !errors.Is(err, ErrNilWorkflow) {
		t.Fatalf("Run: expected ErrNilWorkflow, got: %v", err)
	}

	// Resume with nil workflow
	_, err = engine.Resume(context.Background(), nil, "fake-id", nil)
	if !errors.Is(err, ErrNilWorkflow) {
		t.Fatalf("Resume: expected ErrNilWorkflow, got: %v", err)
	}
}

// TestEmptyNamePanics verifies that empty names are rejected at registration/creation time.
func TestEmptyNamePanics(t *testing.T) {
	ResetTaskRegistry()

	// Task("") should panic
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected Task(\"\") to panic")
			}
			msg := fmt.Sprint(r)
			if !strings.Contains(msg, "name must not be empty") {
				t.Fatalf("expected panic about empty name, got: %s", msg)
			}
		}()
		Task("", func(ctx *Ctx) error { return nil })
	}()

	// NewWorkflow("") should panic
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected NewWorkflow(\"\") to panic")
			}
			msg := fmt.Sprint(r)
			if !strings.Contains(msg, "name must not be empty") {
				t.Fatalf("expected panic about empty name, got: %s", msg)
			}
		}()
		NewWorkflow("")
	}()
}

// TestStateLimit verifies that WithStateLimit rejects new keys when the limit is reached.
func TestStateLimit(t *testing.T) {
	ResetTaskRegistry()

	Task("sl.writer", func(ctx *Ctx) error {
		// Write 3 keys (should succeed with limit=3)
		if err := Set(ctx, "a", 1); err != nil {
			return fmt.Errorf("set a: %w", err)
		}
		if err := Set(ctx, "b", 2); err != nil {
			return fmt.Errorf("set b: %w", err)
		}
		if err := Set(ctx, "c", 3); err != nil {
			return fmt.Errorf("set c: %w", err)
		}
		// 4th new key should fail
		err := Set(ctx, "d", 4)
		if !errors.Is(err, ErrStateLimitExceeded) {
			return fmt.Errorf("expected ErrStateLimitExceeded for 4th key, got: %v", err)
		}
		// Updating existing key should succeed
		if err := Set(ctx, "a", 10); err != nil {
			return fmt.Errorf("update a: %w", err)
		}
		return nil
	})

	wf, err := NewWorkflow("state-limit").
		Step("sl.writer").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithStateLimit(3))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
	if result.Store["a"] != 10 {
		t.Fatalf("expected a=10, got %v", result.Store["a"])
	}
}

// TestShutdownTimeout verifies that WithShutdownTimeout causes Close() to return
// promptly even when a task blocks indefinitely.
func TestShutdownTimeout(t *testing.T) {
	ResetTaskRegistry()

	Task("st.blocker", func(ctx *Ctx) error {
		// Block for a very long time
		time.Sleep(10 * time.Second)
		return nil
	})

	wf, err := NewWorkflow("shutdown-timeout").
		Step("st.blocker").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithShutdownTimeout(200 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	// Start the blocking workflow asynchronously
	_, err = engine.Run(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Give the task time to start executing
	time.Sleep(50 * time.Millisecond)

	// Close should return within the timeout, not block for 10s
	start := time.Now()
	engine.Close()
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Fatalf("Close() took %v, expected ~200ms with shutdown timeout", elapsed)
	}
}

// TestPerTaskTimeout verifies that WithTaskTimeout limits individual task execution time.
func TestPerTaskTimeout(t *testing.T) {
	ResetTaskRegistry()

	Task("ptt.slow", func(ctx *Ctx) error {
		select {
		case <-time.After(5 * time.Second):
			return nil
		case <-ctx.Context().Done():
			return ctx.Context().Err()
		}
	}, WithTaskTimeout(50*time.Millisecond))

	Task("ptt.fast", func(ctx *Ctx) error {
		return nil
	})

	t.Run("task times out", func(t *testing.T) {
		wf, err := NewWorkflow("per-task-timeout").
			Step("ptt.slow").
			Commit()
		if err != nil {
			t.Fatal(err)
		}

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
		if result.Status != Failed && result.Status != Cancelled {
			t.Fatalf("expected Failed or Cancelled, got %s", result.Status)
		}
		// Should fail quickly (~50ms), not after 5s
		if elapsed > 2*time.Second {
			t.Fatalf("task took %v, expected ~50ms timeout", elapsed)
		}
	})

	t.Run("task without timeout runs normally", func(t *testing.T) {
		wf, err := NewWorkflow("no-task-timeout").
			Step("ptt.fast").
			Commit()
		if err != nil {
			t.Fatal(err)
		}

		engine, err := New()
		if err != nil {
			t.Fatal(err)
		}
		defer engine.Close()

		result, err := engine.RunSync(context.Background(), wf, nil)
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != Completed {
			t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
		}
	})
}
