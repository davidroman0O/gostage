package gostage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/store"
)

func TestTaskAndBuilder_SimpleStep(t *testing.T) {
	ResetTaskRegistry()

	Task("greet", func(ctx *Ctx) error {
		name := GetOr[string](ctx, "user.name", "World")
		Set(ctx, "greeting", "Hello, "+name+"!")
		return nil
	})

	wf := NewWorkflowBuilder("hello-world").
		Step("greet").
		Commit()

	if len(wf.Stages) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(wf.Stages))
	}
	if len(wf.Stages[0].Actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(wf.Stages[0].Actions))
	}

	// Execute with the existing runner
	runner := NewRunner()
	wf.Store.Put("user.name", "David")

	err := runner.Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	greeting, _ := GetOrDefault[string](wf.Store, "greeting", "")
	if greeting != "Hello, David!" {
		t.Fatalf("expected 'Hello, David!', got %q", greeting)
	}
}

func TestTaskAndBuilder_MultipleSteps(t *testing.T) {
	ResetTaskRegistry()

	Task("step1", func(ctx *Ctx) error {
		Set(ctx, "count", 1)
		return nil
	})

	Task("step2", func(ctx *Ctx) error {
		count := Get[int](ctx, "count")
		Set(ctx, "count", count+1)
		return nil
	})

	Task("step3", func(ctx *Ctx) error {
		count := Get[int](ctx, "count")
		Set(ctx, "count", count+1)
		return nil
	})

	wf := NewWorkflowBuilder("counter").
		Step("step1").
		Step("step2").
		Step("step3").
		Commit()

	if len(wf.Stages) != 3 {
		t.Fatalf("expected 3 stages, got %d", len(wf.Stages))
	}

	runner := NewRunner()
	err := runner.Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	count, _ := GetOrDefault[int](wf.Store, "count", 0)
	if count != 3 {
		t.Fatalf("expected count=3, got %d", count)
	}
}

func TestTaskAndBuilder_NamedStage(t *testing.T) {
	ResetTaskRegistry()

	Task("validate.input", func(ctx *Ctx) error {
		Set(ctx, "validated", true)
		return nil
	})

	Task("validate.rules", func(ctx *Ctx) error {
		Set(ctx, "rules_checked", true)
		return nil
	})

	wf := NewWorkflowBuilder("pipeline").
		Stage("validation", "validate.input", "validate.rules").
		Commit()

	if len(wf.Stages) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(wf.Stages))
	}
	if len(wf.Stages[0].Actions) != 2 {
		t.Fatalf("expected 2 actions, got %d", len(wf.Stages[0].Actions))
	}

	runner := NewRunner()
	err := runner.Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	validated, _ := GetOrDefault[bool](wf.Store, "validated", false)
	rulesChecked, _ := GetOrDefault[bool](wf.Store, "rules_checked", false)
	if !validated || !rulesChecked {
		t.Fatal("expected both validated and rules_checked to be true")
	}
}

func TestTaskAndBuilder_Branch(t *testing.T) {
	ResetTaskRegistry()

	Task("urgent.process", func(ctx *Ctx) error {
		Set(ctx, "path", "urgent")
		return nil
	})

	Task("normal.process", func(ctx *Ctx) error {
		Set(ctx, "path", "normal")
		return nil
	})

	wf := NewWorkflowBuilder("branching").
		Branch(
			When(func(ctx *Ctx) bool {
				return Get[string](ctx, "priority") == "high"
			}, "urgent.process"),
			Default("normal.process"),
		).
		Commit()

	// Test high priority path
	runner := NewRunner()
	wf.Store.Put("priority", "high")
	err := runner.Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	path, _ := GetOrDefault[string](wf.Store, "path", "")
	if path != "urgent" {
		t.Fatalf("expected 'urgent', got %q", path)
	}
}

func TestTaskAndBuilder_BranchDefault(t *testing.T) {
	ResetTaskRegistry()

	Task("urgent.process", func(ctx *Ctx) error {
		Set(ctx, "path", "urgent")
		return nil
	})

	Task("normal.process", func(ctx *Ctx) error {
		Set(ctx, "path", "normal")
		return nil
	})

	wf := NewWorkflowBuilder("branching-default").
		Branch(
			When(func(ctx *Ctx) bool {
				return Get[string](ctx, "priority") == "high"
			}, "urgent.process"),
			Default("normal.process"),
		).
		Commit()

	// Test default path
	runner := NewRunner()
	wf.Store.Put("priority", "low")
	err := runner.Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	path, _ := GetOrDefault[string](wf.Store, "path", "")
	if path != "normal" {
		t.Fatalf("expected 'normal', got %q", path)
	}
}

func TestTaskAndBuilder_ForEach(t *testing.T) {
	ResetTaskRegistry()

	Task("process.item", func(ctx *Ctx) error {
		item := Item[string](ctx)
		idx := ItemIndex(ctx)
		existing := GetOr[string](ctx, "result", "")
		if existing != "" {
			existing += ","
		}
		Set(ctx, "result", existing+item)
		_ = idx
		return nil
	})

	wf := NewWorkflowBuilder("foreach-test").
		ForEach("items", "process.item").
		Commit()

	runner := NewRunner()
	wf.Store.Put("items", []any{"a", "b", "c"})

	err := runner.Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	result, _ := GetOrDefault[string](wf.Store, "result", "")
	if result != "a,b,c" {
		t.Fatalf("expected 'a,b,c', got %q", result)
	}
}

func TestTaskAndBuilder_Map(t *testing.T) {
	ResetTaskRegistry()

	Task("source", func(ctx *Ctx) error {
		Set(ctx, "raw", "hello world")
		return nil
	})

	Task("consumer", func(ctx *Ctx) error {
		upper := Get[string](ctx, "transformed")
		Set(ctx, "final", upper)
		return nil
	})

	wf := NewWorkflowBuilder("map-test").
		Step("source").
		Map(func(ctx *Ctx) {
			raw := Get[string](ctx, "raw")
			Set(ctx, "transformed", raw+" [mapped]")
		}).
		Step("consumer").
		Commit()

	runner := NewRunner()
	err := runner.Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	final, _ := GetOrDefault[string](wf.Store, "final", "")
	if final != "hello world [mapped]" {
		t.Fatalf("expected 'hello world [mapped]', got %q", final)
	}
}

func TestTaskAndBuilder_DoUntil(t *testing.T) {
	ResetTaskRegistry()

	Task("increment", func(ctx *Ctx) error {
		count := GetOr[int](ctx, "count", 0)
		Set(ctx, "count", count+1)
		return nil
	})

	wf := NewWorkflowBuilder("do-until-test").
		DoUntil("increment", func(ctx *Ctx) bool {
			return Get[int](ctx, "count") >= 5
		}).
		Commit()

	runner := NewRunner()
	err := runner.Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	count, _ := GetOrDefault[int](wf.Store, "count", 0)
	if count != 5 {
		t.Fatalf("expected count=5, got %d", count)
	}
}

func TestTaskAndBuilder_DoWhile(t *testing.T) {
	ResetTaskRegistry()

	Task("decrement", func(ctx *Ctx) error {
		count := GetOr[int](ctx, "count", 10)
		Set(ctx, "count", count-1)
		return nil
	})

	wf := NewWorkflowBuilder("do-while-test").
		DoWhile("decrement", func(ctx *Ctx) bool {
			return Get[int](ctx, "count") > 3
		}).
		Commit()

	runner := NewRunner()
	wf.Store.Put("count", 7)

	err := runner.Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	count, _ := GetOrDefault[int](wf.Store, "count", 0)
	if count != 3 {
		t.Fatalf("expected count=3, got %d", count)
	}
}

func TestTaskAndBuilder_Bail(t *testing.T) {
	ResetTaskRegistry()

	Task("check.age", func(ctx *Ctx) error {
		if Get[int](ctx, "age") < 18 {
			return Bail(ctx, "Must be 18+")
		}
		Set(ctx, "allowed", true)
		return nil
	})

	wf := NewWorkflowBuilder("bail-test").
		Step("check.age").
		Commit()

	runner := NewRunner()
	wf.Store.Put("age", 16)

	err := runner.Execute(context.Background(), wf, nil)
	if err == nil {
		t.Fatal("expected error from bail")
	}

	// Check that it's a bail error wrapped in the action error
	bailErr := &BailError{}
	if !containsBailError(err) {
		t.Fatalf("expected BailError, got: %v", err)
	}
	_ = bailErr
}

func TestTaskAndBuilder_Sub(t *testing.T) {
	ResetTaskRegistry()

	Task("sub.a", func(ctx *Ctx) error {
		Set(ctx, "sub_a", true)
		return nil
	})

	Task("sub.b", func(ctx *Ctx) error {
		Set(ctx, "sub_b", true)
		return nil
	})

	Task("outer.start", func(ctx *Ctx) error {
		Set(ctx, "started", true)
		return nil
	})

	Task("outer.finish", func(ctx *Ctx) error {
		a := Get[bool](ctx, "sub_a")
		b := Get[bool](ctx, "sub_b")
		Set(ctx, "all_done", a && b)
		return nil
	})

	subWf := NewWorkflowBuilder("sub-workflow").
		Step("sub.a").
		Step("sub.b").
		Commit()

	wf := NewWorkflowBuilder("outer-workflow").
		Step("outer.start").
		Sub(subWf).
		Step("outer.finish").
		Commit()

	runner := NewRunner()
	err := runner.Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	allDone, _ := GetOrDefault[bool](wf.Store, "all_done", false)
	if !allDone {
		t.Fatal("expected all_done to be true")
	}
}

func TestTaskAndBuilder_Sleep(t *testing.T) {
	ResetTaskRegistry()

	Task("before", func(ctx *Ctx) error {
		Set(ctx, "step", "before")
		return nil
	})

	Task("after", func(ctx *Ctx) error {
		Set(ctx, "step", "after")
		return nil
	})

	wf := NewWorkflowBuilder("sleep-test").
		Step("before").
		Sleep(10 * time.Millisecond).
		Step("after").
		Commit()

	runner := NewRunner()
	start := time.Now()
	err := runner.Execute(context.Background(), wf, nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	if elapsed < 10*time.Millisecond {
		t.Fatalf("expected at least 10ms, got %v", elapsed)
	}

	step, _ := GetOrDefault[string](wf.Store, "step", "")
	if step != "after" {
		t.Fatalf("expected step='after', got %q", step)
	}
}

func TestTaskAndBuilder_Retry(t *testing.T) {
	ResetTaskRegistry()

	attempts := 0
	Task("flaky", func(ctx *Ctx) error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("temporary failure")
		}
		Set(ctx, "success", true)
		return nil
	}, WithRetry(3))

	wf := NewWorkflowBuilder("retry-test").
		Step("flaky").
		Commit()

	runner := NewRunner()
	err := runner.Execute(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("execution failed: %v", err)
	}

	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}

	success, _ := GetOrDefault[bool](wf.Store, "success", false)
	if !success {
		t.Fatal("expected success to be true")
	}
}

func TestTaskAndBuilder_RetryExhausted(t *testing.T) {
	ResetTaskRegistry()

	Task("always.fail", func(ctx *Ctx) error {
		return fmt.Errorf("persistent failure")
	}, WithRetry(2))

	wf := NewWorkflowBuilder("retry-exhausted").
		Step("always.fail").
		Commit()

	runner := NewRunner()
	err := runner.Execute(context.Background(), wf, nil)
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}
}

func TestTaskAndBuilder_RetryDoesNotRetryBail(t *testing.T) {
	ResetTaskRegistry()

	attempts := 0
	Task("bail.task", func(ctx *Ctx) error {
		attempts++
		return Bail(ctx, "intentional bail")
	}, WithRetry(3))

	wf := NewWorkflowBuilder("bail-no-retry").
		Step("bail.task").
		Commit()

	runner := NewRunner()
	err := runner.Execute(context.Background(), wf, nil)
	if err == nil {
		t.Fatal("expected error from bail")
	}

	// Should only have 1 attempt — bail is not retried
	if attempts != 1 {
		t.Fatalf("expected 1 attempt (bail should not retry), got %d", attempts)
	}
}

func TestIsChild(t *testing.T) {
	// In a normal test, IsChild should return false
	if IsChild() {
		t.Fatal("expected IsChild() to be false in test")
	}
}

// GetOrDefault wraps the store helper for tests.
func GetOrDefault[T any](s interface{ ExportAll() map[string]interface{} }, key string, def T) (T, error) {
	// Use the store package directly
	if kvStore, ok := s.(*store.KVStore); ok {
		return store.GetOrDefault[T](kvStore, key, def)
	}
	return def, nil
}

// containsBailError checks if the error chain contains a BailError.
func containsBailError(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(*BailError); ok {
		return true
	}
	// Check unwrapped errors
	if unwrapped, ok := err.(interface{ Unwrap() error }); ok {
		return containsBailError(unwrapped.Unwrap())
	}
	return false
}
