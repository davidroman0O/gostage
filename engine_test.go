package gostage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestEngine_RunSync_Simple(t *testing.T) {
	ResetTaskRegistry()

	Task("e.greet", func(ctx *Ctx) error {
		name := GetOr[string](ctx, "name", "World")
		Set(ctx, "greeting", "Hello, "+name+"!")
		return nil
	})

	wf, err := NewWorkflow("greet").Step("e.greet").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, Params{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["greeting"] != "Hello, Alice!" {
		t.Fatalf("expected 'Hello, Alice!', got %v", result.Store["greeting"])
	}
}

func TestEngine_RunSync_MultiStep(t *testing.T) {
	ResetTaskRegistry()

	Task("e.step1", func(ctx *Ctx) error {
		Set(ctx, "step1", true)
		return nil
	})
	Task("e.step2", func(ctx *Ctx) error {
		Set(ctx, "step2", true)
		return nil
	})
	Task("e.step3", func(ctx *Ctx) error {
		s1 := Get[bool](ctx, "step1")
		s2 := Get[bool](ctx, "step2")
		Set(ctx, "all_done", s1 && s2)
		return nil
	})

	wf, err := NewWorkflow("multi").
		Step("e.step1").
		Step("e.step2").
		Step("e.step3").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["all_done"] != true {
		t.Fatal("expected all_done to be true")
	}
}

func TestEngine_RunSync_Bail(t *testing.T) {
	ResetTaskRegistry()

	Task("e.check_age", func(ctx *Ctx) error {
		age := Get[int](ctx, "age")
		if age < 18 {
			return Bail(ctx, "Must be 18+")
		}
		return nil
	})

	wf, err := NewWorkflow("bail").Step("e.check_age").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, Params{"age": 16})
	if result.Status != Bailed {
		t.Fatalf("expected Bailed, got %s", result.Status)
	}
	if result.BailReason != "Must be 18+" {
		t.Fatalf("expected 'Must be 18+', got %q", result.BailReason)
	}
}

func TestEngine_RunSync_Suspend(t *testing.T) {
	ResetTaskRegistry()

	Task("e.approve", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			return nil
		}
		return Suspend(ctx, Params{"reason": "needs approval"})
	})

	wf, err := NewWorkflow("suspend").Step("e.approve").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}
}

func TestEngine_RunSync_Failed(t *testing.T) {
	ResetTaskRegistry()

	Task("e.fail", func(ctx *Ctx) error {
		return errors.New("something broke")
	})

	wf, err := NewWorkflow("fail").Step("e.fail").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
}

func TestEngine_RunAndWait(t *testing.T) {
	ResetTaskRegistry()

	Task("e.async", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})

	wf, err := NewWorkflow("async").Step("e.async").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := New()
	defer engine.Close()

	runID, err := engine.Run(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	result, err := engine.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
}

func TestEngine_Cancel(t *testing.T) {
	ResetTaskRegistry()

	Task("e.slow", func(ctx *Ctx) error {
		select {
		case <-time.After(10 * time.Second):
			return nil
		case <-ctx.Context().Done():
			return ctx.Context().Err()
		}
	})

	wf, err := NewWorkflow("cancel").Step("e.slow").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := New()
	defer engine.Close()

	runID, _ := engine.Run(context.Background(), wf, nil)

	// Give it time to start
	time.Sleep(50 * time.Millisecond)

	if err := engine.Cancel(context.Background(), runID); err != nil {
		t.Fatal(err)
	}

	result, _ := engine.Wait(context.Background(), runID)
	if result.Status != Cancelled {
		t.Fatalf("expected Cancelled, got %s", result.Status)
	}
}

func TestEngine_WithTimeout(t *testing.T) {
	ResetTaskRegistry()

	Task("e.timeout", func(ctx *Ctx) error {
		select {
		case <-time.After(5 * time.Second):
			return nil
		case <-ctx.Context().Done():
			return ctx.Context().Err()
		}
	})

	wf, err := NewWorkflow("timeout").Step("e.timeout").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := New(WithTimeout(100 * time.Millisecond))
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Cancelled {
		t.Fatalf("expected Cancelled (from timeout), got %s", result.Status)
	}
}

func TestEngine_Parallel_RealConcurrency(t *testing.T) {
	ResetTaskRegistry()

	var running int64

	Task("e.par_task", func(ctx *Ctx) error {
		current := atomic.AddInt64(&running, 1)
		if current > 1 {
			Set(ctx, "was_concurrent", true)
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt64(&running, -1)
		return nil
	})

	wf, err := NewWorkflow("par").
		Parallel(Step("e.par_task"), Step("e.par_task"), Step("e.par_task")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["was_concurrent"] != true {
		t.Fatal("expected tasks to run concurrently")
	}
}

func TestEngine_Parallel_FirstError(t *testing.T) {
	ResetTaskRegistry()

	Task("e.par_ok", func(ctx *Ctx) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})
	Task("e.par_fail", func(ctx *Ctx) error {
		return errors.New("parallel failure")
	})

	wf, err := NewWorkflow("par-fail").
		Parallel(Step("e.par_ok"), Step("e.par_fail")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
}

func TestEngine_ForEach_Sequential(t *testing.T) {
	ResetTaskRegistry()

	Task("e.fe_task", func(ctx *Ctx) error {
		item := Item[string](ctx)
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("result_%d", idx), "processed:"+item)
		return nil
	})

	wf, err := NewWorkflow("foreach").
		ForEach("items", Step("e.fe_task")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, Params{
		"items": []string{"a", "b", "c"},
	})

	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["result_0"] != "processed:a" {
		t.Fatalf("expected 'processed:a', got %v", result.Store["result_0"])
	}
	if result.Store["result_2"] != "processed:c" {
		t.Fatalf("expected 'processed:c', got %v", result.Store["result_2"])
	}
}

func TestEngine_ForEach_Concurrent(t *testing.T) {
	ResetTaskRegistry()

	var maxConcurrent int64
	var current int64

	Task("e.fe_conc", func(ctx *Ctx) error {
		c := atomic.AddInt64(&current, 1)
		for {
			old := atomic.LoadInt64(&maxConcurrent)
			if c <= old || atomic.CompareAndSwapInt64(&maxConcurrent, old, c) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt64(&current, -1)
		return nil
	})

	wf, err := NewWorkflow("foreach-conc").
		ForEach("items", Step("e.fe_conc"), WithConcurrency(3)).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, Params{
		"items": []string{"a", "b", "c", "d", "e", "f"},
	})

	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	mc := atomic.LoadInt64(&maxConcurrent)
	if mc < 2 {
		t.Fatalf("expected at least 2 concurrent, got %d", mc)
	}
	if mc > 3 {
		t.Fatalf("expected max 3 concurrent, got %d", mc)
	}
}

func TestEngine_Branch_MatchFirst(t *testing.T) {
	ResetTaskRegistry()

	Task("e.urgent", func(ctx *Ctx) error {
		Set(ctx, "path", "urgent")
		return nil
	})
	Task("e.normal", func(ctx *Ctx) error {
		Set(ctx, "path", "normal")
		return nil
	})

	wf, err := NewWorkflow("branch").
		Branch(
			When(func(ctx *Ctx) bool {
				return Get[string](ctx, "priority") == "high"
			}).Step("e.urgent"),
			Default().Step("e.normal"),
		).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, Params{"priority": "high"})
	if result.Store["path"] != "urgent" {
		t.Fatalf("expected 'urgent', got %v", result.Store["path"])
	}
}

func TestEngine_Branch_Default(t *testing.T) {
	ResetTaskRegistry()

	Task("e.urgent2", func(ctx *Ctx) error {
		Set(ctx, "path", "urgent")
		return nil
	})
	Task("e.normal2", func(ctx *Ctx) error {
		Set(ctx, "path", "normal")
		return nil
	})

	wf, err := NewWorkflow("branch-default").
		Branch(
			When(func(ctx *Ctx) bool {
				return Get[string](ctx, "priority") == "high"
			}).Step("e.urgent2"),
			Default().Step("e.normal2"),
		).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, Params{"priority": "low"})
	if result.Store["path"] != "normal" {
		t.Fatalf("expected 'normal', got %v", result.Store["path"])
	}
}

func TestEngine_DoUntil(t *testing.T) {
	ResetTaskRegistry()

	Task("e.increment", func(ctx *Ctx) error {
		count := GetOr[int](ctx, "count", 0)
		Set(ctx, "count", count+1)
		return nil
	})

	wf, err := NewWorkflow("until").
		DoUntil(Step("e.increment"), func(ctx *Ctx) bool {
			return Get[int](ctx, "count") >= 5
		}).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["count"] != 5 {
		t.Fatalf("expected count 5, got %v", result.Store["count"])
	}
}

func TestEngine_DoWhile(t *testing.T) {
	ResetTaskRegistry()

	Task("e.fetch", func(ctx *Ctx) error {
		page := GetOr[int](ctx, "page", 0)
		page++
		Set(ctx, "page", page)
		Set(ctx, "has_more", page < 3)
		return nil
	})

	wf, err := NewWorkflow("while").
		Map(func(ctx *Ctx) error {
			Set(ctx, "has_more", true) // seed the condition
			return nil
		}).
		DoWhile(Step("e.fetch"), func(ctx *Ctx) bool {
			return Get[bool](ctx, "has_more")
		}).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["page"] != 3 {
		t.Fatalf("expected page 3, got %v", result.Store["page"])
	}
}

func TestEngine_Map(t *testing.T) {
	ResetTaskRegistry()

	Task("e.use_data", func(ctx *Ctx) error {
		items := Get[[]string](ctx, "records")
		Set(ctx, "count", len(items))
		return nil
	})

	wf, err := NewWorkflow("map").
		Map(func(ctx *Ctx) error {
			Set(ctx, "records", []string{"a", "b", "c"})
			return nil
		}).
		Step("e.use_data").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["count"] != 3 {
		t.Fatalf("expected count 3, got %v", result.Store["count"])
	}
}

func TestEngine_SubWorkflow(t *testing.T) {
	ResetTaskRegistry()

	Task("e.sub_inner", func(ctx *Ctx) error {
		Set(ctx, "inner_ran", true)
		return nil
	})
	Task("e.sub_outer", func(ctx *Ctx) error {
		Set(ctx, "outer_ran", true)
		return nil
	})

	inner, err := NewWorkflow("inner").Step("e.sub_inner").Commit()
	if err != nil {
		t.Fatal(err)
	}
	outer, err := NewWorkflow("outer").
		Step("e.sub_outer").
		Sub(inner).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), outer, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["outer_ran"] != true {
		t.Fatal("expected outer_ran")
	}
	if result.Store["inner_ran"] != true {
		t.Fatal("expected inner_ran")
	}
}

func TestEngine_Stage(t *testing.T) {
	ResetTaskRegistry()

	order := make([]string, 0)
	Task("e.s1", func(ctx *Ctx) error {
		order = append(order, "s1")
		return nil
	})
	Task("e.s2", func(ctx *Ctx) error {
		order = append(order, "s2")
		return nil
	})

	wf, err := NewWorkflow("staged").
		Stage("validation", Step("e.s1"), Step("e.s2")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if len(order) != 2 || order[0] != "s1" || order[1] != "s2" {
		t.Fatalf("expected sequential execution [s1 s2], got %v", order)
	}
}

func TestEngine_Retry_TaskLevel(t *testing.T) {
	ResetTaskRegistry()

	attempts := 0
	Task("e.flaky", func(ctx *Ctx) error {
		attempts++
		if attempts < 3 {
			return errors.New("flaky error")
		}
		Set(ctx, "success", true)
		return nil
	}, WithRetry(3))

	wf, err := NewWorkflow("retry").Step("e.flaky").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (attempts: %d)", result.Status, attempts)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestEngine_Retry_WorkflowDefault(t *testing.T) {
	ResetTaskRegistry()

	attempts := 0
	Task("e.flaky2", func(ctx *Ctx) error {
		attempts++
		if attempts < 3 {
			return errors.New("flaky")
		}
		return nil
	})

	wf, err := NewWorkflow("retry-default", WithDefaultRetry(5, 10*time.Millisecond)).
		Step("e.flaky2").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
}

func TestEngine_Retry_ExhaustedFails(t *testing.T) {
	ResetTaskRegistry()

	Task("e.always_fail", func(ctx *Ctx) error {
		return errors.New("always fails")
	}, WithRetry(2))

	wf, err := NewWorkflow("exhaust").Step("e.always_fail").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
}

func TestEngine_LifecycleCallbacks(t *testing.T) {
	ResetTaskRegistry()

	var completedSteps []string
	var errorSeen error

	Task("e.cb_ok", func(ctx *Ctx) error { return nil })
	Task("e.cb_fail", func(ctx *Ctx) error { return errors.New("oops") })

	wf, err := NewWorkflow("callbacks",
		OnStepComplete(func(step string, ctx *Ctx) {
			completedSteps = append(completedSteps, step)
		}),
		OnError(func(err error) {
			errorSeen = err
		}),
	).
		Step("e.cb_ok").
		Step("e.cb_fail").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}

	if len(completedSteps) != 1 || completedSteps[0] != "e.cb_ok" {
		t.Fatalf("expected [e.cb_ok] completed, got %v", completedSteps)
	}
	if errorSeen == nil {
		t.Fatal("expected error callback to be called")
	}
}

func TestEngine_Sleep_Memory(t *testing.T) {
	ResetTaskRegistry()

	Task("e.after_sleep", func(ctx *Ctx) error {
		Set(ctx, "awake", true)
		return nil
	})

	wf, err := NewWorkflow("sleep").
		Sleep(10 * time.Millisecond).
		Step("e.after_sleep").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New() // memory persistence → blocking sleep
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["awake"] != true {
		t.Fatal("expected awake to be true")
	}
}

func TestEngine_Sleep_Persistent(t *testing.T) {
	ResetTaskRegistry()

	Task("e.post_sleep", func(ctx *Ctx) error {
		return nil
	})

	wf, err := NewWorkflow("sleep-persist").
		Sleep(time.Hour).
		Step("e.post_sleep").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	engine, _ := New(WithSQLite(filepath.Join(dir, "test.db")))
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Sleeping {
		t.Fatalf("expected Sleeping, got %s", result.Status)
	}
}

func TestEngine_Resume(t *testing.T) {
	ResetTaskRegistry()

	Task("e.approve_order", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			approved := ResumeData[bool](ctx, "approved")
			if !approved {
				return Bail(ctx, "Order rejected")
			}
			Set(ctx, "approved", true)
			return nil
		}
		return Suspend(ctx, Params{"reason": "needs approval"})
	})

	wf, err := NewWorkflow("resume").Step("e.approve_order").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := New()
	defer engine.Close()

	// First run → Suspended
	result, _ := engine.RunSync(context.Background(), wf, Params{"order_id": "ORD-1"})
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}

	// Resume with approval
	resumed, err := engine.Resume(context.Background(), result.RunID, Params{"approved": true})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Status != Completed {
		t.Fatalf("expected Completed after resume, got %s", resumed.Status)
	}
}

func TestEngine_WithSQLite_Integration(t *testing.T) {
	ResetTaskRegistry()

	Task("e.sqlite_task", func(ctx *Ctx) error {
		Set(ctx, "result", "persisted")
		return nil
	})

	wf, err := NewWorkflow("sqlite-test").Step("e.sqlite_task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "engine.db")

	engine, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatalf("failed to create engine with SQLite: %v", err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, Params{"input": "test"})
	if err != nil {
		t.Fatalf("RunSync failed: %v", err)
	}

	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	// Verify run persisted
	run, err := engine.persistence.LoadRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("LoadRun from SQLite failed: %v", err)
	}
	if run.Status != Completed {
		t.Fatalf("expected Completed in SQLite, got %s", run.Status)
	}

	// Verify state was captured in result before cleanup
	if result.Store["result"] != "persisted" {
		t.Fatalf("expected result.Store to contain 'persisted', got %v", result.Store["result"])
	}

	// Verify state was cleaned up from persistence for completed runs
	state, err := engine.persistence.LoadState(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("LoadState from SQLite failed: %v", err)
	}
	if len(state) != 0 {
		t.Fatalf("expected empty state data after cleanup, got %d entries", len(state))
	}
}

func TestEngine_ForEach_Empty(t *testing.T) {
	ResetTaskRegistry()

	Task("e.noop", func(ctx *Ctx) error {
		Set(ctx, "should_not_run", true)
		return nil
	})

	wf, err := NewWorkflow("empty").
		ForEach("items", Step("e.noop")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, Params{"items": []string{}})
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["should_not_run"] == true {
		t.Fatal("task should not have run for empty collection")
	}
}

func TestEngine_Retry_BailNotRetried(t *testing.T) {
	ResetTaskRegistry()

	attempts := 0
	Task("e.bail_retry", func(ctx *Ctx) error {
		attempts++
		return Bail(ctx, "done")
	}, WithRetry(3))

	wf, err := NewWorkflow("bail-no-retry").Step("e.bail_retry").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Bailed {
		t.Fatalf("expected Bailed, got %s", result.Status)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt (bail not retried), got %d", attempts)
	}
}

func TestEngine_StepFailStopsExecution(t *testing.T) {
	ResetTaskRegistry()

	Task("e.ok_step", func(ctx *Ctx) error {
		Set(ctx, "step1", true)
		return nil
	})
	Task("e.fail_step", func(ctx *Ctx) error {
		return errors.New("boom")
	})
	Task("e.after_fail", func(ctx *Ctx) error {
		Set(ctx, "step3", true)
		return nil
	})

	wf, err := NewWorkflow("stop").
		Step("e.ok_step").
		Step("e.fail_step").
		Step("e.after_fail").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	if result.Store["step1"] != true {
		t.Fatal("expected step1 to have run")
	}
	if result.Store["step3"] == true {
		t.Fatal("expected step3 NOT to have run after failure")
	}
}

func TestEngine_DeleteKeySurvivesResume(t *testing.T) {
	ResetTaskRegistry()

	Task("e.set_and_suspend", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			// On resume: delete the "remove" key via the state directly
			ctx.state.Delete("remove")
			Set(ctx, "resumed", true)
			return nil
		}
		// First execution: set both keys, then suspend
		Set(ctx, "keep", "preserved")
		Set(ctx, "remove", "should-disappear")
		return Suspend(ctx, Params{"reason": "test"})
	})

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "delete-key.db")

	// Engine 1: run until suspended
	engine1, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatalf("create engine1: %v", err)
	}

	wf, err := NewWorkflow("delete-key-test").Step("e.set_and_suspend").Commit()
	if err != nil {
		t.Fatal(err)
	}
	result1, err := engine1.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("RunSync: %v", err)
	}
	if result1.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result1.Status)
	}
	runID := result1.RunID

	// Close engine1 (simulates crash)
	engine1.Close()

	// Engine 2: new engine from same database (simulates restart)
	engine2, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatalf("create engine2: %v", err)
	}
	defer engine2.Close()

	// Resume the run
	result2, err := engine2.Resume(context.Background(), runID, nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if result2.Status != Completed {
		t.Fatalf("expected Completed after resume, got %s", result2.Status)
	}

	// "keep" should be present
	if result2.Store["keep"] != "preserved" {
		t.Fatalf("expected 'keep' to be 'preserved', got %v", result2.Store["keep"])
	}

	// "remove" should be absent (deleted during resume, flushed to persistence)
	if _, exists := result2.Store["remove"]; exists {
		t.Fatalf("expected 'remove' to be absent after delete, but got %v", result2.Store["remove"])
	}

	// "resumed" should confirm the resume path ran
	if result2.Store["resumed"] != true {
		t.Fatal("expected 'resumed' to be true")
	}
}

func TestRetryExplicitZeroHonored(t *testing.T) {
	ResetTaskRegistry()

	var attempts int32
	Task("retry.explicit-zero", func(ctx *Ctx) error {
		atomic.AddInt32(&attempts, 1)
		return fmt.Errorf("always fails")
	}, WithRetry(0)) // explicit zero: no retries

	wf, err := NewWorkflow("retry-zero-test", WithDefaultRetry(3, 10*time.Millisecond)).
		Step("retry.explicit-zero").
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
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
	// With explicit WithRetry(0), the task should execute exactly once,
	// NOT inherit the workflow default of 3 retries.
	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("expected 1 attempt (explicit zero retries), got %d", got)
	}
}

func TestForEachConcurrentSubWorkflow(t *testing.T) {
	ResetTaskRegistry()

	var counter int32
	Task("foreach.sub.step1", func(ctx *Ctx) error {
		atomic.AddInt32(&counter, 1)
		Set(ctx, "step1_done", true)
		return nil
	})
	Task("foreach.sub.step2", func(ctx *Ctx) error {
		atomic.AddInt32(&counter, 1)
		return nil
	})

	subWf, err := NewWorkflow("sub-wf").
		Step("foreach.sub.step1").
		Step("foreach.sub.step2").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	wf, err := NewWorkflow("foreach-concurrent-sub").
		ForEach("items", Sub(subWf), WithConcurrency(4)).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	items := []string{"a", "b", "c", "d"}
	result, err := engine.RunSync(context.Background(), wf, Params{"items": items})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
	// 4 items * 2 steps = 8 task executions
	if got := atomic.LoadInt32(&counter); got != 8 {
		t.Fatalf("expected 8 task executions, got %d", got)
	}
}

func TestRunSyncRejectsAfterClose(t *testing.T) {
	ResetTaskRegistry()

	Task("runsync.noop", func(ctx *Ctx) error {
		return nil
	})

	wf, err := NewWorkflow("runsync-closed").Step("runsync.noop").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	engine.Close()

	_, err = engine.RunSync(context.Background(), wf, nil)
	if !errors.Is(err, ErrEngineClosed) {
		t.Fatalf("expected ErrEngineClosed, got %v", err)
	}
}

func TestRunSyncCancellable(t *testing.T) {
	ResetTaskRegistry()

	started := make(chan struct{})
	Task("runsync.block", func(ctx *Ctx) error {
		close(started)
		<-ctx.goCtx.Done()
		return ctx.goCtx.Err()
	})

	wf, err := NewWorkflow("runsync-cancel").Step("runsync.block").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	done := make(chan *Result, 1)
	var runID RunID
	go func() {
		result, _ := engine.RunSync(context.Background(), wf, nil)
		done <- result
	}()

	// Wait for task to start
	<-started

	// Find the run and cancel it
	time.Sleep(10 * time.Millisecond) // let registration complete
	engine.mu.Lock()
	for id := range engine.runs {
		runID = id
		break
	}
	engine.mu.Unlock()

	err = engine.Cancel(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-done:
		if result.Status != Failed && result.Status != Cancelled {
			t.Fatalf("expected Failed or Cancelled, got %s", result.Status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunSync did not complete after Cancel")
	}
}

// TestEngine_LoopIterationLimit verifies DoUntil returns an error when
// the condition never becomes true, instead of looping infinitely.
func TestEngine_LoopIterationLimit(t *testing.T) {
	ResetTaskRegistry()

	Task("loop.noop", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("loop-limit").
		DoUntil(Step("loop.noop"), func(ctx *Ctx) bool {
			return false // never true — should hit maxLoopIterations
		}).Commit()
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
		t.Fatalf("unexpected RunSync error: %v", err)
	}
	if result.Status != Failed {
		t.Fatalf("expected Failed status, got %s", result.Status)
	}
}

// TestEngine_ForEachConcurrentFailure verifies that a concurrent ForEach
// propagates errors from failing items and cancels remaining work.
func TestEngine_ForEachConcurrentFailure(t *testing.T) {
	ResetTaskRegistry()

	var completed atomic.Int32
	Task("fe.work", func(ctx *Ctx) error {
		idx := Item[int](ctx)
		if idx == 2 {
			return fmt.Errorf("item %d fails", idx)
		}
		time.Sleep(50 * time.Millisecond) // give failing item time to cancel
		completed.Add(1)
		return nil
	})

	wf, err := NewWorkflow("fe-fail").
		ForEach("items", Step("fe.work"), WithConcurrency(4)).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, Params{
		"items": []int{0, 1, 2, 3, 4},
	})
	if err != nil {
		t.Fatalf("unexpected RunSync error: %v", err)
	}
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
}

// TestEngine_ParallelRefPanic verifies that a panic in a parallel goroutine
// is caught and propagated as an error instead of crashing the process.
func TestEngine_ParallelRefPanic(t *testing.T) {
	ResetTaskRegistry()

	Task("par.ok", func(ctx *Ctx) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	Task("par.panic", func(ctx *Ctx) error {
		panic("boom from parallel ref")
	})

	wf, err := NewWorkflow("par-panic").
		Parallel(Step("par.ok"), Step("par.panic")).
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
		t.Fatalf("unexpected RunSync error: %v", err)
	}
	if result.Status != Failed {
		t.Fatalf("expected Failed, got %s", result.Status)
	}
}

// TestEngine_MiddlewareShortCircuit verifies that step middleware can
// short-circuit execution by not calling next().
func TestEngine_MiddlewareShortCircuit(t *testing.T) {
	ResetTaskRegistry()

	var taskRan atomic.Bool
	Task("mw.skipped", func(ctx *Ctx) error {
		taskRan.Store(true)
		return nil
	})

	wf, err := NewWorkflow("mw-skip").
		Step("mw.skipped").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(
		WithStepMiddleware(func(ctx context.Context, info StepInfo, runID RunID, next func() error) error {
			// Intentionally do NOT call next() — short-circuit
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if taskRan.Load() {
		t.Fatal("task should not have executed — middleware short-circuited")
	}
}

// TestEngine_ForEachConcurrentState verifies that concurrent ForEach items
// can write to shared state without triggering the race detector.
func TestEngine_ForEachConcurrentState(t *testing.T) {
	ResetTaskRegistry()

	Task("fe.write", func(ctx *Ctx) error {
		idx := Item[int](ctx)
		Set(ctx, fmt.Sprintf("result_%d", idx), idx*10)
		return nil
	})

	wf, err := NewWorkflow("fe-state").
		ForEach("items", Step("fe.write"), WithConcurrency(4)).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, Params{
		"items": []int{0, 1, 2, 3, 4, 5, 6, 7},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	// Verify all items wrote their results
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("result_%d", i)
		val, ok := result.Store[key]
		if !ok {
			t.Fatalf("missing key %q", key)
		}
		if val != i*10 {
			t.Fatalf("%q = %v, want %d", key, val, i*10)
		}
	}
}

func TestEngine_GetRun(t *testing.T) {
	ResetTaskRegistry()
	Task("gr.task", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})
	wf, err := NewWorkflow("get-run").Step("gr.task").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(WithSQLite(filepath.Join(t.TempDir(), "gr.db")))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	run, err := engine.GetRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != Completed {
		t.Fatalf("expected Completed, got %s", run.Status)
	}
	if run.RunID != result.RunID {
		t.Fatalf("RunID mismatch: %s vs %s", run.RunID, result.RunID)
	}
}

func TestEngine_WithAutoRecover(t *testing.T) {
	ResetTaskRegistry()
	Task("ar.task", func(ctx *Ctx) error { return nil })

	dbPath := filepath.Join(t.TempDir(), "ar.db")

	engine1, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	run := &RunState{
		RunID:      "crashed-run",
		WorkflowID: "ar-wf",
		Status:     Running,
		StepStates: make(map[string]Status),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	if err := engine1.persistence.SaveRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	engine1.Close()

	engine2, err := New(WithSQLite(dbPath), WithAutoRecover())
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()

	recovered, err := engine2.GetRun(ctx, "crashed-run")
	if err != nil {
		t.Fatalf("GetRun after recovery: %v", err)
	}
	if recovered.Status != Failed {
		t.Fatalf("expected Failed after auto-recover, got %s", recovered.Status)
	}
}

func TestEngine_SleepWakeCycle_EndToEnd(t *testing.T) {
	ResetTaskRegistry()
	Task("sw.before", func(ctx *Ctx) error {
		Set(ctx, "phase", "before-sleep")
		return nil
	})
	Task("sw.after", func(ctx *Ctx) error {
		Set(ctx, "phase", "after-sleep")
		return nil
	})

	wf, err := NewWorkflow("sleep-wake").
		Step("sw.before").
		Sleep(100 * time.Millisecond).
		Step("sw.after").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "sw.db")
	engine, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	runID, err := engine.Run(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	// The run goes through two handle lifecycles:
	// 1. Run → Sleeping (initial handle cleaned up)
	// 2. Wake → Completed (new handle created by wakeWorkflow)
	// Poll GetRun until the full cycle completes. The run may not be
	// in persistence yet immediately after Run() returns (async execution).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, getErr := engine.GetRun(context.Background(), runID)
		if getErr != nil {
			// Run not yet in persistence — execution hasn't started
			if errors.Is(getErr, ErrRunNotFound) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			t.Fatalf("GetRun: %v", getErr)
		}
		if run.Status == Completed {
			return
		}
		if run.Status == Failed {
			t.Fatalf("run failed during sleep-wake cycle")
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("sleep-wake cycle did not complete within 5 seconds")
}

func TestEngine_EnableStep(t *testing.T) {
	ResetTaskRegistry()
	var order []string
	Task("en.a", func(ctx *Ctx) error {
		order = append(order, "a")
		return nil
	})
	Task("en.b", func(ctx *Ctx) error {
		order = append(order, "b")
		DisableStep(ctx, "en.c")
		EnableStep(ctx, "en.c")
		return nil
	})
	Task("en.c", func(ctx *Ctx) error {
		order = append(order, "c")
		return nil
	})

	wf, err := NewWorkflow("enable-step").
		Step("en.a").
		Step("en.b").
		Step("en.c").
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
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if len(order) != 3 || order[2] != "c" {
		t.Fatalf("expected [a b c], got %v", order)
	}
}

func TestEngine_EnableByTag(t *testing.T) {
	ResetTaskRegistry()
	var order []string
	Task("et.setup", func(ctx *Ctx) error {
		order = append(order, "setup")
		DisableByTag(ctx, "optional")
		EnableByTag(ctx, "optional")
		return nil
	})
	Task("et.opt", func(ctx *Ctx) error {
		order = append(order, "opt")
		return nil
	})

	wf, err := NewWorkflow("enable-tag").
		Step("et.setup").
		Step("et.opt", WithStepTags("optional")).
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
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if len(order) != 2 || order[1] != "opt" {
		t.Fatalf("expected [setup opt], got %v", order)
	}
}

func TestEngine_WithPersistence(t *testing.T) {
	ResetTaskRegistry()
	Task("wp.task", func(ctx *Ctx) error {
		Set(ctx, "val", 42)
		return nil
	})
	wf, err := NewWorkflow("with-persist").Step("wp.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	customPersist := newMemoryPersistence()
	engine, err := New(WithPersistence(customPersist))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	run, loadErr := customPersist.LoadRun(context.Background(), result.RunID)
	if loadErr != nil {
		t.Fatalf("LoadRun from custom persistence: %v", loadErr)
	}
	if run.Status != Completed {
		t.Fatalf("custom persistence shows %s, want Completed", run.Status)
	}
}

func TestEngine_WithLogger(t *testing.T) {
	ResetTaskRegistry()
	Task("wl.task", func(ctx *Ctx) error { return nil })
	wf, err := NewWorkflow("with-logger").Step("wl.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	logger := NewDefaultLogger()
	engine, err := New(WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
}

func TestTask_WithDescription(t *testing.T) {
	ResetTaskRegistry()
	Task("wd.task", func(ctx *Ctx) error { return nil },
		WithDescription("Process payment charge"),
	)
	td := defaultRegistry.lookupTask("wd.task")
	if td == nil {
		t.Fatal("task not registered")
	}
	if td.description != "Process payment charge" {
		t.Fatalf("expected description 'Process payment charge', got %q", td.description)
	}
}

func TestNewWorkflowFromDefWithRegistry(t *testing.T) {
	ResetTaskRegistry()

	customReg := NewRegistry()
	customReg.RegisterTask("defr.task", func(ctx *Ctx) error {
		Set(ctx, "source", "custom-registry")
		return nil
	})

	wf, err := NewWorkflow("defr-wf").Step("defr.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	def, err := WorkflowToDefinition(wf)
	if err != nil {
		t.Fatalf("WorkflowToDefinition: %v", err)
	}
	jsonBytes, err := MarshalWorkflowDefinition(def)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	def2, err := UnmarshalWorkflowDefinition(jsonBytes)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	rebuilt, err := NewWorkflowFromDefWithRegistry(def2, customReg)
	if err != nil {
		t.Fatalf("NewWorkflowFromDefWithRegistry: %v", err)
	}

	engine, err := New(WithRegistry(customReg))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), rebuilt, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	source, _ := ResultGet[string](result, "source")
	if source != "custom-registry" {
		t.Fatalf("expected 'custom-registry', got %q", source)
	}
}

func TestEngine_PurgeRuns(t *testing.T) {
	ResetTaskRegistry()
	Task("pr.task", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("pr-wf").Step("pr.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "pr.db")
	engine, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	for i := 0; i < 3; i++ {
		result, runErr := engine.RunSync(context.Background(), wf, nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
		if result.Status != Completed {
			t.Fatalf("run %d: expected Completed, got %s", i, result.Status)
		}
	}

	runs, err := engine.ListRuns(context.Background(), RunFilter{Status: Completed})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 3 {
		t.Fatalf("expected 3 completed runs, got %d", len(runs))
	}

	purged, err := engine.PurgeRuns(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if purged != 3 {
		t.Fatalf("expected 3 purged, got %d", purged)
	}

	remaining, err := engine.ListRuns(context.Background(), RunFilter{Status: Completed})
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected 0 remaining, got %d", len(remaining))
	}
}

func TestEngine_WithRunGC(t *testing.T) {
	ResetTaskRegistry()
	Task("gc.task", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("gc-wf").Step("gc.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "gc.db")
	engine, err := New(
		WithSQLite(dbPath),
		WithRunGC(0, 100*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	// Wait for GC to run
	time.Sleep(300 * time.Millisecond)

	runs, err := engine.ListRuns(context.Background(), RunFilter{Status: Completed})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected GC to purge completed run, got %d remaining", len(runs))
	}

	engine.Close()
}

func TestEngine_SuspendAnonymousWorkflow_Fails(t *testing.T) {
	ResetTaskRegistry()
	Task("sa.task", func(ctx *Ctx) error {
		return Suspend(ctx, Params{"reason": "need input"})
	})

	// Build a workflow with an anonymous closure — WorkflowDef will be nil
	wf, err := NewWorkflow("sa-wf").
		Step("sa.task").
		Branch(When(func(ctx *Ctx) bool { return true }).Step("sa.task")).
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
	if result.Status != Failed {
		t.Fatalf("expected Failed (anonymous workflow can't suspend), got %s", result.Status)
	}
	if !errors.Is(result.Error, ErrWorkflowNotResumable) {
		t.Fatalf("expected ErrWorkflowNotResumable, got: %v", result.Error)
	}
}

func TestEngine_ResumeNilWorkflowDef(t *testing.T) {
	ResetTaskRegistry()
	Task("rn.task", func(ctx *Ctx) error {
		return Suspend(ctx, Params{"reason": "input"})
	})

	dbPath := filepath.Join(t.TempDir(), "rn.db")
	engine, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}

	// Manually create a suspended run with nil WorkflowDef
	ctx := context.Background()
	run := &RunState{
		RunID:      "nil-def-run",
		WorkflowID: "rn-wf",
		Status:     Suspended,
		StepStates: make(map[string]Status),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	if err := engine.persistence.SaveRun(ctx, run); err != nil {
		t.Fatal(err)
	}

	_, err = engine.Resume(ctx, "nil-def-run", Params{"data": "value"})
	if err == nil {
		t.Fatal("expected error for nil WorkflowDef")
	}
	if !errors.Is(err, ErrWorkflowNotResumable) {
		t.Fatalf("expected ErrWorkflowNotResumable, got: %v", err)
	}
	engine.Close()
}

func TestEngine_ResumeMissingTask(t *testing.T) {
	ResetTaskRegistry()
	Task("rm.task", func(ctx *Ctx) error {
		return Suspend(ctx, Params{"need": "input"})
	})

	wf, err := NewWorkflow("rm-wf").Step("rm.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "rm.db")
	engine, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}
	runID := result.RunID
	engine.Close()

	// Create a new engine WITHOUT registering the task
	ResetTaskRegistry()
	engine2, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()

	_, err = engine2.Resume(context.Background(), runID, Params{"data": "value"})
	if err == nil {
		t.Fatal("expected error for missing task")
	}
}

func TestEngine_ResumeCrossEngine(t *testing.T) {
	ResetTaskRegistry()
	Task("ce.task", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			Set(ctx, "resumed", true)
			return nil
		}
		return Suspend(ctx, Params{"need": "approval"})
	})

	wf, err := NewWorkflow("ce-wf").Step("ce.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "ce.db")
	engine1, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}

	result, err := engine1.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}
	runID := result.RunID
	engine1.Close()

	// New engine, same database — Resume rebuilds from persisted WorkflowDef
	engine2, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()

	result2, err := engine2.Resume(context.Background(), runID, Params{"approved": true})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if result2.Status != Completed {
		t.Fatalf("expected Completed, got %s", result2.Status)
	}
	resumed, _ := ResultGet[bool](result2, "resumed")
	if !resumed {
		t.Fatal("expected resumed=true")
	}
}
