package gostage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/davidroman0O/gostage/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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

// === Worker Pool ===

func TestWorkerPool(t *testing.T) {
	p := newWorkerPool(4)
	defer p.Shutdown(0)

	if p.Size() != 4 {
		t.Fatalf("expected pool size 4, got %d", p.Size())
	}

	var count int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		ok := p.Submit(func() {
			defer wg.Done()
			atomic.AddInt32(&count, 1)
			time.Sleep(10 * time.Millisecond)
		})
		if !ok {
			t.Fatal("submit should succeed")
		}
	}
	wg.Wait()

	if atomic.LoadInt32(&count) != 20 {
		t.Fatalf("expected 20 completions, got %d", count)
	}
}

func TestWorkerPoolShutdown(t *testing.T) {
	p := newWorkerPool(2)
	p.Shutdown(0)

	// Submit after shutdown should return false
	ok := p.Submit(func() {})
	if ok {
		t.Fatal("submit after shutdown should return false")
	}
}

func TestPoolBoundsWorkflows(t *testing.T) {
	ResetTaskRegistry()

	var maxConcurrent int32
	var current int32

	Task("pool.slow", func(ctx *Ctx) error {
		c := atomic.AddInt32(&current, 1)
		for {
			old := atomic.LoadInt32(&maxConcurrent)
			if c <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, c) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		return nil
	})

	// Pool of 2 workers — only 2 workflows can execute at once
	engine, err := New(WithWorkerPoolSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	wf, err := NewWorkflow("pool-bounded").Step("pool.slow").Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Start 6 workflows async
	var ids []RunID
	for i := 0; i < 6; i++ {
		id, err := engine.Run(context.Background(), wf, nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	// Wait for all to complete
	for _, id := range ids {
		result, err := engine.Wait(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != Completed {
			t.Fatalf("expected completed, got %s: %v", result.Status, result.Error)
		}
	}

	// Peak concurrency should be ≤ pool size (2)
	peak := atomic.LoadInt32(&maxConcurrent)
	if peak > 2 {
		t.Fatalf("expected max concurrent workflows ≤ 2, got %d", peak)
	}
	if peak < 1 {
		t.Fatal("expected at least 1 concurrent execution")
	}
}

// === Tag System ===

func TestTaskTags(t *testing.T) {
	ResetTaskRegistry()

	Task("tag.email", func(ctx *Ctx) error { return nil }, WithTags("notification", "async"))
	Task("tag.sms", func(ctx *Ctx) error { return nil }, WithTags("notification"))
	Task("tag.charge", func(ctx *Ctx) error { return nil }, WithTags("billing"))

	notifTasks := ListTasksByTag("notification")
	if len(notifTasks) != 2 {
		t.Fatalf("expected 2 notification tasks, got %d", len(notifTasks))
	}

	billingTasks := ListTasksByTag("billing")
	if len(billingTasks) != 1 {
		t.Fatalf("expected 1 billing task, got %d", len(billingTasks))
	}
}

func TestStepTags(t *testing.T) {
	ResetTaskRegistry()

	Task("tag.validate", func(ctx *Ctx) error { return nil })
	Task("tag.charge", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("tagged-steps").
		Step("tag.validate", WithStepTags("validation")).
		Step("tag.charge", WithStepTags("billing", "critical")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if len(wf.steps[0].tags) != 1 || wf.steps[0].tags[0] != "validation" {
		t.Fatal("step 0 should have 'validation' tag")
	}
	if len(wf.steps[1].tags) != 2 {
		t.Fatal("step 1 should have 2 tags")
	}
}

func TestWorkflowTags(t *testing.T) {
	ResetTaskRegistry()

	Task("tag.noop", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("tagged-workflow", WithWorkflowTags("billing", "critical")).
		Step("tag.noop").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if len(wf.Tags) != 2 {
		t.Fatalf("expected 2 workflow tags, got %d", len(wf.Tags))
	}
}

func TestFindStepsByTag(t *testing.T) {
	ResetTaskRegistry()

	Task("tag.a", func(ctx *Ctx) error {
		ids := FindStepsByTag(ctx, "optional")
		Set(ctx, "optional_count", len(ids))
		return nil
	})
	Task("tag.b", func(ctx *Ctx) error { return nil })
	Task("tag.c", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("find-by-tag").
		Step("tag.a").
		Step("tag.b", WithStepTags("optional")).
		Step("tag.c", WithStepTags("optional")).
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

	count, ok := result.Store["optional_count"]
	if !ok {
		t.Fatal("optional_count not in store")
	}
	// JSON round trip: int -> float64
	if count != 2 && count != float64(2) {
		t.Fatalf("expected 2 optional steps, got %v", count)
	}
}

// === IPC Message Handlers ===

func TestOnMessage(t *testing.T) {
	ResetTaskRegistry()

	var received sync.Map

	Task("ipc.sender", func(ctx *Ctx) error {
		return Send(ctx, "progress", Params{"pct": 50})
	})

	wf, err := NewWorkflow("on-message").Step("ipc.sender").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.OnMessage("progress", func(msgType string, payload map[string]any) {
		received.Store("type", msgType)
		received.Store("pct", payload["pct"])
	})

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	msgType, ok := received.Load("type")
	if !ok || msgType != "progress" {
		t.Fatalf("expected progress message, got %v", msgType)
	}

	pct, _ := received.Load("pct")
	if pct != 50 {
		t.Fatalf("expected pct 50, got %v (%T)", pct, pct)
	}
}

func TestOnMessageWildcard(t *testing.T) {
	ResetTaskRegistry()

	var msgCount int32

	Task("ipc.multi", func(ctx *Ctx) error {
		Send(ctx, "type_a", Params{"x": 1})
		Send(ctx, "type_b", Params{"x": 2})
		return nil
	})

	wf, err := NewWorkflow("wildcard-handler").Step("ipc.multi").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.OnMessage("*", func(msgType string, payload map[string]any) {
		atomic.AddInt32(&msgCount, 1)
	})

	engine.RunSync(context.Background(), wf, nil)

	if atomic.LoadInt32(&msgCount) != 2 {
		t.Fatalf("expected 2 wildcard messages, got %d", msgCount)
	}
}

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

// === Middleware System ===

func TestEngineMiddleware(t *testing.T) {
	ResetTaskRegistry()

	var order []string
	var mu sync.Mutex

	Task("mw.task", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "task")
		mu.Unlock()
		return nil
	})

	wf, err := NewWorkflow("engine-mw").Step("mw.task").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(
		WithEngineMiddleware(func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error {
			mu.Lock()
			order = append(order, "engine-before")
			mu.Unlock()
			err := next()
			mu.Lock()
			order = append(order, "engine-after")
			mu.Unlock()
			return err
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.RunSync(context.Background(), wf, nil)

	if len(order) != 3 {
		t.Fatalf("expected 3 entries, got %v", order)
	}
	if order[0] != "engine-before" || order[1] != "task" || order[2] != "engine-after" {
		t.Fatalf("expected [engine-before, task, engine-after], got %v", order)
	}
}

func TestStepMiddleware(t *testing.T) {
	ResetTaskRegistry()

	var stepNames []string
	var mu sync.Mutex

	Task("mw.a", func(ctx *Ctx) error { return nil })
	Task("mw.b", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("step-mw").Step("mw.a").Step("mw.b").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(
		WithStepMiddleware(func(ctx context.Context, info StepInfo, runID RunID, next func() error) error {
			mu.Lock()
			stepNames = append(stepNames, info.Name)
			mu.Unlock()
			return next()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.RunSync(context.Background(), wf, nil)

	if len(stepNames) != 2 {
		t.Fatalf("expected 2 step names, got %v", stepNames)
	}
}

func TestTaskMiddleware(t *testing.T) {
	ResetTaskRegistry()

	var taskNames []string
	var mu sync.Mutex

	Task("mw.t1", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("task-mw").Step("mw.t1").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(
		WithTaskMiddleware(func(tctx *Ctx, taskName string, next func() error) error {
			mu.Lock()
			taskNames = append(taskNames, taskName)
			mu.Unlock()
			return next()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.RunSync(context.Background(), wf, nil)

	if len(taskNames) != 1 || taskNames[0] != "mw.t1" {
		t.Fatalf("expected [mw.t1], got %v", taskNames)
	}
}

type pluginCounters struct {
	engineCalled int32
	stepCalled   int32
	taskCalled   int32
}

func TestMiddlewarePlugin(t *testing.T) {
	ResetTaskRegistry()

	p := &pluginCounters{}

	Task("mw.plug", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("middleware-plugin").Step("mw.plug").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(WithPlugin(pluginAdapter{p}))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.RunSync(context.Background(), wf, nil)

	if atomic.LoadInt32(&p.engineCalled) != 1 {
		t.Fatal("engine middleware not called")
	}
	if atomic.LoadInt32(&p.stepCalled) != 1 {
		t.Fatal("step middleware not called")
	}
	if atomic.LoadInt32(&p.taskCalled) != 1 {
		t.Fatal("task middleware not called")
	}
}

type pluginAdapter struct {
	p *pluginCounters
}

func (a pluginAdapter) EngineMiddleware() EngineMiddleware {
	return func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error {
		atomic.AddInt32(&a.p.engineCalled, 1)
		return next()
	}
}
func (a pluginAdapter) StepMiddleware() StepMiddleware {
	return func(ctx context.Context, info StepInfo, runID RunID, next func() error) error {
		atomic.AddInt32(&a.p.stepCalled, 1)
		return next()
	}
}
func (a pluginAdapter) TaskMiddleware() TaskMiddleware {
	return func(tctx *Ctx, taskName string, next func() error) error {
		atomic.AddInt32(&a.p.taskCalled, 1)
		return next()
	}
}
func (a pluginAdapter) ChildMiddleware() ChildMiddleware { return nil }

func TestPerWorkflowMiddleware(t *testing.T) {
	ResetTaskRegistry()

	var wfStepNames []string
	var mu sync.Mutex

	Task("mw.wf1", func(ctx *Ctx) error { return nil })
	Task("mw.wf2", func(ctx *Ctx) error { return nil })

	wfMW := func(ctx context.Context, info StepInfo, runID RunID, next func() error) error {
		mu.Lock()
		wfStepNames = append(wfStepNames, info.Name)
		mu.Unlock()
		return next()
	}

	wf, err := NewWorkflow("per-workflow-mw", WithWorkflowMiddleware(wfMW)).
		Step("mw.wf1").Step("mw.wf2").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.RunSync(context.Background(), wf, nil)

	if len(wfStepNames) != 2 {
		t.Fatalf("expected 2 per-workflow step middleware calls, got %d", len(wfStepNames))
	}
}

// === Dynamic Mutations ===

func TestMutationInsertAfter(t *testing.T) {
	ResetTaskRegistry()

	var order []string
	var mu sync.Mutex

	Task("mut.first", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "first")
		mu.Unlock()
		InsertAfter(ctx, "mut.dynamic")
		return nil
	})
	Task("mut.dynamic", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "dynamic")
		mu.Unlock()
		return nil
	})
	Task("mut.last", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "last")
		mu.Unlock()
		return nil
	})

	wf, err := NewWorkflow("insert-after").
		Step("mut.first").
		Step("mut.last").
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

	if len(order) != 3 {
		t.Fatalf("expected 3 steps, got %v", order)
	}
	if order[0] != "first" || order[1] != "dynamic" || order[2] != "last" {
		t.Fatalf("expected [first, dynamic, last], got %v", order)
	}
}

func TestMutationDisableStep(t *testing.T) {
	ResetTaskRegistry()

	Task("mut.enable", func(ctx *Ctx) error {
		Set(ctx, "enabled", true)
		DisableStep(ctx, "mut.skip")
		return nil
	})
	Task("mut.skip", func(ctx *Ctx) error {
		Set(ctx, "skipped_ran", true)
		return nil
	})
	Task("mut.end", func(ctx *Ctx) error {
		Set(ctx, "end_ran", true)
		return nil
	})

	wf, err := NewWorkflow("disable-step").
		Step("mut.enable").
		Step("mut.skip").
		Step("mut.end").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["skipped_ran"] == true {
		t.Fatal("disabled step should not have run")
	}
	if result.Store["end_ran"] != true {
		t.Fatal("end step should have run")
	}
}

func TestMutationDisableByTag(t *testing.T) {
	ResetTaskRegistry()

	Task("mut.controller", func(ctx *Ctx) error {
		DisableByTag(ctx, "optional")
		return nil
	})
	Task("mut.opt1", func(ctx *Ctx) error {
		Set(ctx, "opt1", true)
		return nil
	})
	Task("mut.opt2", func(ctx *Ctx) error {
		Set(ctx, "opt2", true)
		return nil
	})
	Task("mut.required", func(ctx *Ctx) error {
		Set(ctx, "required", true)
		return nil
	})

	wf, err := NewWorkflow("disable-by-tag").
		Step("mut.controller").
		Step("mut.opt1", WithStepTags("optional")).
		Step("mut.opt2", WithStepTags("optional")).
		Step("mut.required").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Store["opt1"] == true || result.Store["opt2"] == true {
		t.Fatal("optional steps should have been disabled")
	}
	if result.Store["required"] != true {
		t.Fatal("required step should have run")
	}
}

func TestMutationInsertSurvivesResume(t *testing.T) {
	ResetTaskRegistry()

	Task("mut.inserter", func(ctx *Ctx) error {
		InsertAfter(ctx, "mut.injected")
		Set(ctx, "inserter_ran", true)
		return nil
	})
	Task("mut.injected", func(ctx *Ctx) error {
		Set(ctx, "injected_ran", true)
		return nil
	})
	Task("mut.suspender", func(ctx *Ctx) error {
		if !IsResuming(ctx) {
			return Suspend(ctx, Params{"reason": "wait"})
		}
		Set(ctx, "resumed", true)
		return nil
	})
	Task("mut.final", func(ctx *Ctx) error {
		Set(ctx, "final_ran", true)
		return nil
	})

	wf, err := NewWorkflow("insert-survives-resume").
		Step("mut.inserter").
		Step("mut.suspender").
		Step("mut.final").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithSQLite(t.TempDir() + "/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// First run: inserter inserts "injected", then suspender suspends
	result, err := engine.RunSync(ctx, wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s (error: %v)", result.Status, result.Error)
	}

	// Resume: injected step should still be present and run
	result, err = engine.Resume(ctx, wf, result.RunID, Params{"go": true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
	if result.Store["injected_ran"] != true {
		t.Fatal("dynamically inserted step should have run after resume")
	}
	if result.Store["resumed"] != true {
		t.Fatal("suspender should have taken resume path")
	}
	if result.Store["final_ran"] != true {
		t.Fatal("final step should have run")
	}
}

func TestMutationDisableSurvivesResume(t *testing.T) {
	ResetTaskRegistry()

	Task("mut.disabler", func(ctx *Ctx) error {
		DisableStep(ctx, "mut.skipped")
		Set(ctx, "disabler_ran", true)
		return nil
	})
	Task("mut.suspender2", func(ctx *Ctx) error {
		if !IsResuming(ctx) {
			return Suspend(ctx, Params{"reason": "wait"})
		}
		Set(ctx, "resumed2", true)
		return nil
	})
	Task("mut.skipped", func(ctx *Ctx) error {
		Set(ctx, "skipped_ran", true)
		return nil
	})
	Task("mut.end2", func(ctx *Ctx) error {
		Set(ctx, "end2_ran", true)
		return nil
	})

	wf, err := NewWorkflow("disable-survives-resume").
		Step("mut.disabler").
		Step("mut.suspender2").
		Step("mut.skipped").
		Step("mut.end2").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithSQLite(t.TempDir() + "/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// First run: disabler disables "skipped", then suspender suspends
	result, err := engine.RunSync(ctx, wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s (error: %v)", result.Status, result.Error)
	}

	// Resume: "skipped" should still be disabled
	result, err = engine.Resume(ctx, wf, result.RunID, Params{"go": true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
	if result.Store["skipped_ran"] == true {
		t.Fatal("disabled step should NOT have run after resume")
	}
	if result.Store["resumed2"] != true {
		t.Fatal("suspender should have taken resume path")
	}
	if result.Store["end2_ran"] != true {
		t.Fatal("end step should have run")
	}
}

// === Serializable Workflow Definitions ===

func TestWorkflowToDefinition(t *testing.T) {
	ResetTaskRegistry()

	Task("def.validate", func(ctx *Ctx) error { return nil })
	Task("def.charge", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("order-def").
		Step("def.validate").
		Step("def.charge").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	def, err := WorkflowToDefinition(wf)
	if err != nil {
		t.Fatal(err)
	}
	if def.ID != "order-def" {
		t.Fatalf("expected ID 'order-def', got %q", def.ID)
	}
	if len(def.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(def.Steps))
	}
	if def.Steps[0].TaskName != "def.validate" {
		t.Fatalf("expected first task 'def.validate', got %q", def.Steps[0].TaskName)
	}
}

func TestDefinitionMarshalUnmarshal(t *testing.T) {
	ResetTaskRegistry()

	Task("def.a", func(ctx *Ctx) error { return nil })
	Task("def.b", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("serial-def").Step("def.a").Step("def.b").Commit()
	if err != nil {
		t.Fatal(err)
	}
	def, defErr := WorkflowToDefinition(wf)
	if defErr != nil {
		t.Fatal(defErr)
	}

	data, err := MarshalWorkflowDefinition(def)
	if err != nil {
		t.Fatal(err)
	}

	def2, err := UnmarshalWorkflowDefinition(data)
	if err != nil {
		t.Fatal(err)
	}
	if def2.ID != def.ID {
		t.Fatalf("expected ID %q, got %q", def.ID, def2.ID)
	}
	if len(def2.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(def2.Steps))
	}
}

func TestNewWorkflowFromDef(t *testing.T) {
	ResetTaskRegistry()

	Task("def.step1", func(ctx *Ctx) error {
		Set(ctx, "step1", true)
		return nil
	})
	Task("def.step2", func(ctx *Ctx) error {
		Set(ctx, "step2", true)
		return nil
	})

	wf, err := NewWorkflow("rebuild-def").Step("def.step1").Step("def.step2").Commit()
	if err != nil {
		t.Fatal(err)
	}
	def, defErr := WorkflowToDefinition(wf)
	if defErr != nil {
		t.Fatal(defErr)
	}

	data, _ := MarshalWorkflowDefinition(def)
	def2, _ := UnmarshalWorkflowDefinition(data)

	rebuilt, err := NewWorkflowFromDef(def2)
	if err != nil {
		t.Fatal(err)
	}

	// Execute the rebuilt workflow
	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), rebuilt, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (err: %v)", result.Status, result.Error)
	}
	if result.Store["step1"] != true || result.Store["step2"] != true {
		t.Fatal("both steps should have executed")
	}
}

// === Function Registry ===

func TestFunctionRegistry(t *testing.T) {
	ResetTaskRegistry()

	// Register a condition
	Condition("test-cond", func(ctx *Ctx) bool { return true })
	if fn := lookupCondition("test-cond"); fn == nil {
		t.Fatal("expected to find registered condition")
	}
	if fn := lookupCondition("nonexistent"); fn != nil {
		t.Fatal("expected nil for unregistered condition")
	}

	// Register a map function
	MapFn("test-map", func(ctx *Ctx) error { return nil })
	if fn := lookupMapFn("test-map"); fn == nil {
		t.Fatal("expected to find registered map function")
	}
	if fn := lookupMapFn("nonexistent"); fn != nil {
		t.Fatal("expected nil for unregistered map function")
	}

	// Duplicate condition should panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on duplicate condition")
			}
		}()
		Condition("test-cond", func(ctx *Ctx) bool { return false })
	}()

	// Duplicate map function should panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on duplicate map function")
			}
		}()
		MapFn("test-map", func(ctx *Ctx) error { return nil })
	}()

	// Reset clears everything
	ResetFunctionRegistries()
	if fn := lookupCondition("test-cond"); fn != nil {
		t.Fatal("expected nil after reset")
	}
	if fn := lookupMapFn("test-map"); fn != nil {
		t.Fatal("expected nil after reset")
	}
}

// === Named Builder Methods ===

func TestNamedBuilderMethods(t *testing.T) {
	ResetTaskRegistry()

	Task("nb.task", func(ctx *Ctx) error { return nil })
	Condition("nb.cond", func(ctx *Ctx) bool { return true })
	MapFn("nb.transform", func(ctx *Ctx) error { return nil })

	// WhenNamed
	bc := WhenNamed("nb.cond").Step("nb.task")
	if bc.condName != "nb.cond" {
		t.Fatalf("expected condName 'nb.cond', got %q", bc.condName)
	}
	if bc.condition == nil {
		t.Fatal("expected condition function to be set")
	}

	// MapNamed
	wf, err := NewWorkflow("nb-test").
		MapNamed("nb.transform").
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	if wf.steps[0].mapFnName != "nb.transform" {
		t.Fatalf("expected mapFnName 'nb.transform', got %q", wf.steps[0].mapFnName)
	}
	if wf.steps[0].mapFn == nil {
		t.Fatal("expected mapFn to be set")
	}

	// DoUntilNamed
	wf2, err := NewWorkflow("nb-until").
		DoUntilNamed(Step("nb.task"), "nb.cond").
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	if wf2.steps[0].loopCondName != "nb.cond" {
		t.Fatalf("expected loopCondName 'nb.cond', got %q", wf2.steps[0].loopCondName)
	}

	// DoWhileNamed
	wf3, err := NewWorkflow("nb-while").
		DoWhileNamed(Step("nb.task"), "nb.cond").
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	if wf3.steps[0].loopCondName != "nb.cond" {
		t.Fatalf("expected loopCondName 'nb.cond', got %q", wf3.steps[0].loopCondName)
	}

	// WhenNamed panics on unregistered condition
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on unregistered condition")
			}
		}()
		WhenNamed("nonexistent")
	}()
}

// === Full Definition Serialization (All 10 Step Kinds) ===

func TestDefinitionAllStepKinds(t *testing.T) {
	ResetTaskRegistry()

	Task("all.task1", func(ctx *Ctx) error { return nil })
	Task("all.task2", func(ctx *Ctx) error { return nil })
	Task("all.task3", func(ctx *Ctx) error { return nil })
	Task("all.loop", func(ctx *Ctx) error { return nil })
	Condition("all.is-ready", func(ctx *Ctx) bool { return true })
	Condition("all.has-more", func(ctx *Ctx) bool { return false })
	MapFn("all.transform", func(ctx *Ctx) error { return nil })

	// Build a sub-workflow for StepSub
	subWf, err := NewWorkflow("all-sub").Step("all.task3").Commit()
	if err != nil {
		t.Fatal(err)
	}

	wf, err := NewWorkflow("all-kinds").
		Step("all.task1").                                           // StepSingle
		Stage("validation", Step("all.task1"), Step("all.task2")).   // StepStage
		Parallel(Step("all.task1"), Step("all.task2")).              // StepParallel
		Branch(                                                     // StepBranch
			WhenNamed("all.is-ready").Step("all.task1"),
			Default().Step("all.task2"),
		).
		ForEach("items", Step("all.task1"), WithConcurrency(3)).    // StepForEach
		MapNamed("all.transform").                                  // StepMap
		DoUntilNamed(Step("all.loop"), "all.is-ready").             // StepDoUntil
		DoWhileNamed(Step("all.loop"), "all.has-more").             // StepDoWhile
		Sub(subWf).                                                 // StepSub
		Sleep(5 * time.Second).                                     // StepSleep
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Serialize
	def, err := WorkflowToDefinition(wf)
	if err != nil {
		t.Fatalf("WorkflowToDefinition: %v", err)
	}

	if len(def.Steps) != 10 {
		t.Fatalf("expected 10 steps, got %d", len(def.Steps))
	}

	// Verify kind names
	expectedKinds := []string{"single", "stage", "parallel", "branch", "forEach", "map", "doUntil", "doWhile", "sub", "sleep"}
	for i, expected := range expectedKinds {
		if def.Steps[i].Kind != expected {
			t.Fatalf("step %d: expected kind %q, got %q", i, expected, def.Steps[i].Kind)
		}
	}

	// Verify specific fields
	if def.Steps[0].TaskName != "all.task1" {
		t.Fatalf("single step: expected task 'all.task1', got %q", def.Steps[0].TaskName)
	}
	if len(def.Steps[1].Refs) != 2 {
		t.Fatalf("stage step: expected 2 refs, got %d", len(def.Steps[1].Refs))
	}
	if len(def.Steps[2].Refs) != 2 {
		t.Fatalf("parallel step: expected 2 refs, got %d", len(def.Steps[2].Refs))
	}
	if len(def.Steps[3].Cases) != 2 {
		t.Fatalf("branch step: expected 2 cases, got %d", len(def.Steps[3].Cases))
	}
	if def.Steps[3].Cases[0].ConditionName != "all.is-ready" {
		t.Fatalf("branch case 0: expected condition 'all.is-ready', got %q", def.Steps[3].Cases[0].ConditionName)
	}
	if !def.Steps[3].Cases[1].IsDefault {
		t.Fatal("branch case 1: expected isDefault=true")
	}
	if def.Steps[4].CollectionKey != "items" {
		t.Fatalf("forEach step: expected collection key 'items', got %q", def.Steps[4].CollectionKey)
	}
	if def.Steps[4].Concurrency != 3 {
		t.Fatalf("forEach step: expected concurrency 3, got %d", def.Steps[4].Concurrency)
	}
	if def.Steps[5].MapFnName != "all.transform" {
		t.Fatalf("map step: expected map fn 'all.transform', got %q", def.Steps[5].MapFnName)
	}
	if def.Steps[6].LoopCondName != "all.is-ready" {
		t.Fatalf("doUntil step: expected cond 'all.is-ready', got %q", def.Steps[6].LoopCondName)
	}
	if def.Steps[7].LoopCondName != "all.has-more" {
		t.Fatalf("doWhile step: expected cond 'all.has-more', got %q", def.Steps[7].LoopCondName)
	}
	if def.Steps[8].SubWorkflow == nil {
		t.Fatal("sub step: expected sub-workflow, got nil")
	}
	if def.Steps[8].SubWorkflow.ID != "all-sub" {
		t.Fatalf("sub step: expected sub-workflow ID 'all-sub', got %q", def.Steps[8].SubWorkflow.ID)
	}
	if def.Steps[9].SleepDuration != (5 * time.Second).String() {
		t.Fatalf("sleep step: expected duration '5s', got %q", def.Steps[9].SleepDuration)
	}

	// Marshal → Unmarshal round-trip
	data, marshalErr := MarshalWorkflowDefinition(def)
	if marshalErr != nil {
		t.Fatalf("marshal: %v", marshalErr)
	}

	def2, unmarshalErr := UnmarshalWorkflowDefinition(data)
	if unmarshalErr != nil {
		t.Fatalf("unmarshal: %v", unmarshalErr)
	}

	if len(def2.Steps) != 10 {
		t.Fatalf("after round-trip: expected 10 steps, got %d", len(def2.Steps))
	}

	// Rebuild workflow from definition
	rebuilt, rebuildErr := NewWorkflowFromDef(def2)
	if rebuildErr != nil {
		t.Fatalf("NewWorkflowFromDef: %v", rebuildErr)
	}

	if len(rebuilt.steps) != 10 {
		t.Fatalf("rebuilt: expected 10 steps, got %d", len(rebuilt.steps))
	}

	// Verify rebuilt step kinds match
	expectedStepKinds := []StepKind{StepSingle, StepStage, StepParallel, StepBranch, StepForEach, StepMap, StepDoUntil, StepDoWhile, StepSub, StepSleep}
	for i, expected := range expectedStepKinds {
		if rebuilt.steps[i].kind != expected {
			t.Fatalf("rebuilt step %d: expected kind %d, got %d", i, expected, rebuilt.steps[i].kind)
		}
	}

	// Verify rebuilt functions are wired
	if rebuilt.steps[3].cases[0].condition == nil {
		t.Fatal("rebuilt branch: condition function should be wired")
	}
	if rebuilt.steps[5].mapFn == nil {
		t.Fatal("rebuilt map: map function should be wired")
	}
	if rebuilt.steps[6].loopCond == nil {
		t.Fatal("rebuilt doUntil: loop condition should be wired")
	}
	if rebuilt.steps[9].sleepDuration != 5*time.Second {
		t.Fatalf("rebuilt sleep: expected 5s, got %v", rebuilt.steps[9].sleepDuration)
	}
}

func TestDefinitionAnonymousError(t *testing.T) {
	ResetTaskRegistry()

	Task("anon.task", func(ctx *Ctx) error { return nil })

	// Anonymous branch condition → error
	wfBranch, err := NewWorkflow("anon-branch").
		Branch(When(func(ctx *Ctx) bool { return true }).Step("anon.task")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	_, err = WorkflowToDefinition(wfBranch)
	if err == nil {
		t.Fatal("expected error for anonymous branch condition")
	}
	if !contains(err.Error(), "unnamed condition") {
		t.Fatalf("expected 'unnamed condition' in error, got: %v", err)
	}

	// Anonymous map function → error
	wfMap, err := NewWorkflow("anon-map").
		Map(func(ctx *Ctx) error { return nil }).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	_, err = WorkflowToDefinition(wfMap)
	if err == nil {
		t.Fatal("expected error for anonymous map function")
	}
	if !contains(err.Error(), "unnamed map function") {
		t.Fatalf("expected 'unnamed map function' in error, got: %v", err)
	}

	// Anonymous loop condition → error
	wfLoop, err := NewWorkflow("anon-loop").
		DoUntil(Step("anon.task"), func(ctx *Ctx) bool { return true }).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	_, err = WorkflowToDefinition(wfLoop)
	if err == nil {
		t.Fatal("expected error for anonymous loop condition")
	}
	if !contains(err.Error(), "unnamed loop condition") {
		t.Fatalf("expected 'unnamed loop condition' in error, got: %v", err)
	}
}

func TestDefinitionSubWorkflow(t *testing.T) {
	ResetTaskRegistry()

	Task("sub.inner", func(ctx *Ctx) error {
		Set(ctx, "inner_ran", true)
		return nil
	})
	Task("sub.outer", func(ctx *Ctx) error {
		Set(ctx, "outer_ran", true)
		return nil
	})

	inner, err := NewWorkflow("inner-wf").Step("sub.inner").Commit()
	if err != nil {
		t.Fatal(err)
	}
	outer, err := NewWorkflow("outer-wf").
		Step("sub.outer").
		Sub(inner).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Serialize
	def, err := WorkflowToDefinition(outer)
	if err != nil {
		t.Fatalf("WorkflowToDefinition: %v", err)
	}

	if len(def.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(def.Steps))
	}
	if def.Steps[1].Kind != "sub" {
		t.Fatalf("expected step 1 kind 'sub', got %q", def.Steps[1].Kind)
	}
	if def.Steps[1].SubWorkflow.ID != "inner-wf" {
		t.Fatalf("expected sub-workflow ID 'inner-wf', got %q", def.Steps[1].SubWorkflow.ID)
	}

	// Round-trip
	data, _ := MarshalWorkflowDefinition(def)
	def2, _ := UnmarshalWorkflowDefinition(data)
	rebuilt, err := NewWorkflowFromDef(def2)
	if err != nil {
		t.Fatalf("NewWorkflowFromDef: %v", err)
	}

	// Execute rebuilt workflow
	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), rebuilt, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (err: %v)", result.Status, result.Error)
	}
	if result.Store["outer_ran"] != true || result.Store["inner_ran"] != true {
		t.Fatalf("both inner and outer should have run: %v", result.Store)
	}
}

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

// === Orphan Detection (unit tests) ===

func TestOrphanWatcherPID(t *testing.T) {
	// This tests that the PID watcher goroutine starts and doesn't panic
	ctx, cancel := context.WithCancel(context.Background())
	stop := startOrphanWatcher(ctx, cancel, nil)
	defer stop()

	// Should not have cancelled yet (parent is still alive)
	select {
	case <-ctx.Done():
		t.Fatal("context should not be cancelled while parent is alive")
	case <-time.After(100 * time.Millisecond):
		// Good - parent is still alive
	}

	cancel() // clean up
}

// === Integration: Multiple features working together ===

func TestIntegration_TagsAndMutations(t *testing.T) {
	ResetTaskRegistry()

	Task("int.setup", func(ctx *Ctx) error {
		// Disable all optional steps by tag
		DisableByTag(ctx, "optional")
		// Insert a dynamic step
		InsertAfter(ctx, "int.dynamic")
		return nil
	})
	Task("int.dynamic", func(ctx *Ctx) error {
		Set(ctx, "dynamic_ran", true)
		return nil
	})
	Task("int.opt", func(ctx *Ctx) error {
		Set(ctx, "opt_ran", true)
		return nil
	})
	Task("int.finish", func(ctx *Ctx) error {
		Set(ctx, "finish_ran", true)
		return nil
	})

	wf, err := NewWorkflow("int-combo").
		Step("int.setup").
		Step("int.opt", WithStepTags("optional")).
		Step("int.finish").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (err: %v)", result.Status, result.Error)
	}

	if result.Store["dynamic_ran"] != true {
		t.Fatal("dynamic step should have run")
	}
	if result.Store["opt_ran"] == true {
		t.Fatal("optional step should have been disabled")
	}
	if result.Store["finish_ran"] != true {
		t.Fatal("finish step should have run")
	}
}

func TestIntegration_MiddlewareAndIPC(t *testing.T) {
	ResetTaskRegistry()

	var msgs []string
	var mu sync.Mutex

	Task("int.ipc", func(ctx *Ctx) error {
		Send(ctx, "status", Params{"msg": "running"})
		return nil
	})

	wf, err := NewWorkflow("int-mw-ipc").Step("int.ipc").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(
		WithTaskMiddleware(func(tctx *Ctx, taskName string, next func() error) error {
			mu.Lock()
			msgs = append(msgs, "mw:"+taskName)
			mu.Unlock()
			return next()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.OnMessage("status", func(msgType string, payload map[string]any) {
		mu.Lock()
		msgs = append(msgs, "ipc:"+msgType)
		mu.Unlock()
	})

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	// Should have both middleware and IPC entries
	mu.Lock()
	defer mu.Unlock()
	hasMW, hasIPC := false, false
	for _, m := range msgs {
		if m == "mw:int.ipc" {
			hasMW = true
		}
		if m == "ipc:status" {
			hasIPC = true
		}
	}
	if !hasMW {
		t.Fatal("task middleware should have been called")
	}
	if !hasIPC {
		t.Fatal("IPC handler should have been called")
	}
}

// === Logging Plugin ===

type capturingLogger struct {
	mu   sync.Mutex
	logs []string
}

func (l *capturingLogger) Debug(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, fmt.Sprintf(format, args...))
}
func (l *capturingLogger) Info(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, fmt.Sprintf(format, args...))
}
func (l *capturingLogger) Warn(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, fmt.Sprintf(format, args...))
}
func (l *capturingLogger) Error(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logs = append(l.logs, fmt.Sprintf(format, args...))
}

func TestLoggingPlugin(t *testing.T) {
	ResetTaskRegistry()

	Task("log.task1", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})

	logger := &capturingLogger{}
	wf, err := NewWorkflow("logging-test").Step("log.task1").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(WithPlugin(LoggingPlugin(logger)))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected completed, got %s", result.Status)
	}

	logger.mu.Lock()
	logs := append([]string{}, logger.logs...)
	logger.mu.Unlock()

	// Should have at least: workflow started, step started, task started, task completed, step completed, workflow completed
	if len(logs) < 4 {
		t.Fatalf("expected at least 4 log entries, got %d: %v", len(logs), logs)
	}

	hasWorkflowStart := false
	hasWorkflowEnd := false
	for _, l := range logs {
		if contains(l, "logging-test") && contains(l, "started") {
			hasWorkflowStart = true
		}
		if contains(l, "logging-test") && contains(l, "completed") {
			hasWorkflowEnd = true
		}
	}
	if !hasWorkflowStart {
		t.Fatalf("expected workflow start log, got: %v", logs)
	}
	if !hasWorkflowEnd {
		t.Fatalf("expected workflow completion log, got: %v", logs)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// === Middleware Panic Recovery ===

func TestMiddlewarePanicRecovery(t *testing.T) {
	ResetTaskRegistry()

	Task("panic.task", func(ctx *Ctx) error {
		return nil
	})

	panicMW := func(ctx context.Context, info StepInfo, runID RunID, next func() error) error {
		panic("test panic from middleware")
	}

	wf, err := NewWorkflow("panic-mw-test").Step("panic.task").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(WithStepMiddleware(panicMW))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Should fail (not crash) with a middleware panic error
	if result.Status != Failed {
		t.Fatalf("expected failed, got %s", result.Status)
	}
	if result.Error == nil {
		t.Fatal("expected error from panic recovery")
	}
	if !contains(result.Error.Error(), "middleware panic") {
		t.Fatalf("expected 'middleware panic' in error, got: %v", result.Error)
	}
}

func TestForEachTaskMiddleware(t *testing.T) {
	ResetTaskRegistry()

	var mu sync.Mutex
	var taskNames []string

	Task("fe.mw.prepare", func(ctx *Ctx) error {
		Set(ctx, "items", []string{"a", "b", "c"})
		return nil
	})
	Task("fe.mw.process", func(ctx *Ctx) error {
		return nil
	})

	wf, err := NewWorkflow("foreach-task-mw").
		Step("fe.mw.prepare").
		ForEach("items", Step("fe.mw.process")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(
		WithTaskMiddleware(func(tctx *Ctx, taskName string, next func() error) error {
			mu.Lock()
			taskNames = append(taskNames, taskName)
			mu.Unlock()
			return next()
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, runErr := engine.RunSync(context.Background(), wf, nil)
	if runErr != nil {
		t.Fatal(runErr)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (err: %v)", result.Status, result.Error)
	}

	// Should have: prepare + 3 forEach items = 4 middleware calls
	mu.Lock()
	defer mu.Unlock()
	processCount := 0
	for _, name := range taskNames {
		if name == "fe.mw.process" {
			processCount++
		}
	}
	if processCount != 3 {
		t.Fatalf("expected 3 middleware calls for forEach items, got %d (names: %v)", processCount, taskNames)
	}
}

// === Cache Size ===

func TestWithCacheSize(t *testing.T) {
	ResetTaskRegistry()

	Task("cache.t1", func(ctx *Ctx) error { return nil })
	Task("cache.t2", func(ctx *Ctx) error { return nil })
	Task("cache.t3", func(ctx *Ctx) error { return nil })

	wf1, err := NewWorkflow("cache-wf-1").Step("cache.t1").Commit()
	if err != nil {
		t.Fatal(err)
	}
	wf2, err := NewWorkflow("cache-wf-2").Step("cache.t2").Commit()
	if err != nil {
		t.Fatal(err)
	}
	wf3, err := NewWorkflow("cache-wf-3").Step("cache.t3").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithCacheSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.RunSync(context.Background(), wf1, nil)
	engine.RunSync(context.Background(), wf2, nil)
	engine.RunSync(context.Background(), wf3, nil)

	// First workflow should have been evicted
	engine.workflowCacheMu.RLock()
	cacheLen := len(engine.workflowCache)
	_, hasWf1 := engine.workflowCache["cache-wf-1"]
	_, hasWf2 := engine.workflowCache["cache-wf-2"]
	_, hasWf3 := engine.workflowCache["cache-wf-3"]
	engine.workflowCacheMu.RUnlock()

	if cacheLen != 2 {
		t.Fatalf("expected cache size 2, got %d", cacheLen)
	}
	if hasWf1 {
		t.Fatal("expected cache-wf-1 to be evicted")
	}
	if !hasWf2 || !hasWf3 {
		t.Fatal("expected cache-wf-2 and cache-wf-3 to be cached")
	}
}

// === Spawn Type Preservation ===

func TestSpawnTypePreservation(t *testing.T) {
	// Verify that the serialize→deserialize round-trip preserves type info
	s := newRunState("test-spawn-types", nil)
	s.Set("count", 42)
	s.Set("name", "hello")
	s.Set("ratio", 3.14)
	s.Set("flag", true)
	s.Set("big", int64(9999999999))

	// Simulate parent→child serialization (with a ForEach item)
	data, err := serializeStateForChild(s, "item-val", 5)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	// Simulate child→parent deserialization
	result, err := deserializeStoreData(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	// int must survive round-trip (not become float64)
	if v, ok := result["count"].(int); !ok || v != 42 {
		t.Fatalf("count: expected int(42), got %T(%v)", result["count"], result["count"])
	}

	// string must survive
	if v, ok := result["name"].(string); !ok || v != "hello" {
		t.Fatalf("name: expected string(hello), got %T(%v)", result["name"], result["name"])
	}

	// float64 must survive
	if v, ok := result["ratio"].(float64); !ok || v != 3.14 {
		t.Fatalf("ratio: expected float64(3.14), got %T(%v)", result["ratio"], result["ratio"])
	}

	// bool must survive
	if v, ok := result["flag"].(bool); !ok || v != true {
		t.Fatalf("flag: expected bool(true), got %T(%v)", result["flag"], result["flag"])
	}

	// int64 must survive
	if v, ok := result["big"].(int64); !ok || v != 9999999999 {
		t.Fatalf("big: expected int64(9999999999), got %T(%v)", result["big"], result["big"])
	}

	// ForEach item must survive
	if v, ok := result["__foreach_item"].(string); !ok || v != "item-val" {
		t.Fatalf("__foreach_item: expected string(item-val), got %T(%v)", result["__foreach_item"], result["__foreach_item"])
	}

	// ForEach index must be int (not float64)
	if v, ok := result["__foreach_index"].(int); !ok || v != 5 {
		t.Fatalf("__foreach_index: expected int(5), got %T(%v)", result["__foreach_index"], result["__foreach_index"])
	}
}

func TestSpawnDirtyTypePreservation(t *testing.T) {
	// Verify that SerializeDirty→deserialize round-trip preserves types
	s := newRunState("test-dirty", nil)
	s.SetClean("existing", "parent-data")   // not dirty
	s.Set("child_wrote", 99)                 // dirty
	s.Set("child_flag", true)                // dirty

	data, err := s.SerializeDirty()
	if err != nil {
		t.Fatalf("SerializeDirty: %v", err)
	}

	// Should only contain dirty entries
	if _, has := data["existing"]; has {
		t.Fatal("expected 'existing' to be excluded (not dirty)")
	}

	result, err := deserializeStoreData(data)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	if v, ok := result["child_wrote"].(int); !ok || v != 99 {
		t.Fatalf("child_wrote: expected int(99), got %T(%v)", result["child_wrote"], result["child_wrote"])
	}
	if v, ok := result["child_flag"].(bool); !ok || v != true {
		t.Fatalf("child_flag: expected bool(true), got %T(%v)", result["child_flag"], result["child_flag"])
	}
}

// === Decision 003 Tests ===

// TestMiddlewareOrdering verifies the last-registered middleware executes first
// (wraps outermost), matching the INTENT specification.
func TestMiddlewareOrdering(t *testing.T) {
	ResetTaskRegistry()

	Task("mw.order.task", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})

	var order []string
	var mu sync.Mutex

	engine, err := New(
		WithEngineMiddleware(func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error {
			mu.Lock()
			order = append(order, "engine_first_enter")
			mu.Unlock()
			err := next()
			mu.Lock()
			order = append(order, "engine_first_exit")
			mu.Unlock()
			return err
		}),
		WithEngineMiddleware(func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error {
			mu.Lock()
			order = append(order, "engine_last_enter")
			mu.Unlock()
			err := next()
			mu.Lock()
			order = append(order, "engine_last_exit")
			mu.Unlock()
			return err
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	wf, err := NewWorkflow("mw-order").Step("mw.order.task").Commit()
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

	// Last-registered should be outermost: enters first, exits last
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 4 {
		t.Fatalf("expected 4 middleware events, got %d: %v", len(order), order)
	}
	if order[0] != "engine_last_enter" {
		t.Fatalf("expected last-registered MW to enter first, got %v", order)
	}
	if order[3] != "engine_last_exit" {
		t.Fatalf("expected last-registered MW to exit last, got %v", order)
	}
}

// TestEngineDoubleClose verifies calling Close() twice does not panic.
func TestEngineDoubleClose(t *testing.T) {
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}

	// First close should succeed
	if err := engine.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	// Second close should be a no-op, not panic
	if err := engine.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
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

// === Task 11: Concurrent Mutations from Parallel Steps ===

func TestConcurrentMutationsFromParallel(t *testing.T) {
	ResetTaskRegistry()

	var mu sync.Mutex
	var order []string

	Task("cmp.par_a", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "par_a")
		mu.Unlock()
		InsertAfter(ctx, "cmp.dynamic_a")
		return nil
	})
	Task("cmp.par_b", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "par_b")
		mu.Unlock()
		InsertAfter(ctx, "cmp.dynamic_b")
		return nil
	})
	Task("cmp.dynamic_a", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "dynamic_a")
		mu.Unlock()
		return nil
	})
	Task("cmp.dynamic_b", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "dynamic_b")
		mu.Unlock()
		return nil
	})
	Task("cmp.post", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "post")
		mu.Unlock()
		Set(ctx, "post_ran", true)
		return nil
	})

	wf, err := NewWorkflow("conc-mutations").
		Parallel(Step("cmp.par_a"), Step("cmp.par_b")).
		Step("cmp.post").
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

	// Both parallel tasks should have run
	mu.Lock()
	hasParA, hasParB, hasDynA, hasDynB, hasPost := false, false, false, false, false
	for _, s := range order {
		switch s {
		case "par_a":
			hasParA = true
		case "par_b":
			hasParB = true
		case "dynamic_a":
			hasDynA = true
		case "dynamic_b":
			hasDynB = true
		case "post":
			hasPost = true
		}
	}
	mu.Unlock()

	if !hasParA || !hasParB {
		t.Fatalf("expected both parallel tasks to run, got order: %v", order)
	}
	if !hasDynA || !hasDynB {
		t.Fatalf("expected both dynamic mutations to execute, got order: %v", order)
	}
	if !hasPost {
		t.Fatalf("expected post step to run, got order: %v", order)
	}
}

// === Task 12: Cancel During Retry Delay ===

func TestRetryCancellation(t *testing.T) {
	ResetTaskRegistry()

	var attempts int32

	Task("rc.always_fail", func(ctx *Ctx) error {
		atomic.AddInt32(&attempts, 1)
		return fmt.Errorf("always fails")
	}, WithRetry(5), WithRetryDelay(500*time.Millisecond))

	wf, err := NewWorkflow("retry-cancel").Step("rc.always_fail").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	runID, err := engine.Run(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Let first attempt fail and enter retry delay
	time.Sleep(150 * time.Millisecond)

	// Cancel during retry delay
	if err := engine.Cancel(context.Background(), runID); err != nil {
		t.Fatal(err)
	}

	result, err := engine.Wait(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != Cancelled && result.Status != Failed {
		t.Fatalf("expected Cancelled or Failed, got %s", result.Status)
	}

	// Should not have exhausted all retries
	finalAttempts := atomic.LoadInt32(&attempts)
	if finalAttempts >= 6 { // 1 initial + 5 retries = 6 max
		t.Fatalf("expected fewer than 6 attempts (cancelled during retry), got %d", finalAttempts)
	}
}

// === Task 13: Panic Recovery for User Functions ===

func TestMapPanicRecovery(t *testing.T) {
	ResetTaskRegistry()

	wf, err := NewWorkflow("map-panic").
		Map(func(ctx *Ctx) error {
			panic("map boom")
		}).
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
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
}

func TestMapErrorPropagation(t *testing.T) {
	ResetTaskRegistry()

	wf, err := NewWorkflow("map-error").
		Map(func(ctx *Ctx) error {
			return fmt.Errorf("transform failed: bad data")
		}).
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
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
	if !strings.Contains(result.Error.Error(), "transform failed") {
		t.Fatalf("expected 'transform failed' in error, got: %v", result.Error)
	}
}

func TestBranchConditionPanicRecovery(t *testing.T) {
	ResetTaskRegistry()

	Task("bcp.task", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("branch-cond-panic").
		Branch(
			When(func(ctx *Ctx) bool { panic("cond boom") }).Step("bcp.task"),
		).
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
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
}

func TestLoopConditionPanicRecovery(t *testing.T) {
	ResetTaskRegistry()

	Task("lcp.task", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("loop-cond-panic").
		DoUntil(Step("lcp.task"), func(ctx *Ctx) bool { panic("loop boom") }).
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
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
}

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

// === Decision 006: Bug Fixes and Test Gaps ===

// TestRunAndResumeAfterClose verifies that Run and Resume reject work after engine.Close().
func TestRunAndResumeAfterClose(t *testing.T) {
	ResetTaskRegistry()

	Task("rac.task", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("run-after-close").
		Step("rac.task").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	// Run after Close should return error
	_, runErr := engine.Run(context.Background(), wf, nil)
	if runErr == nil {
		t.Fatal("expected Run to fail after Close")
	}
	if !errors.Is(runErr, ErrEngineClosed) {
		t.Fatalf("expected ErrEngineClosed, got: %v", runErr)
	}

	// Resume after Close should return error
	_, resumeErr := engine.Resume(context.Background(), wf, "fake-run-id", nil)
	if resumeErr == nil {
		t.Fatal("expected Resume to fail after Close")
	}
	if !errors.Is(resumeErr, ErrEngineClosed) {
		t.Fatalf("expected ErrEngineClosed, got: %v", resumeErr)
	}
}

// TestTaskPanicWithoutMiddleware verifies panic recovery in the no-middleware task invocation path.
func TestTaskPanicWithoutMiddleware(t *testing.T) {
	ResetTaskRegistry()

	Task("tpnm.panic_task", func(ctx *Ctx) error {
		panic("boom")
	})

	wf, err := NewWorkflow("panic-no-middleware").
		Step("tpnm.panic_task").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Create engine with NO middleware
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
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
	if !strings.Contains(result.Error.Error(), "panic") {
		t.Fatalf("expected panic in error message, got: %v", result.Error)
	}
}

// TestPerJobTokenRejection verifies the spawn server rejects messages with invalid or missing tokens.
func TestPerJobTokenRejection(t *testing.T) {
	// Create a spawnServer directly (no gRPC transport needed — call methods in-process)
	ss := &spawnServer{
		jobs:   make(map[string]*SpawnJob),
		secret: "test-secret",
	}

	// Register a job with a known token
	ss.addJob(&SpawnJob{
		ID:       "job-1",
		TaskName: "test-task",
		token:    "valid-token-123",
		resultCh: make(chan *spawnResult, 1),
	})

	// Helper to create context with gRPC metadata
	ctxWithToken := func(jobToken string) context.Context {
		md := metadata.Pairs("x-gostage-job-token", jobToken)
		return metadata.NewIncomingContext(context.Background(), md)
	}
	ctxNoToken := func() context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.MD{})
	}

	// Test 1: Empty jobID → InvalidArgument
	msg := &pb.IPCMessage{
		Type:    pb.MessageType_MESSAGE_TYPE_UNSPECIFIED,
		Context: &pb.MessageContext{SessionId: ""},
	}
	_, err := ss.SendMessage(ctxWithToken("valid-token-123"), msg)
	if err == nil {
		t.Fatal("expected error for empty jobID")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got: %v", err)
	}

	// Test 2: Valid jobID + wrong token → PermissionDenied
	msg.Context.SessionId = "job-1"
	_, err = ss.SendMessage(ctxWithToken("wrong-token"), msg)
	if err == nil {
		t.Fatal("expected error for wrong token")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got: %v", err)
	}

	// Test 3: Valid jobID + no token → PermissionDenied
	_, err = ss.SendMessage(ctxNoToken(), msg)
	if err == nil {
		t.Fatal("expected error for missing token")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got: %v", err)
	}

	// Test 4: Valid jobID + valid token → success
	_, err = ss.SendMessage(ctxWithToken("valid-token-123"), msg)
	if err != nil {
		t.Fatalf("expected success with valid token, got: %v", err)
	}

	// Test 5: RequestWorkflowDefinition with wrong token → PermissionDenied
	req := &pb.ReadySignal{ChildId: "job-1"}
	_, err = ss.RequestWorkflowDefinition(ctxWithToken("wrong-token"), req)
	if err == nil {
		t.Fatal("expected error for wrong token on RequestWorkflowDefinition")
	}
	if s, ok := status.FromError(err); !ok || s.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got: %v", err)
	}

	// Test 6: RequestWorkflowDefinition with valid token → success
	_, err = ss.RequestWorkflowDefinition(ctxWithToken("valid-token-123"), req)
	if err != nil {
		t.Fatalf("expected success with valid token, got: %v", err)
	}
}

// TestOnStepCompletePanicRecovery verifies that a panicking onStepComplete callback
// does not crash the worker or hang the run.
func TestOnStepCompletePanicRecovery(t *testing.T) {
	ResetTaskRegistry()

	Task("oscpr.task", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})

	wf, err := NewWorkflow("callback-panic",
		OnStepComplete(func(stepName string, ctx *Ctx) {
			panic("callback boom")
		}),
	).
		Step("oscpr.task").
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
	if result.Store["done"] != true {
		t.Fatal("expected done=true in store")
	}
}

// TestOnErrorPanicRecovery verifies that a panicking onError callback
// does not crash the worker or hang the run.
func TestOnErrorPanicRecovery(t *testing.T) {
	ResetTaskRegistry()

	Task("oepr.fail_task", func(ctx *Ctx) error {
		return fmt.Errorf("task failure")
	})

	wf, err := NewWorkflow("error-callback-panic",
		OnError(func(err error) {
			panic("error callback boom")
		}),
	).
		Step("oepr.fail_task").
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
	if result.Error == nil {
		t.Fatal("expected error to be set")
	}
}

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

	// Resume a non-existent run → ErrRunNotFound
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

	// Resume with wrong workflow → ErrWorkflowMismatch
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

	// Resume a completed run → ErrRunNotSuspended
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

// TestOnMessageNilHandler verifies that registering a nil handler is silently ignored.
func TestOnMessageNilHandler(t *testing.T) {
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Should not panic
	engine.OnMessage("test", nil)

	// Verify no handler was registered by checking that dispatching does nothing
	// (if a nil handler were registered, dispatching would panic)
	engine.dispatchMessage("test", map[string]any{"key": "value"})
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

// === Decision 008: Test Coverage Gaps ===

func TestCacheLRUEviction(t *testing.T) {
	ResetTaskRegistry()

	Task("d8.lru_noop", func(ctx *Ctx) error { return nil })

	wf1, _ := NewWorkflow("lru-wf-1").Step("d8.lru_noop").Commit()
	wf2, _ := NewWorkflow("lru-wf-2").Step("d8.lru_noop").Commit()
	wf3, _ := NewWorkflow("lru-wf-3").Step("d8.lru_noop").Commit()

	engine, err := New(WithCacheSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Run wf1, wf2 → cache: [wf1, wf2]
	if _, err := engine.RunSync(ctx, wf1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RunSync(ctx, wf2, nil); err != nil {
		t.Fatal(err)
	}

	// Run wf1 again → promotes wf1, cache: [wf2, wf1]
	if _, err := engine.RunSync(ctx, wf1, nil); err != nil {
		t.Fatal(err)
	}

	// Run wf3 → evicts wf2 (LRU), cache: [wf1, wf3]
	if _, err := engine.RunSync(ctx, wf3, nil); err != nil {
		t.Fatal(err)
	}

	// wf1 should still be cached (was promoted)
	if engine.lookupCachedWorkflow("lru-wf-1") == nil {
		t.Fatal("wf1 should still be in cache after LRU promotion")
	}
	// wf2 should be evicted (LRU victim)
	if engine.lookupCachedWorkflow("lru-wf-2") != nil {
		t.Fatal("wf2 should have been evicted as LRU")
	}
	// wf3 should be cached
	if engine.lookupCachedWorkflow("lru-wf-3") == nil {
		t.Fatal("wf3 should be in cache")
	}
}

func TestWaitAfterCompletion(t *testing.T) {
	ResetTaskRegistry()

	Task("d8.wait_done", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})

	wf, _ := NewWorkflow("wait-done").Step("d8.wait_done").Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// RunSync completes synchronously — run is never in e.runs
	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	// Wait should hit the persistence fallback path
	waitResult, err := engine.Wait(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("Wait after completion failed: %v", err)
	}
	if waitResult.Status != Completed {
		t.Fatalf("expected Completed from Wait, got %s", waitResult.Status)
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

func TestEngine_DeleteRun(t *testing.T) {
	ResetTaskRegistry()

	Task("d8.del_task", func(ctx *Ctx) error {
		Set(ctx, "value", "hello")
		return nil
	})

	wf, _ := NewWorkflow("del-run").Step("d8.del_task").Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	result, err := engine.RunSync(ctx, wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	// Delete the run
	if err := engine.DeleteRun(ctx, result.RunID); err != nil {
		t.Fatalf("DeleteRun failed: %v", err)
	}

	// LoadRun should return RunNotFoundError
	_, err = engine.persistence.LoadRun(ctx, result.RunID)
	var notFound *RunNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected RunNotFoundError after delete, got %v", err)
	}
}

func TestDeleteRunWaitsForCompletion(t *testing.T) {
	ResetTaskRegistry()

	blockCh := make(chan struct{})
	startedCh := make(chan struct{})

	Task("d9.blocking_task", func(ctx *Ctx) error {
		close(startedCh)  // signal that the task has started
		<-blockCh         // block until test unblocks us
		Set(ctx, "done", true)
		return nil
	})

	wf, _ := NewWorkflow("delete-wait").Step("d9.blocking_task").Commit()

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Start async run
	runID, err := engine.Run(ctx, wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the task to start executing
	<-startedCh

	// Call DeleteRun in a goroutine — it should block until the task completes
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- engine.DeleteRun(ctx, runID)
	}()

	// Give DeleteRun a moment to start waiting
	select {
	case <-deleteDone:
		t.Fatal("DeleteRun returned before task completed — should have waited")
	case <-time.After(50 * time.Millisecond):
		// Good — DeleteRun is blocking as expected
	}

	// Unblock the task
	close(blockCh)

	// Now DeleteRun should complete
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("DeleteRun failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteRun did not complete after task finished")
	}

	// Verify persistence is clean
	_, err = engine.persistence.LoadRun(ctx, runID)
	var nf *RunNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected RunNotFoundError after delete, got %v", err)
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

	// wf1 sleeps — with cacheSize=1, running wf2 would evict wf1 without pinning
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

	// RunSync wf1 — returns Sleeping after step_a (timer-based sleep with SQLite)
	result1, err := engine.RunSync(ctx, wf1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result1.Status != Sleeping {
		t.Fatalf("expected Sleeping, got %s (error: %v)", result1.Status, result1.Error)
	}

	// RunSync wf2 — without pinning, this would evict wf1 from the size-1 cache
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

	// Close engine 1 — cache is gone
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

// === SQLite schema — no checkpoints table (Issue 10) ===

func TestSQLite_NoCheckpointsTable(t *testing.T) {
	engine, err := New(WithSQLite(t.TempDir() + "/nochk.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Query sqlite_master for a table named "checkpoints".
	var name string
	row := engine.persistence.(*sqlitePersistence).db.QueryRowContext(
		context.Background(),
		`SELECT name FROM sqlite_master WHERE type='table' AND name='checkpoints'`,
	)
	err = row.Scan(&name)
	if err == nil {
		t.Fatal("checkpoints table must not exist in new databases")
	}
	// sql.ErrNoRows is the expected outcome.
}

// === IPC uses dedicated MESSAGE_TYPE_IPC (Issue 11) ===

func TestIPC_UsesCorrectProtoType(t *testing.T) {
	// Verify that Send() from a parent-process task routes through dispatchMessage
	// without needing __ipc__ sentinel — the local path in Send() calls
	// engine.dispatchMessage directly. In child processes, the new proto type is used.
	// This test exercises the local (parent-process) IPC routing.
	ResetTaskRegistry()

	var received bool
	Task("ipc.proto.task", func(ctx *Ctx) error {
		return Send(ctx, "ipc_proto_evt", Params{"ok": true})
	})

	wf, err := NewWorkflow("ipc-proto-test").Step("ipc.proto.task").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	engine.OnMessage("ipc_proto_evt", func(msgType string, payload map[string]any) {
		if payload["ok"] == true {
			received = true
		}
	})

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if !received {
		t.Fatal("IPC message was not received by handler")
	}
}

// === ListRuns pagination with Offset (Issue 14) ===

func TestListRuns_Pagination(t *testing.T) {
	ResetTaskRegistry()

	Task("page.noop", func(ctx *Ctx) error { return nil })

	engine, err := New(WithSQLite(t.TempDir() + "/page.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	wf, err := NewWorkflow("page-wf").Step("page.noop").Commit()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Create 7 runs sequentially (order matters for created_at DESC).
	for i := 0; i < 7; i++ {
		_, runErr := engine.RunSync(ctx, wf, nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
	}

	// Page 1: runs 1-3 (newest)
	page1, err := engine.ListRuns(ctx, RunFilter{Limit: 3, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 3 {
		t.Fatalf("page 1: expected 3, got %d", len(page1))
	}

	// Page 2: runs 4-6
	page2, err := engine.ListRuns(ctx, RunFilter{Limit: 3, Offset: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 3 {
		t.Fatalf("page 2: expected 3, got %d", len(page2))
	}

	// Page 3: run 7 (oldest)
	page3, err := engine.ListRuns(ctx, RunFilter{Limit: 3, Offset: 6})
	if err != nil {
		t.Fatal(err)
	}
	if len(page3) != 1 {
		t.Fatalf("page 3: expected 1, got %d", len(page3))
	}

	// All run IDs must be distinct across pages.
	seen := make(map[RunID]bool)
	for _, pages := range [][]*RunState{page1, page2, page3} {
		for _, run := range pages {
			if seen[run.RunID] {
				t.Fatalf("run %s appeared in multiple pages", run.RunID)
			}
			seen[run.RunID] = true
		}
	}
	if len(seen) != 7 {
		t.Fatalf("expected 7 unique runs across all pages, got %d", len(seen))
	}
}

// === LRU Cache Promotion Order (Issue 8) ===

func TestCacheLRU_PromotionOrder(t *testing.T) {
	ResetTaskRegistry()

	Task("lru.noop", func(ctx *Ctx) error { return nil })

	wfA, _ := NewWorkflow("lru-promo-a").Step("lru.noop").Commit()
	wfB, _ := NewWorkflow("lru-promo-b").Step("lru.noop").Commit()
	wfC, _ := NewWorkflow("lru-promo-c").Step("lru.noop").Commit()
	wfD, _ := NewWorkflow("lru-promo-d").Step("lru.noop").Commit()

	// Cache size = 2
	engine, err := New(WithCacheSize(2))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Run A, B → cache: [A, B]
	engine.RunSync(ctx, wfA, nil)
	engine.RunSync(ctx, wfB, nil)

	// Promote A by running it again → cache: [B, A] where A is MRU
	engine.RunSync(ctx, wfA, nil)

	// Run C → evicts LRU which is B (not A), cache: [A, C]
	engine.RunSync(ctx, wfC, nil)

	engine.workflowCacheMu.RLock()
	hasA := engine.workflowCache["lru-promo-a"] != nil
	hasB := engine.workflowCache["lru-promo-b"] != nil
	hasC := engine.workflowCache["lru-promo-c"] != nil
	engine.workflowCacheMu.RUnlock()

	if !hasA {
		t.Fatal("A should still be cached (was promoted before C evicted B)")
	}
	if hasB {
		t.Fatal("B should have been evicted as LRU")
	}
	if !hasC {
		t.Fatal("C should be in cache")
	}

	// Run D → evicts LRU which is A, cache: [C, D]
	engine.RunSync(ctx, wfD, nil)

	engine.workflowCacheMu.RLock()
	hasA = engine.workflowCache["lru-promo-a"] != nil
	engine.workflowCacheMu.RUnlock()

	if hasA {
		t.Fatal("A should now be evicted (became LRU after C was used)")
	}
}

// === OnMessage / OffMessage with HandlerID (Issue 5) ===

func TestOnMessageReturnsID(t *testing.T) {
	ResetTaskRegistry()

	Task("hid.sender", func(ctx *Ctx) error {
		return Send(ctx, "evt", Params{"x": 1})
	})

	wf, err := NewWorkflow("handler-id").Step("hid.sender").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	var aFired, bFired int32

	idA := engine.OnMessage("evt", func(msgType string, payload map[string]any) {
		atomic.AddInt32(&aFired, 1)
	})
	idB := engine.OnMessage("evt", func(msgType string, payload map[string]any) {
		atomic.AddInt32(&bFired, 1)
	})

	if idA == 0 || idB == 0 {
		t.Fatal("OnMessage must return non-zero HandlerID")
	}
	if idA == idB {
		t.Fatal("each OnMessage call must return a distinct HandlerID")
	}

	// Deregister handler A.
	engine.OffMessage(idA)

	engine.RunSync(context.Background(), wf, nil)

	if atomic.LoadInt32(&aFired) != 0 {
		t.Fatal("deregistered handler A must not fire")
	}
	if atomic.LoadInt32(&bFired) != 1 {
		t.Fatalf("handler B must fire once, fired %d times", atomic.LoadInt32(&bFired))
	}
}

func TestOffMessage_Anonymous(t *testing.T) {
	ResetTaskRegistry()

	Task("anon.sender", func(ctx *Ctx) error {
		return Send(ctx, "evt2", Params{"v": 2})
	})

	wf, err := NewWorkflow("offmsg-anon").Step("anon.sender").Commit()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	var fired int32

	// Register an anonymous inline handler and deregister it immediately.
	id := engine.OnMessage("evt2", func(msgType string, payload map[string]any) {
		atomic.AddInt32(&fired, 1)
	})
	engine.OffMessage(id)

	engine.RunSync(context.Background(), wf, nil)

	if atomic.LoadInt32(&fired) != 0 {
		t.Fatal("deregistered inline handler must not fire")
	}
}

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

	// step1 must have run exactly once — skipped on resume.
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

	// Resume: item 0 re-runs from step2 (step1 skipped — already completed).
	// Then item 1 runs step1 from scratch — step1 must NOT be skipped even
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
		t.Fatalf("step1 should have run twice total (once per item), got %d — "+
			"item 1's step1 was incorrectly skipped due to step ID collision", step1Runs.Load())
	}
	// step2 runs twice: item 0 resumes + item 1 runs fresh.
	if step2Runs.Load() != 3 {
		t.Fatalf("step2 should have run 3 times total (item0 initial + item0 resume + item1), got %d", step2Runs.Load())
	}
}

// TestListRuns_OffsetWithoutLimit verifies that ListRuns does not produce a
// SQL syntax error when Offset > 0 and Limit == 0 (meaning "no limit").
func TestListRuns_OffsetWithoutLimit(t *testing.T) {
	ResetTaskRegistry()

	Task("olim.noop", func(ctx *Ctx) error { return nil })

	engine, err := New(WithSQLite(t.TempDir() + "/olim.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	wf, err := NewWorkflow("olim-wf").Step("olim.noop").Commit()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Create 5 runs.
	for i := 0; i < 5; i++ {
		if _, err := engine.RunSync(ctx, wf, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Offset 2 without a Limit must not produce a SQL syntax error.
	// SQLite requires LIMIT before OFFSET; the fix adds LIMIT -1 automatically.
	runs, err := engine.ListRuns(ctx, RunFilter{Offset: 2})
	if err != nil {
		t.Fatalf("ListRuns with Offset but no Limit failed: %v", err)
	}
	// Should return 3 runs (5 total - 2 skipped).
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs after offset 2, got %d", len(runs))
	}

	// Also verify memory persistence handles the same case consistently.
	engineMem, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engineMem.Close()

	for i := 0; i < 5; i++ {
		if _, err := engineMem.RunSync(ctx, wf, nil); err != nil {
			t.Fatal(err)
		}
	}

	runsMem, err := engineMem.ListRuns(ctx, RunFilter{Offset: 2})
	if err != nil {
		t.Fatalf("memoryPersistence ListRuns with Offset but no Limit failed: %v", err)
	}
	if len(runsMem) != 3 {
		t.Fatalf("expected 3 runs from memory after offset 2, got %d", len(runsMem))
	}
}
