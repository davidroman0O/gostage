package gostage

import (
	"context"
	"fmt"
	"sync"
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
			return Suspend(ctx, P{"reason": "need approval"})
		}
		Set(ctx, "step2_done", true)
		return nil
	})
	Task("resume.step3", func(ctx *Ctx) error {
		atomic.AddInt32(&step3Count, 1)
		Set(ctx, "step3_done", true)
		return nil
	})

	wf := NewWorkflow("step-resume").
		Step("resume.step1").
		Step("resume.step2").
		Step("resume.step3").
		Commit()

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
	result, err = engine.Resume(context.Background(), wf, result.RunID, P{"approved": true})
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
			return Suspend(ctx, P{"waiting": item})
		}
		Set(ctx, fmt.Sprintf("processed_%d", idx), item)
		_ = count
		return nil
	})

	Task("resume.done", func(ctx *Ctx) error {
		Set(ctx, "done", true)
		return nil
	})

	wf := NewWorkflow("foreach-resume").
		Step("resume.prepare").
		ForEach("items", Step("resume.process")).
		Step("resume.done").
		Commit()

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
	result, err = engine.Resume(context.Background(), wf, result.RunID, P{})
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
	defer p.Shutdown()

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
	p.Shutdown()

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

	wf := NewWorkflow("pool-bounded").Step("pool.slow").Commit()

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

	wf := NewWorkflow("tagged-steps").
		Step("tag.validate", WithStepTags("validation")).
		Step("tag.charge", WithStepTags("billing", "critical")).
		Commit()

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

	wf := NewWorkflow("tagged-workflow", WithWorkflowTags("billing", "critical")).
		Step("tag.noop").
		Commit()

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

	wf := NewWorkflow("find-by-tag").
		Step("tag.a").
		Step("tag.b", WithStepTags("optional")).
		Step("tag.c", WithStepTags("optional")).
		Commit()

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
		return Send(ctx, "progress", P{"pct": 50})
	})

	wf := NewWorkflow("on-message").Step("ipc.sender").Commit()
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
		Send(ctx, "type_a", P{"x": 1})
		Send(ctx, "type_b", P{"x": 2})
		return nil
	})

	wf := NewWorkflow("wildcard-handler").Step("ipc.multi").Commit()
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

func TestTimerPopulate(t *testing.T) {
	var woken int32

	ts := newTimerScheduler(func(runID RunID) {
		atomic.AddInt32(&woken, 1)
	})
	defer ts.Stop()

	runs := []*RunState{
		{RunID: "sleep-1", Status: Sleeping, WakeAt: time.Now().Add(50 * time.Millisecond)},
		{RunID: "sleep-2", Status: Sleeping, WakeAt: time.Now().Add(100 * time.Millisecond)},
		{RunID: "other", Status: Running},
	}
	ts.Populate(runs)

	if ts.Pending() != 2 {
		t.Fatalf("expected 2 pending, got %d", ts.Pending())
	}

	time.Sleep(200 * time.Millisecond)

	if atomic.LoadInt32(&woken) != 2 {
		t.Fatalf("expected 2 woken, got %d", woken)
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

	wf := NewWorkflow("engine-mw").Step("mw.task").Commit()
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

	wf := NewWorkflow("step-mw").Step("mw.a").Step("mw.b").Commit()
	engine, err := New(
		WithStepMiddleware(func(ctx context.Context, s *step, runID RunID, next func() error) error {
			mu.Lock()
			stepNames = append(stepNames, s.name)
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

	wf := NewWorkflow("task-mw").Step("mw.t1").Commit()
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

	wf := NewWorkflow("middleware-plugin").Step("mw.plug").Commit()
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
	return func(ctx context.Context, s *step, runID RunID, next func() error) error {
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

	wfMW := func(ctx context.Context, s *step, runID RunID, next func() error) error {
		mu.Lock()
		wfStepNames = append(wfStepNames, s.name)
		mu.Unlock()
		return next()
	}

	wf := NewWorkflow("per-workflow-mw", WithWorkflowMiddleware(wfMW)).
		Step("mw.wf1").Step("mw.wf2").Commit()

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

	wf := NewWorkflow("insert-after").
		Step("mut.first").
		Step("mut.last").
		Commit()

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

	wf := NewWorkflow("disable-step").
		Step("mut.enable").
		Step("mut.skip").
		Step("mut.end").
		Commit()

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

	wf := NewWorkflow("disable-by-tag").
		Step("mut.controller").
		Step("mut.opt1", WithStepTags("optional")).
		Step("mut.opt2", WithStepTags("optional")).
		Step("mut.required").
		Commit()

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
			return Suspend(ctx, P{"reason": "wait"})
		}
		Set(ctx, "resumed", true)
		return nil
	})
	Task("mut.final", func(ctx *Ctx) error {
		Set(ctx, "final_ran", true)
		return nil
	})

	wf := NewWorkflow("insert-survives-resume").
		Step("mut.inserter").
		Step("mut.suspender").
		Step("mut.final").
		Commit()

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
	result, err = engine.Resume(ctx, wf, result.RunID, P{"go": true})
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
			return Suspend(ctx, P{"reason": "wait"})
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

	wf := NewWorkflow("disable-survives-resume").
		Step("mut.disabler").
		Step("mut.suspender2").
		Step("mut.skipped").
		Step("mut.end2").
		Commit()

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
	result, err = engine.Resume(ctx, wf, result.RunID, P{"go": true})
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

	wf := NewWorkflow("order-def").
		Step("def.validate").
		Step("def.charge").
		Commit()

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

	wf := NewWorkflow("serial-def").Step("def.a").Step("def.b").Commit()
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

	wf := NewWorkflow("rebuild-def").Step("def.step1").Step("def.step2").Commit()
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
	MapFn("test-map", func(ctx *Ctx) {})
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
		MapFn("test-map", func(ctx *Ctx) {})
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
	MapFn("nb.transform", func(ctx *Ctx) {})

	// WhenNamed
	bc := WhenNamed("nb.cond").Step("nb.task")
	if bc.condName != "nb.cond" {
		t.Fatalf("expected condName 'nb.cond', got %q", bc.condName)
	}
	if bc.condition == nil {
		t.Fatal("expected condition function to be set")
	}

	// MapNamed
	wf := NewWorkflow("nb-test").
		MapNamed("nb.transform").
		Commit()
	if wf.steps[0].mapFnName != "nb.transform" {
		t.Fatalf("expected mapFnName 'nb.transform', got %q", wf.steps[0].mapFnName)
	}
	if wf.steps[0].mapFn == nil {
		t.Fatal("expected mapFn to be set")
	}

	// DoUntilNamed
	wf2 := NewWorkflow("nb-until").
		DoUntilNamed(Step("nb.task"), "nb.cond").
		Commit()
	if wf2.steps[0].loopCondName != "nb.cond" {
		t.Fatalf("expected loopCondName 'nb.cond', got %q", wf2.steps[0].loopCondName)
	}

	// DoWhileNamed
	wf3 := NewWorkflow("nb-while").
		DoWhileNamed(Step("nb.task"), "nb.cond").
		Commit()
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
	MapFn("all.transform", func(ctx *Ctx) {})

	// Build a sub-workflow for stepSub
	subWf := NewWorkflow("all-sub").Step("all.task3").Commit()

	wf := NewWorkflow("all-kinds").
		Step("all.task1").                                           // stepSingle
		Stage("validation", Step("all.task1"), Step("all.task2")).   // stepStage
		Parallel(Step("all.task1"), Step("all.task2")).              // stepParallel
		Branch(                                                     // stepBranch
			WhenNamed("all.is-ready").Step("all.task1"),
			Default().Step("all.task2"),
		).
		ForEach("items", Step("all.task1"), WithConcurrency(3)).    // stepForEach
		MapNamed("all.transform").                                  // stepMap
		DoUntilNamed(Step("all.loop"), "all.is-ready").             // stepDoUntil
		DoWhileNamed(Step("all.loop"), "all.has-more").             // stepDoWhile
		Sub(subWf).                                                 // stepSub
		Sleep(5 * time.Second).                                     // stepSleep
		Commit()

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
	expectedStepKinds := []stepKind{stepSingle, stepStage, stepParallel, stepBranch, stepForEach, stepMap, stepDoUntil, stepDoWhile, stepSub, stepSleep}
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
	wfBranch := NewWorkflow("anon-branch").
		Branch(When(func(ctx *Ctx) bool { return true }).Step("anon.task")).
		Commit()
	_, err := WorkflowToDefinition(wfBranch)
	if err == nil {
		t.Fatal("expected error for anonymous branch condition")
	}
	if !contains(err.Error(), "unnamed condition") {
		t.Fatalf("expected 'unnamed condition' in error, got: %v", err)
	}

	// Anonymous map function → error
	wfMap := NewWorkflow("anon-map").
		Map(func(ctx *Ctx) {}).
		Commit()
	_, err = WorkflowToDefinition(wfMap)
	if err == nil {
		t.Fatal("expected error for anonymous map function")
	}
	if !contains(err.Error(), "unnamed map function") {
		t.Fatalf("expected 'unnamed map function' in error, got: %v", err)
	}

	// Anonymous loop condition → error
	wfLoop := NewWorkflow("anon-loop").
		DoUntil(Step("anon.task"), func(ctx *Ctx) bool { return true }).
		Commit()
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

	inner := NewWorkflow("inner-wf").Step("sub.inner").Commit()
	outer := NewWorkflow("outer-wf").
		Step("sub.outer").
		Sub(inner).
		Commit()

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
	wf := NewWorkflow("recover-wake").Step("recover.wake").Commit()
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

	wf := NewWorkflow("int-combo").
		Step("int.setup").
		Step("int.opt", WithStepTags("optional")).
		Step("int.finish").
		Commit()

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
		Send(ctx, "status", P{"msg": "running"})
		return nil
	})

	wf := NewWorkflow("int-mw-ipc").Step("int.ipc").Commit()
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
	wf := NewWorkflow("logging-test").Step("log.task1").Commit()
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

	panicMW := func(ctx context.Context, s *step, runID RunID, next func() error) error {
		panic("test panic from middleware")
	}

	wf := NewWorkflow("panic-mw-test").Step("panic.task").Commit()
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

	wf := NewWorkflow("foreach-task-mw").
		Step("fe.mw.prepare").
		ForEach("items", Step("fe.mw.process")).
		Commit()

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

	wf1 := NewWorkflow("cache-wf-1").Step("cache.t1").Commit()
	wf2 := NewWorkflow("cache-wf-2").Step("cache.t2").Commit()
	wf3 := NewWorkflow("cache-wf-3").Step("cache.t3").Commit()

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
