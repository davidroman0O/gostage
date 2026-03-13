package gostage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

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
