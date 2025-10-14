package runner

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/runtime/local"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/types"
	"github.com/davidroman0O/gostage/v3/types/defaults"
)

type stubAction struct {
	name        string
	description string
}

func (s stubAction) Name() string                { return s.name }
func (s stubAction) Description() string         { return s.description }
func (s stubAction) Tags() []string              { return nil }
func (s stubAction) Execute(types.Context) error { return nil }

type captureLogger struct {
	infos  []string
	warns  []string
	errors []string
}

func (c *captureLogger) Debug(string, ...any) {}

func (c *captureLogger) Info(format string, args ...any) {
	c.infos = append(c.infos, fmt.Sprintf(format, args...))
}

func (c *captureLogger) Warn(format string, args ...any) {
	c.warns = append(c.warns, fmt.Sprintf(format, args...))
}

func (c *captureLogger) Error(format string, args ...any) {
	c.errors = append(c.errors, fmt.Sprintf(format, args...))
}

// recordingBroker captures broker interactions for assertions.
type recordingBroker struct {
	manager state.Manager

	progressCalls []struct {
		workflowID string
		stageID    string
		actionName string
		percent    int
		message    string
	}
	summaries []state.ExecutionReport
}

func newRecordingBroker(t *testing.T) *recordingBroker {
	t.Helper()
	mgr, err := state.NewStoreManager(state.NewMemoryStore())
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}
	return &recordingBroker{manager: mgr}
}

func (r *recordingBroker) WorkflowRegistered(ctx context.Context, wf state.WorkflowRecord) error {
	return r.manager.WorkflowRegistered(ctx, wf)
}

func (r *recordingBroker) WorkflowStatus(ctx context.Context, workflowID string, status state.WorkflowState) error {
	return r.manager.WorkflowStatus(ctx, workflowID, status)
}

func (r *recordingBroker) StageRegistered(ctx context.Context, workflowID string, stage state.StageRecord) error {
	return r.manager.StageRegistered(ctx, workflowID, stage)
}

func (r *recordingBroker) StageStatus(ctx context.Context, workflowID, stageID string, status state.WorkflowState) error {
	return r.manager.StageStatus(ctx, workflowID, stageID, status)
}

func (r *recordingBroker) ActionRegistered(ctx context.Context, workflowID, stageID string, action state.ActionRecord) error {
	return r.manager.ActionRegistered(ctx, workflowID, stageID, action)
}

func (r *recordingBroker) ActionStatus(ctx context.Context, workflowID, stageID, actionName string, status state.WorkflowState) error {
	return r.manager.ActionStatus(ctx, workflowID, stageID, actionName, status)
}

func (r *recordingBroker) ActionProgress(ctx context.Context, workflowID, stageID, actionName string, progress int, message string) error {
	r.progressCalls = append(r.progressCalls, struct {
		workflowID string
		stageID    string
		actionName string
		percent    int
		message    string
	}{workflowID, stageID, actionName, progress, message})
	return r.manager.ActionProgress(ctx, workflowID, stageID, actionName, progress, message)
}

func (r *recordingBroker) ActionRemoved(ctx context.Context, workflowID, stageID, actionName, createdBy string) error {
	return r.manager.ActionRemoved(ctx, workflowID, stageID, actionName, createdBy)
}

func (r *recordingBroker) StageRemoved(ctx context.Context, workflowID, stageID, createdBy string) error {
	return r.manager.StageRemoved(ctx, workflowID, stageID, createdBy)
}

func (r *recordingBroker) ExecutionSummary(ctx context.Context, workflowID string, report state.ExecutionReport) error {
	r.summaries = append(r.summaries, report)
	return r.manager.StoreExecutionSummary(ctx, workflowID, report)
}

func newTestRunner(t *testing.T) (*Runner, *recordingBroker) {
	t.Helper()
	reg := registry.NewSafeRegistry()
	registry.SetDefault(reg)
	broker := newRecordingBroker(t)
	r := New(local.Factory{}, reg, broker)
	return r, broker
}

func mustRegisterAction(name string, run func(types.Context) error) {
	defaults.MustRegisterAction(name, "", run)
}

func registerWorkflowForTest(id string, acts ...string) types.Workflow {
	stage := defaults.NewStage("stage-1", "Stage One", "test stage")
	for _, act := range acts {
		mustRegisterAction(act, func(types.Context) error { return nil })
		stage.AddActions(defaults.NewAction(act))
	}
	workflow := defaults.NewWorkflow(id, "Workflow", "")
	workflow.AddStage(stage)
	return workflow
}

func TestRunnerExecutesActions(t *testing.T) {
	executed := make([]string, 0)
	reg := registry.NewSafeRegistry()
	registry.SetDefault(reg)

	stage := defaults.NewStage("stage-1", "Stage", "")
	mustRegisterAction("a1", func(ctx types.Context) error {
		executed = append(executed, "a1")
		return nil
	})
	mustRegisterAction("a2", func(ctx types.Context) error {
		executed = append(executed, "a2")
		return nil
	})
	stage.AddActions(defaults.NewAction("a1"), defaults.NewAction("a2"))
	_ = stage.InitialStore().Put("seed", "value")

	wf := defaults.NewWorkflow("wf-1", "Workflow", "")
	wf.AddStage(stage)

	broker := newRecordingBroker(t)
	r := New(local.Factory{}, reg, broker)
	result := r.Run(wf, RunOptions{})

	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}
	expected := []string{"a1", "a2"}
	if strings.Join(executed, ",") != strings.Join(expected, ",") {
		t.Fatalf("unexpected execution order: %v", executed)
	}
	if value, err := store.Get[string](wf.Store(), "seed"); err != nil || value != "value" {
		t.Fatalf("initial store missing: %v %v", value, err)
	}
}

func TestRunnerFailsOnUnregisteredAction(t *testing.T) {
	reg := registry.NewSafeRegistry()
	registry.SetDefault(reg)
	stage := defaults.NewStage("stage-1", "Stage", "")
	stage.AddActions(stubAction{name: "missing"})

	wf := defaults.NewWorkflow("wf-unregistered", "Workflow", "")
	wf.AddStage(stage)

	broker := newRecordingBroker(t)
	runner := New(local.Factory{}, reg, broker)
	result := runner.Run(wf, RunOptions{})

	if result.Success || result.Error == nil {
		t.Fatalf("expected failure, got success=%t error=%v", result.Success, result.Error)
	}
	if !strings.Contains(result.Error.Error(), "missing") {
		t.Fatalf("unexpected error: %v", result.Error)
	}
}

func TestRunnerDynamicActions(t *testing.T) {
	reg := registry.NewSafeRegistry()
	registry.SetDefault(reg)

	mustRegisterAction("seed", func(ctx types.Context) error {
		registerNoop("dyn")
		dyn := defaults.NewAction("dyn")
		ctx.Actions().Add(dyn)
		return nil
	})
	mustRegisterAction("dyn", func(types.Context) error { return nil })

	stage := defaults.NewStage("stage-1", "Stage", "")
	stage.AddActions(defaults.NewAction("seed"))

	wf := defaults.NewWorkflow("wf-dynamic", "Workflow", "")
	wf.AddStage(stage)

	broker := newRecordingBroker(t)
	runner := New(local.Factory{}, reg, broker)
	result := runner.Run(wf, RunOptions{})

	if !result.Success {
		t.Fatalf("expected success, got error %v", result.Error)
	}
	if len(result.DynamicActions) == 0 {
		t.Fatalf("expected dynamic actions recorded")
	}
}

func TestRunnerActionMiddleware(t *testing.T) {
	reg := registry.NewSafeRegistry()
	registry.SetDefault(reg)
	called := 0

	mustRegisterAction("base", func(ctx types.Context) error { return nil })

	stage := defaults.NewStage("stage-1", "Stage", "")
	action := defaults.NewAction("base")
	stage.WithActionMiddleware(func(next types.ActionRunnerFunc) types.ActionRunnerFunc {
		return func(ctx types.Context, act types.Action, index int, isLast bool) error {
			called++
			return next(ctx, act, index, isLast)
		}
	})
	stage.AddActions(action)

	wf := defaults.NewWorkflow("wf-mw", "Workflow", "")
	wf.AddStage(stage)

	broker := newRecordingBroker(t)
	runner := New(local.Factory{}, reg, broker)
	result := runner.Run(wf, RunOptions{})

	if !result.Success {
		t.Fatalf("expected success, got %v", result.Error)
	}
	if called != 1 {
		t.Fatalf("expected middleware to run once, got %d", called)
	}
}

func registerNoop(name string) {
	defaults.MustRegisterAction(name, "", func(types.Context) error { return nil })
}
