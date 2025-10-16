package runner

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/runtime/local"
	"github.com/davidroman0O/gostage/v3/state"
	storepkg "github.com/davidroman0O/gostage/v3/store"
	"github.com/davidroman0O/gostage/v3/workflow"
)

type stubAction struct {
	name        string
	description string
}

func (s stubAction) Name() string             { return s.name }
func (s stubAction) Description() string      { return s.description }
func (s stubAction) Tags() []string           { return nil }
func (s stubAction) Execute(rt.Context) error { return nil }

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
	events []struct {
		workflowID string
		stageID    string
		actionName string
		kind       string
		message    string
		metadata   map[string]any
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

func (r *recordingBroker) ActionEvent(ctx context.Context, workflowID, stageID, actionName, kind, message string, metadata map[string]any) error {
	var metaCopy map[string]any
	if len(metadata) > 0 {
		metaCopy = make(map[string]any, len(metadata))
		for k, v := range metadata {
			metaCopy[k] = v
		}
	}
	r.events = append(r.events, struct {
		workflowID string
		stageID    string
		actionName string
		kind       string
		message    string
		metadata   map[string]any
	}{workflowID, stageID, actionName, kind, message, metaCopy})
	return r.manager.ActionEvent(ctx, workflowID, stageID, actionName, kind, message, metadata)
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

func mustRegisterAction(name string, run func(rt.Context) error) {
	if err := registry.Default().RegisterAction(name, run, registry.ActionMetadata{}); err != nil {
		panic(err)
	}
}

func registerWorkflowForTest(id string, acts ...string) rt.Workflow {
	stageDef := workflow.Stage{
		Name:    "Stage One",
		Actions: make([]workflow.Action, 0, len(acts)),
	}
	for _, act := range acts {
		mustRegisterAction(act, func(rt.Context) error { return nil })
		stageDef.Actions = append(stageDef.Actions, workflow.Action{Ref: act})
	}
	def := workflow.Definition{
		ID:     id,
		Name:   "Workflow",
		Stages: []workflow.Stage{stageDef},
	}
	if normalized, _, err := workflow.EnsureIDs(def); err != nil {
		panic(err)
	} else {
		def = normalized
	}
	wf, err := workflow.Materialize(def, registry.Default())
	if err != nil {
		panic(err)
	}
	return wf
}

func TestRunnerExecutesActions(t *testing.T) {
	executed := make([]string, 0)
	reg := registry.NewSafeRegistry()
	registry.SetDefault(reg)

	stage := workflow.NewRuntimeStage("stage-1", "Stage", "")
	mustRegisterAction("a1", func(ctx rt.Context) error {
		executed = append(executed, "a1")
		return nil
	})
	mustRegisterAction("a2", func(ctx rt.Context) error {
		executed = append(executed, "a2")
		return nil
	})
	stage.AddActions(workflow.MustRuntimeAction("a1"), workflow.MustRuntimeAction("a2"))
	_ = storepkg.Put(stage.InitialStore(), "seed", "value")

	wf := workflow.NewRuntimeWorkflow("wf-1", "Workflow", "")
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
	if value, err := storepkg.Get[string](wf.Store(), "seed"); err != nil || value != "value" {
		t.Fatalf("initial store missing: %v %v", value, err)
	}
}

func TestRunnerFailsOnUnregisteredAction(t *testing.T) {
	reg := registry.NewSafeRegistry()
	registry.SetDefault(reg)
	stage := workflow.NewRuntimeStage("stage-1", "Stage", "")
	stage.AddActions(stubAction{name: "missing"})

	wf := workflow.NewRuntimeWorkflow("wf-unregistered", "Workflow", "")
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

	var addedID string
	mustRegisterAction("seed", func(ctx rt.Context) error {
		registerNoop("dyn")
		dyn := workflow.MustRuntimeAction("dyn")
		addedID = ctx.Actions().Add(dyn)
		return nil
	})
	mustRegisterAction("dyn", func(rt.Context) error { return nil })

	stage := workflow.NewRuntimeStage("stage-1", "Stage", "")
	stage.AddActions(workflow.MustRuntimeAction("seed"))

	wf := workflow.NewRuntimeWorkflow("wf-dynamic", "Workflow", "")
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
	if addedID == "" {
		t.Fatalf("dynamic action id should not be empty")
	}
}

func TestRunnerHonorsInitialDisabledActions(t *testing.T) {
	executed := 0
	reg := registry.NewSafeRegistry()
	registry.SetDefault(reg)
	mustRegisterAction("test.disabled", func(rt.Context) error {
		executed++
		return nil
	})

	stage := workflow.NewRuntimeStage("stage-1", "Stage", "")
	stage.AddActions(workflow.MustRuntimeAction("test.disabled"))
	stage.Actions().Disable("test.disabled")

	wf := workflow.NewRuntimeWorkflow("wf-disabled", "Workflow", "")
	wf.AddStage(stage)

	broker := newRecordingBroker(t)
	r := New(local.Factory{}, reg, broker)
	result := r.Run(wf, RunOptions{})

	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got error %v", result.Error)
	}
	if executed != 0 {
		t.Fatalf("expected disabled action not to execute, ran %d times", executed)
	}
	if result.DisabledActions == nil || !result.DisabledActions["test.disabled"] {
		t.Fatalf("expected disabled actions map to include test.disabled")
	}
	if len(result.Actions) == 0 {
		t.Fatalf("expected action statuses recorded")
	}
	if result.Actions[0].Status != StatusSkipped {
		t.Fatalf("expected action status skipped, got %s", result.Actions[0].Status)
	}
}

func TestRunnerActionMiddleware(t *testing.T) {
	reg := registry.NewSafeRegistry()
	registry.SetDefault(reg)
	called := 0

	mustRegisterAction("base", func(ctx rt.Context) error { return nil })

	stage := workflow.NewRuntimeStage("stage-1", "Stage", "")
	action := workflow.MustRuntimeAction("base")
	stage.WithActionMiddleware(func(next rt.ActionRunnerFunc) rt.ActionRunnerFunc {
		return func(ctx rt.Context, act rt.Action, index int, isLast bool) error {
			called++
			return next(ctx, act, index, isLast)
		}
	})
	stage.AddActions(action)

	wf := workflow.NewRuntimeWorkflow("wf-mw", "Workflow", "")
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

func TestRunnerEmitsCustomActionEvent(t *testing.T) {
	reg := registry.NewSafeRegistry()
	registry.SetDefault(reg)
	mustRegisterAction("custom.event", func(ctx rt.Context) error {
		details := map[string]any{"foo": "bar"}
		if err := ctx.Broker().Event("action.custom", "done", details); err != nil {
			return err
		}
		details["foo"] = "mutated"
		return nil
	})

	stage := workflow.NewRuntimeStage("stage-1", "Stage", "")
	stage.AddActions(workflow.MustRuntimeAction("custom.event"))

	wf := workflow.NewRuntimeWorkflow("wf-custom-event", "Workflow", "")
	wf.AddStage(stage)

	broker := newRecordingBroker(t)
	runner := New(local.Factory{}, reg, broker)
	result := runner.Run(wf, RunOptions{})

	if !result.Success {
		t.Fatalf("expected success, got %v", result.Error)
	}
	if len(broker.events) != 1 {
		t.Fatalf("expected 1 custom event, got %d", len(broker.events))
	}
	evt := broker.events[0]
	if evt.kind != "action.custom" || evt.message != "done" {
		t.Fatalf("unexpected event metadata %+v", evt)
	}
	if evt.metadata == nil || evt.metadata["foo"] != "bar" {
		t.Fatalf("expected metadata copy preserved, got %+v", evt.metadata)
	}
	if evt.workflowID == "" || evt.stageID == "" || evt.actionName == "" {
		t.Fatalf("expected workflow/stage/action ids recorded, got %+v", evt)
	}
}

func TestRunnerActionDefinitionMiddleware(t *testing.T) {
	reg := registry.NewSafeRegistry()
	registry.SetDefault(reg)

	if err := reg.RegisterAction("base", func(rt.Context) error { return nil }, registry.ActionMetadata{}); err != nil {
		t.Fatalf("register action: %v", err)
	}

	called := 0
	workflow.MustRegisterActionMiddleware("count", func(next rt.ActionRunnerFunc) rt.ActionRunnerFunc {
		return func(ctx rt.Context, act rt.Action, index int, isLast bool) error {
			called++
			return next(ctx, act, index, isLast)
		}
	})

	def := workflow.Definition{
		ID: "wf-action-mw",
		Stages: []workflow.Stage{
			{
				Actions: []workflow.Action{{Ref: "base", Middleware: []string{"count"}}},
			},
		},
	}
	if normalized, _, err := workflow.EnsureIDs(def); err != nil {
		t.Fatalf("ensure ids: %v", err)
	} else {
		def = normalized
	}

	wf, err := workflow.Materialize(def, reg)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}

	broker := newRecordingBroker(t)
	runner := New(local.Factory{}, reg, broker)
	result := runner.Run(wf, RunOptions{})

	if !result.Success {
		t.Fatalf("expected success, got %v", result.Error)
	}
	if called != 1 {
		t.Fatalf("expected action middleware to run once, got %d", called)
	}
}

func registerNoop(name string) {
	if err := registry.Default().RegisterAction(name, func(rt.Context) error { return nil }, registry.ActionMetadata{}); err != nil {
		panic(err)
	}
}
