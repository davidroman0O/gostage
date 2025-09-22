package runner

import (
	"context"
	"testing"

	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v2/runtime/local"
	"github.com/davidroman0O/gostage/v2/types"
	"github.com/davidroman0O/gostage/v2/types/defaults"
)

func newTestRunner() *Runner {
	return New(local.Factory{})
}

func stageStatus(result RunResult, id string) (StageStatus, bool) {
	for _, status := range result.Stages {
		if status.ID == id {
			return status, true
		}
	}
	return StageStatus{}, false
}

func actionStatus(result RunResult, stageID, name string) (ActionStatus, bool) {
	for _, status := range result.Actions {
		if status.StageID == stageID && status.Name == name {
			return status, true
		}
	}
	return ActionStatus{}, false
}

func TestRunnerExecutesActions(t *testing.T) {
	executed := make([]string, 0)

	stage := defaults.NewStage("stage-1", "Stage One", "basic stage")
	stage.AddActions(
		defaults.NewAction("a1", "", func(ctx types.Context) error {
			executed = append(executed, "a1")
			return nil
		}),
		defaults.NewAction("a2", "", func(ctx types.Context) error {
			executed = append(executed, "a2")
			return nil
		}),
	)
	_ = stage.InitialStore().Put("seed", "value")

	workflow := defaults.NewWorkflow("wf-1", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})

	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got error %v", result.Error)
	}

	expected := []string{"a1", "a2"}
	if len(executed) != len(expected) {
		t.Fatalf("unexpected execution order: %v", executed)
	}
	for i, name := range expected {
		if executed[i] != name {
			t.Fatalf("unexpected order %v", executed)
		}
	}

	status, ok := stageStatus(result, "stage-1")
	if !ok || status.Status != StatusCompleted {
		t.Fatalf("expected stage completed, got %+v", status)
	}

	if act, ok := actionStatus(result, "stage-1", "a1"); !ok || act.Status != StatusCompleted {
		t.Fatalf("expected action a1 completed, got %+v", act)
	}
	if act, ok := actionStatus(result, "stage-1", "a2"); !ok || act.Status != StatusCompleted {
		t.Fatalf("expected action a2 completed, got %+v", act)
	}

	if value, err := store.Get[string](workflow.Store(), "seed"); err != nil || value != "value" {
		t.Fatalf("expected initial store key propagated, got (%v, %v)", value, err)
	}
}

func TestRunnerDynamicActions(t *testing.T) {
	executed := make([]string, 0)

	stage := defaults.NewStage("stage-1", "Stage One", "dynamic stage")
	stage.AddActions(
		defaults.NewAction("seed", "", func(ctx types.Context) error {
			executed = append(executed, "seed")
			ctx.Actions().Add(defaults.NewAction("dynamic", "", func(ctx types.Context) error {
				executed = append(executed, "dynamic")
				return nil
			}))
			return nil
		}),
		defaults.NewAction("tail", "", func(ctx types.Context) error {
			executed = append(executed, "tail")
			return nil
		}),
	)

	workflow := defaults.NewWorkflow("wf-dyn-actions", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	expected := []string{"seed", "dynamic", "tail"}
	if len(executed) != len(expected) {
		t.Fatalf("unexpected execution count: %v", executed)
	}
	for i, name := range expected {
		if executed[i] != name {
			t.Fatalf("unexpected order %v", executed)
		}
	}

	if len(result.DynamicActions) != 1 || result.DynamicActions[0].Action.Name() != "dynamic" {
		t.Fatalf("expected dynamic action telemetry, got %+v", result.DynamicActions)
	}
}

func TestRunnerActionMiddleware(t *testing.T) {
	calls := make([]string, 0)

	stage := defaults.NewStage("stage-1", "Stage", "")
	stage.UseActionMiddleware(func(next types.ActionRunnerFunc) types.ActionRunnerFunc {
		return func(ctx types.Context, action types.Action, index int, isLast bool) error {
			calls = append(calls, "before:"+action.Name())
			err := next(ctx, action, index, isLast)
			calls = append(calls, "after:"+action.Name())
			return err
		}
	})
	stage.AddActions(defaults.NewAction("run", "", func(ctx types.Context) error {
		calls = append(calls, "execute")
		return nil
	}))

	workflow := defaults.NewWorkflow("wf-action-mw", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})

	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	expected := []string{"before:run", "execute", "after:run"}
	if len(calls) != len(expected) {
		t.Fatalf("unexpected middleware calls %v", calls)
	}
	for i, entry := range expected {
		if calls[i] != entry {
			t.Fatalf("unexpected order %v", calls)
		}
	}
}

func TestRunnerDynamicStages(t *testing.T) {
	execution := make([]string, 0)

	dynamicStage := defaults.NewStage("stage-2", "Dynamic", "")
	dynamicStage.AddActions(defaults.NewAction("dyn-action", "", func(ctx types.Context) error {
		execution = append(execution, "stage-2:dyn-action")
		return nil
	}))

	seedStage := defaults.NewStage("stage-1", "Seed", "")
	seedStage.AddActions(defaults.NewAction("spawn", "", func(ctx types.Context) error {
		execution = append(execution, "stage-1:spawn")
		ctx.Stages().Add(dynamicStage)
		return nil
	}))

	workflow := defaults.NewWorkflow("wf-dyn-stage", "Workflow", "")
	workflow.AddStage(seedStage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	expected := []string{"stage-1:spawn", "stage-2:dyn-action"}
	if len(execution) != len(expected) {
		t.Fatalf("unexpected execution %v", execution)
	}
	for i, name := range expected {
		if execution[i] != name {
			t.Fatalf("unexpected order %v", execution)
		}
	}

	if len(result.DynamicStages) != 1 || result.DynamicStages[0].Stage.ID() != "stage-2" {
		t.Fatalf("expected dynamic stage telemetry, got %+v", result.DynamicStages)
	}
}

func TestRunnerDisablesAction(t *testing.T) {
	executed := make([]string, 0)

	stage := defaults.NewStage("stage-1", "Stage", "")
	stage.AddActions(
		defaults.NewAction("first", "", func(ctx types.Context) error {
			executed = append(executed, "first")
			ctx.Actions().Disable("second")
			return nil
		}),
		defaults.NewAction("second", "", func(ctx types.Context) error {
			executed = append(executed, "second")
			return nil
		}),
	)

	workflow := defaults.NewWorkflow("wf-disable", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	if len(executed) != 1 || executed[0] != "first" {
		t.Fatalf("expected only first action to execute, got %v", executed)
	}

	if status, ok := actionStatus(result, "stage-1", "second"); !ok || status.Status != StatusSkipped {
		t.Fatalf("expected second action skipped, got %+v", status)
	}

	if !result.DisabledActions["second"] {
		t.Fatalf("expected disabled actions map to contain 'second'")
	}
}

func TestRunnerIgnoreErrors(t *testing.T) {
	stage := defaults.NewStage("stage-1", "Stage", "")
	stage.AddActions(defaults.NewAction("fail", "", func(ctx types.Context) error {
		return exampleError("boom")
	}))

	workflow := defaults.NewWorkflow("wf-ignore", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{IgnoreErrors: true})

	if result.Error != nil {
		t.Fatalf("expected error to be suppressed when IgnoreErrors is set")
	}
	if !result.Success {
		t.Fatalf("expected success flag")
	}

	status, ok := stageStatus(result, "stage-1")
	if !ok || status.Status != StatusFailed {
		t.Fatalf("expected stage marked failed, got %+v", status)
	}
}

type exampleError string

func (e exampleError) Error() string { return string(e) }

func TestRunnerWithContextOption(t *testing.T) {
	stage := defaults.NewStage("stage-1", "Stage", "")
	stage.AddActions(defaults.NewAction("noop", "", nil))

	workflow := defaults.NewWorkflow("wf-context", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	runCtx, cancel := context.WithCancel(context.Background())
	cancel()
	result := runner.Run(workflow, RunOptions{Context: runCtx})
	if !result.Success {
		t.Fatalf("expected success when context already canceled; got %v", result.Error)
	}
}
