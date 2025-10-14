package local

import (
	"testing"

	"github.com/davidroman0O/gostage/v3/types"
	"github.com/davidroman0O/gostage/v3/types/defaults"
)

func registerNoop(name string) {
	defaults.MustRegisterAction(name, "", func(types.Context) error { return nil })
}

func TestActionMutationRuntimeRecording(t *testing.T) {
	wf := defaults.NewWorkflow("wf", "Workflow", "")
	stage := defaults.NewStage("stage-1", "Stage", "")
	registerNoop("seed")
	seed := defaults.NewAction("seed")
	stage.AddActions(seed)
	wf.AddStage(stage)

	ctx := newActionContext(wf)
	ctx.setStage(stage)
	ctx.setAction(seed, 0, true)

	mutation := newActionMutation(ctx)
	registerNoop("dyn")
	dyn := defaults.NewAction("dyn")
	mutation.Add(dyn)
	mutation.Disable("seed")
	mutation.Enable("seed")
	mutation.Remove("dyn")

	runtime := stage.RuntimeState()
	if len(runtime.DynamicActions) != 1 {
		t.Fatalf("expected 1 dynamic action, got %d", len(runtime.DynamicActions))
	}
	if runtime.DynamicActions[0].Action.Name() != "dyn" {
		t.Fatalf("expected dynamic action 'dyn', got %s", runtime.DynamicActions[0].Action.Name())
	}
	if runtime.DynamicActions[0].CreatedBy != "stage-1::seed" {
		t.Fatalf("unexpected createdBy: %s", runtime.DynamicActions[0].CreatedBy)
	}

	if _, ok := runtime.Disabled["seed"]; !ok {
		t.Fatalf("expected disabled entry for seed")
	}
	if _, ok := runtime.Enabled["seed"]; !ok {
		t.Fatalf("expected enabled entry for seed")
	}
	if _, ok := runtime.Removed["dyn"]; !ok {
		t.Fatalf("expected removed entry for dyn")
	}
}

func TestStageMutationRuntimeRecording(t *testing.T) {
	wf := defaults.NewWorkflow("wf", "Workflow", "")
	stage := defaults.NewStage("stage-1", "Stage", "")
	registerNoop("seed")
	seed := defaults.NewAction("seed")
	stage.AddActions(seed)
	wf.AddStage(stage)

	ctx := newActionContext(wf)
	ctx.setStage(stage)
	ctx.setAction(seed, 0, true)

	stageMutation := newStageMutation(ctx)
	dynStage := defaults.NewStage("stage-2", "Dynamic", "")
	stageMutation.Add(dynStage)
	stageMutation.Disable("stage-1")
	stageMutation.Disable("stage-2")
	stageMutation.Enable("stage-2")
	stageMutation.Remove("stage-2")

	wfRuntime := wf.RuntimeState()
	if len(wfRuntime.DynamicStages) != 1 {
		t.Fatalf("expected 1 dynamic stage, got %d", len(wfRuntime.DynamicStages))
	}
	if wfRuntime.DynamicStages[0].Stage != dynStage {
		t.Fatalf("unexpected dynamic stage reference")
	}
	if wfRuntime.DynamicStages[0].CreatedBy != "stage-1::seed" {
		t.Fatalf("unexpected createdBy: %s", wfRuntime.DynamicStages[0].CreatedBy)
	}

	if _, ok := wfRuntime.Disabled["stage-1"]; !ok {
		t.Fatalf("expected disabled entry for stage-1")
	}
	if _, ok := wfRuntime.Disabled["stage-2"]; !ok {
		t.Fatalf("expected disabled entry for stage-2")
	}
	if _, ok := wfRuntime.Enabled["stage-2"]; !ok {
		t.Fatalf("expected enabled entry for stage-2")
	}
	if _, ok := wfRuntime.Removed["stage-2"]; !ok {
		t.Fatalf("expected removed entry for stage-2")
	}
}
