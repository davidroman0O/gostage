package local

import (
	"testing"

	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func registerNoop(name string) {
	if err := registry.Default().RegisterAction(name, func(rt.Context) error { return nil }, registry.ActionMetadata{}); err != nil {
		panic(err)
	}
}

func TestActionMutationRuntimeRecording(t *testing.T) {
	registry.SetDefault(registry.NewSafeRegistry())
	wf := workflow.NewRuntimeWorkflow("wf", "Workflow", "")
	stage := workflow.NewRuntimeStage("stage-1", "Stage", "")
	registerNoop("seed")
	seed := workflow.MustRuntimeAction("seed")
	stage.AddActions(seed)
	wf.AddStage(stage)

	ctx := newActionContext(wf)
	ctx.setStage(stage)
	ctx.setAction(seed, 0, true)

	mutation := newActionMutation(ctx)
	registerNoop("dyn")
	dyn := workflow.MustRuntimeAction("dyn")
	addedID := mutation.Add(dyn)
	mutation.Disable("seed")
	mutation.Enable("seed")
	mutation.Remove(addedID)

	runtime := stage.RuntimeState()
	if len(runtime.DynamicActions) != 1 {
		t.Fatalf("expected 1 dynamic action, got %d", len(runtime.DynamicActions))
	}
	if runtime.DynamicActions[0].Action.Name() != addedID {
		t.Fatalf("unexpected dynamic action id %s", runtime.DynamicActions[0].Action.Name())
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
	if _, ok := runtime.Removed[addedID]; !ok {
		t.Fatalf("expected removed entry for generated action id")
	}
}

func TestStageMutationRuntimeRecording(t *testing.T) {
	registry.SetDefault(registry.NewSafeRegistry())
	wf := workflow.NewRuntimeWorkflow("wf", "Workflow", "")
	stage := workflow.NewRuntimeStage("stage-1", "Stage", "")
	registerNoop("seed")
	seed := workflow.MustRuntimeAction("seed")
	stage.AddActions(seed)
	wf.AddStage(stage)

	ctx := newActionContext(wf)
	ctx.setStage(stage)
	ctx.setAction(seed, 0, true)

	stageMutation := newStageMutation(ctx)
	dynStage := workflow.NewRuntimeStage("stage-2", "Dynamic", "")
	addedStageID := stageMutation.Add(dynStage)
	stageMutation.Disable("stage-1")
	stageMutation.Disable(addedStageID)
	stageMutation.Enable(addedStageID)
	stageMutation.Remove(addedStageID)

	wfRuntime := wf.RuntimeState()
	if len(wfRuntime.DynamicStages) != 1 {
		t.Fatalf("expected 1 dynamic stage, got %d", len(wfRuntime.DynamicStages))
	}
	if wfRuntime.DynamicStages[0].Stage.ID() != addedStageID {
		t.Fatalf("unexpected dynamic stage id %s", wfRuntime.DynamicStages[0].Stage.ID())
	}
	if wfRuntime.DynamicStages[0].CreatedBy != "stage-1::seed" {
		t.Fatalf("unexpected createdBy: %s", wfRuntime.DynamicStages[0].CreatedBy)
	}

	if _, ok := wfRuntime.Disabled["stage-1"]; !ok {
		t.Fatalf("expected disabled entry for stage-1")
	}
	if _, ok := wfRuntime.Disabled[addedStageID]; !ok {
		t.Fatalf("expected disabled entry for generated stage id")
	}
	if _, ok := wfRuntime.Enabled[addedStageID]; !ok {
		t.Fatalf("expected enabled entry for generated stage id")
	}
	if _, ok := wfRuntime.Removed[addedStageID]; !ok {
		t.Fatalf("expected removed entry for generated stage id")
	}
}
