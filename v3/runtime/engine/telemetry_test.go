package engine

import (
	"testing"

	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/runtime/local"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestTelemetrySeedsWorkflowAndActions(t *testing.T) {
	wf := workflow.NewRuntimeWorkflow("wf", "Workflow", "")
	stage := workflow.NewRuntimeStage("stage-1", "Stage 1", "desc")
	stage.AddTags("tag")
	action := testAction{name: "action-1", description: "act", tags: []string{"a"}}
	stage.AddActions(action)
	wf.AddStage(stage)

	tele := NewTelemetry(wf, StatusPending)

	stages := tele.StageStatuses()
	if len(stages) != 1 {
		t.Fatalf("expected 1 stage status, got %d", len(stages))
	}
	if stages[0].ID != stage.ID() || stages[0].Status != StatusPending {
		t.Fatalf("unexpected stage status: %+v", stages[0])
	}

	tele.Action(stage.ID(), action.Name(), action.Description(), action.Tags(), false, "")
	actions := tele.ActionStatuses()
	if len(actions) != 1 {
		t.Fatalf("expected 1 action status, got %d", len(actions))
	}
	if actions[0].StageID != stage.ID() || actions[0].Name != action.Name() {
		t.Fatalf("unexpected action status: %+v", actions[0])
	}

	execCtx := local.Factory{}.New(wf, nil)
	if disabled := tele.DisabledStages(execCtx); len(disabled) != 0 {
		t.Fatalf("expected no disabled stages, got %+v", disabled)
	}

	execCtx.SetDisabledMaps(nil, map[string]bool{stage.ID(): true})
	disabled := tele.DisabledStages(execCtx)
	if len(disabled) != 1 || !disabled[stage.ID()] {
		t.Fatalf("expected disabled stage entry, got %+v", disabled)
	}
}

func TestTelemetryDynamicInsertionAndRemoval(t *testing.T) {
	wf := workflow.NewRuntimeWorkflow("wf", "Workflow", "")
	stage := workflow.NewRuntimeStage("stage-1", "Stage 1", "")
	stage.AddActions(testAction{name: "a1"})
	wf.AddStage(stage)

	tele := NewTelemetry(wf, StatusPending)

	tele.AddDynamicAction(stage.ID(), testAction{name: "dyn"}, "stage-1::a1")
	dynActions := tele.DynamicActions()
	if len(dynActions) != 1 || dynActions[0].CreatedBy != "stage-1::a1" {
		t.Fatalf("unexpected dynamic actions: %+v", dynActions)
	}

	dynStage := workflow.NewRuntimeStage("dyn-stage", "Dynamic", "")
	tele.AddDynamicStage(dynStage, "stage-1::a1")
	if !tele.HasPendingStages() {
		t.Fatalf("expected pending dynamic stages")
	}
	updated := tele.InsertPendingStages([]rt.Stage{stage}, 1)
	if len(updated) != 2 || updated[1].ID() != dynStage.ID() {
		t.Fatalf("unexpected inserted stages: %+v", updated)
	}
	if tele.HasPendingStages() {
		t.Fatalf("pending stages should be cleared after insertion")
	}

	tele.MarkActionRemoved(stage.ID(), "a1", "stage-1::a1")
	removedActions := tele.RemovedActions()
	if len(removedActions) != 1 || removedActions[ActionKey(stage.ID(), "a1")] != "stage-1::a1" {
		t.Fatalf("unexpected removed actions: %+v", removedActions)
	}

	tele.MarkStageRemoved(stage.ID(), "stage-1::a1")
	removedStages := tele.RemovedStages()
	if len(removedStages) != 1 || removedStages[stage.ID()] != "stage-1::a1" {
		t.Fatalf("unexpected removed stages: %+v", removedStages)
	}
}

type testAction struct {
	name        string
	description string
	tags        []string
}

func (a testAction) Name() string           { return a.name }
func (a testAction) Description() string    { return a.description }
func (a testAction) Tags() []string         { return append([]string(nil), a.tags...) }
func (testAction) Execute(rt.Context) error { return nil }
