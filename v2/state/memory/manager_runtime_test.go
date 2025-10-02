package memory

import (
	"context"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v2/state"
)

func TestManagerTracksSkippedStageAndAction(t *testing.T) {
	mgr := New()
	defer mgr.Close()

	ctx := context.Background()
	wf := state.WorkflowRecord{
		ID:    "wf-1",
		Name:  "wf",
		State: state.WorkflowPending,
		Definition: state.SubWorkflowDef{
			ID:   "wf-1",
			Name: "wf",
		},
		Stages: map[string]*state.StageRecord{
			"stage-1": {
				ID:      "stage-1",
				Name:    "Stage",
				Actions: make(map[string]*state.ActionRecord),
			},
		},
	}

	if err := mgr.WorkflowRegistered(ctx, wf); err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	stageRec := state.StageRecord{ID: "stage-1", Name: "Stage"}
	if err := mgr.StageRegistered(ctx, "wf-1", stageRec); err != nil {
		t.Fatalf("register stage: %v", err)
	}

	actionRec := state.ActionRecord{Name: "action-1", Description: "", Tags: nil}
	if err := mgr.ActionRegistered(ctx, "wf-1", "stage-1", actionRec); err != nil {
		t.Fatalf("register action: %v", err)
	}

	if err := mgr.StageStatus(ctx, "wf-1", "stage-1", state.WorkflowSkipped); err != nil {
		t.Fatalf("set stage status: %v", err)
	}
	if err := mgr.ActionStatus(ctx, "wf-1", "stage-1", "action-1", state.WorkflowSkipped); err != nil {
		t.Fatalf("set action status: %v", err)
	}

	status, err := mgr.GetStatus("wf-1")
	if err != nil {
		t.Fatalf("get status: %v", err)
	}

	stage := status.Stages["stage-1"]
	if stage == nil {
		t.Fatalf("expected stage record present")
	}
	if stage.Status != state.WorkflowSkipped {
		t.Fatalf("expected stage status skipped, got %s", stage.Status)
	}
	if stage.EndedAt.IsZero() {
		t.Fatalf("expected stage EndedAt set")
	}

	action := stage.Actions["action-1"]
	if action == nil {
		t.Fatalf("expected action record present")
	}
	if action.Status != state.WorkflowSkipped {
		t.Fatalf("expected action status skipped, got %s", action.Status)
	}
	if action.EndedAt.IsZero() {
		t.Fatalf("expected action EndedAt set")
	}

	if action.EndedAt.Before(stage.EndedAt.Add(-time.Second)) {
		t.Fatalf("expected action end timestamp not far before stage end")
	}
}
