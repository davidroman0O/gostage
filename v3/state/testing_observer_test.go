package state_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	state "github.com/davidroman0O/gostage/v3/state"
	statetest "github.com/davidroman0O/gostage/v3/state/testkit"
)

func TestCaptureObserverRecordsLifecycle(t *testing.T) {
	store := state.NewMemoryStore()
	observer := statetest.NewCaptureObserver()
	manager, err := state.NewStoreManager(store, state.WithManagerObservers(observer))
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}

	ctx := context.Background()
	wf := state.WorkflowRecord{ID: state.WorkflowID("wf-1"), Name: "test"}
	if err := manager.WorkflowRegistered(ctx, wf); err != nil {
		t.Fatalf("workflow registered: %v", err)
	}
	if err := manager.WorkflowStatus(ctx, string(wf.ID), state.WorkflowRunning); err != nil {
		t.Fatalf("workflow status: %v", err)
	}

	stage := state.StageRecord{ID: "stage-1", Name: "build", Tags: []string{"primary"}}
	if err := manager.StageRegistered(ctx, string(wf.ID), stage); err != nil {
		t.Fatalf("stage registered: %v", err)
	}
	if err := manager.StageStatus(ctx, string(wf.ID), stage.ID, state.WorkflowCompleted); err != nil {
		t.Fatalf("stage status: %v", err)
	}

	action := state.ActionRecord{Name: "action-1", Description: "do", Tags: []string{"a"}}
	if err := manager.ActionRegistered(ctx, string(wf.ID), stage.ID, action); err != nil {
		t.Fatalf("action registered: %v", err)
	}
	if err := manager.ActionStatus(ctx, string(wf.ID), stage.ID, action.Name, state.WorkflowCompleted); err != nil {
		t.Fatalf("action status: %v", err)
	}
	if err := manager.ActionProgress(ctx, string(wf.ID), stage.ID, action.Name, 75, "almost"); err != nil {
		t.Fatalf("action progress: %v", err)
	}
	if err := manager.ActionEvent(ctx, string(wf.ID), stage.ID, action.Name, "action.custom", "custom", map[string]any{"foo": "bar"}); err != nil {
		t.Fatalf("action event: %v", err)
	}
	if err := manager.ActionRemoved(ctx, string(wf.ID), stage.ID, action.Name, "tester"); err != nil {
		t.Fatalf("action removed: %v", err)
	}
	if err := manager.StageRemoved(ctx, string(wf.ID), stage.ID, "tester"); err != nil {
		t.Fatalf("stage removed: %v", err)
	}

	report := state.ExecutionReport{
		WorkflowID:   string(wf.ID),
		WorkflowName: "test",
		Status:       state.WorkflowCompleted,
		Success:      true,
		CompletedAt:  time.Now(),
		Duration:     time.Second,
		FinalStore:   map[string]any{"result": true},
		DisabledStages: map[string]bool{
			stage.ID: true,
		},
		DisabledActions: map[string]bool{
			action.Name: true,
		},
		RemovedStages: map[string]string{
			stage.ID: "tester",
		},
		RemovedActions: map[string]string{
			action.Name: "tester",
		},
	}
	if err := manager.StoreExecutionSummary(ctx, string(wf.ID), report); err != nil {
		t.Fatalf("store summary: %v", err)
	}

	snapshot := observer.Snapshot()

	if _, ok := snapshot.Workflows[wf.ID]; !ok {
		t.Fatalf("expected workflow snapshot")
	}
	if len(snapshot.WorkflowStatuses) == 0 {
		t.Fatalf("expected workflow status events")
	}

	stages := snapshot.Stages[wf.ID]
	if stages == nil {
		t.Fatalf("expected stage map")
	}
	if stages[stage.ID].Status != state.WorkflowRemoved {
		t.Fatalf("expected stage removed status, got %v", stages[stage.ID].Status)
	}

	actions := snapshot.Actions[wf.ID]
	key := fmt.Sprintf("%s::%s", stage.ID, action.Name)
	if actions[key].State != state.WorkflowRemoved {
		t.Fatalf("expected action removed state, got %v", actions[key].State)
	}

	progress := snapshot.Progress[wf.ID][key]
	if progress.Progress != 75 || progress.Message != "almost" {
		t.Fatalf("unexpected progress %+v", progress)
	}
	if len(snapshot.ActionEvents) == 0 {
		t.Fatalf("expected custom action events, got none")
	}
	evt := snapshot.ActionEvents[0]
	if evt.Kind != "action.custom" || evt.Message != "custom" || evt.Metadata["foo"] != "bar" {
		t.Fatalf("unexpected action event %+v", evt)
	}
	if evt.StageID != stage.ID || evt.ActionID != action.Name {
		t.Fatalf("unexpected event stage/action %+v", evt)
	}

	summary, ok := snapshot.Summaries[wf.ID]
	if !ok || !summary.Success || summary.Output["result"] != true {
		t.Fatalf("expected summary with result, got %+v", summary)
	}

	if len(snapshot.ActionRemovals) == 0 || snapshot.ActionRemovals[0].ActionID != action.Name {
		t.Fatalf("expected action removal event, got %+v", snapshot.ActionRemovals)
	}
	if len(snapshot.StageRemovals) == 0 || snapshot.StageRemovals[0].StageID != stage.ID {
		t.Fatalf("expected stage removal event, got %+v", snapshot.StageRemovals)
	}
}
