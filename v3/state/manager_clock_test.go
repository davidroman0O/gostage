package state

import (
	"context"
	"testing"
	"time"
)

func TestStoreManagerUsesClockForStageAndActionTimestamps(t *testing.T) {
	store := NewMemoryStore()
	base := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC)
	var calls []time.Time
	clock := func() time.Time {
		ts := base.Add(time.Duration(len(calls)) * time.Second)
		calls = append(calls, ts)
		return ts
	}

	manager, err := NewStoreManager(store, WithManagerClock(clock))
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}

	ctx := context.Background()
	wf := WorkflowRecord{ID: WorkflowID("wf-clock")}
	if err := manager.WorkflowRegistered(ctx, wf); err != nil {
		t.Fatalf("workflow registered: %v", err)
	}

	stage := StageRecord{ID: "stage-1"}
	if err := manager.StageRegistered(ctx, string(wf.ID), stage); err != nil {
		t.Fatalf("stage registered: %v", err)
	}

	action := ActionRecord{Name: "action-1"}
	if err := manager.ActionRegistered(ctx, string(wf.ID), stage.ID, action); err != nil {
		t.Fatalf("action registered: %v", err)
	}

	if err := manager.StageStatus(ctx, string(wf.ID), stage.ID, WorkflowRunning); err != nil {
		t.Fatalf("stage running: %v", err)
	}
	if len(calls) < 2 {
		t.Fatalf("expected at least two clock calls, got %d", len(calls))
	}
	stageAfterRun := store.stages[wf.ID][stage.ID]
	expectedStageStart := calls[1]
	if stageAfterRun == nil || stageAfterRun.StartedAt == nil || !stageAfterRun.StartedAt.Equal(expectedStageStart) {
		t.Fatalf("expected stage started %v after running, got %#v", expectedStageStart, stageAfterRun)
	}

	if err := manager.StageStatus(ctx, string(wf.ID), stage.ID, WorkflowCompleted); err != nil {
		t.Fatalf("stage completed: %v", err)
	}
	if len(calls) < 3 {
		t.Fatalf("expected at least three clock calls, got %d", len(calls))
	}

	if err := manager.ActionStatus(ctx, string(wf.ID), stage.ID, action.Name, WorkflowRunning); err != nil {
		t.Fatalf("action running: %v", err)
	}
	if len(calls) < 4 {
		t.Fatalf("expected at least four clock calls, got %d", len(calls))
	}
	actionAfterRun := store.actions[wf.ID][stage.ID][action.Name]
	expectedActionStart := calls[3]
	if actionAfterRun == nil || actionAfterRun.StartedAt == nil || !actionAfterRun.StartedAt.Equal(expectedActionStart) {
		t.Fatalf("expected action started %v after running, got %#v", expectedActionStart, actionAfterRun)
	}

	if err := manager.ActionStatus(ctx, string(wf.ID), stage.ID, action.Name, WorkflowCompleted); err != nil {
		t.Fatalf("action completed: %v", err)
	}
	if len(calls) < 5 {
		t.Fatalf("expected at least five clock calls, got %d", len(calls))
	}

	gotStage := store.stages[wf.ID][stage.ID]
	if gotStage.StartedAt == nil || !gotStage.StartedAt.Equal(expectedStageStart) {
		t.Fatalf("expected stage started %v, got %#v", expectedStageStart, gotStage.StartedAt)
	}
	expectedStageCompleted := calls[2]
	if gotStage.CompletedAt == nil || !gotStage.CompletedAt.Equal(expectedStageCompleted) {
		t.Fatalf("expected stage completed %v, got %#v", expectedStageCompleted, gotStage.CompletedAt)
	}

	gotAction := store.actions[wf.ID][stage.ID][action.Name]
	if gotAction.StartedAt == nil || !gotAction.StartedAt.Equal(expectedActionStart) {
		t.Fatalf("expected action started %v, got %#v", expectedActionStart, gotAction.StartedAt)
	}
	expectedActionCompleted := calls[4]
	if gotAction.CompletedAt == nil || !gotAction.CompletedAt.Equal(expectedActionCompleted) {
		t.Fatalf("expected action completed %v, got %#v", expectedActionCompleted, gotAction.CompletedAt)
	}
}
