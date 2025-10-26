package state

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestSQLiteStoreWaitResult(t *testing.T) {
	db := openTestDB(t)
	store, err := NewSQLiteStore(db)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	reader, err := NewSQLiteStateReader(db)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}

	ctx := context.Background()
	wfID := WorkflowID("wf-1")
	rec := WorkflowRecord{
		ID:        wfID,
		Name:      "Checkout",
		Tags:      []string{"payments"},
		Metadata:  map[string]any{"env": "test"},
		CreatedAt: time.Now(),
		State:     WorkflowRunning,
	}
	if err := store.RecordWorkflow(ctx, rec); err != nil {
		t.Fatalf("record workflow: %v", err)
	}

	stageDef := workflow.Stage{
		Name:        "Prepare",
		Description: "Prepare description",
		Actions: []workflow.Action{{
			Ref:         "prepare.ref",
			Description: "Prepare action description",
		}},
	}
	stageNormalized, _, err := workflow.EnsureIDs(workflow.Definition{Stages: []workflow.Stage{stageDef}})
	if err != nil {
		t.Fatalf("ensure ids: %v", err)
	}
	stage := stageNormalized.Stages[0]
	action := stage.Actions[0]
	stageRecord := StageRecord{
		ID:          stage.ID,
		Name:        stage.Name,
		Description: stage.Description,
		Tags:        append([]string(nil), stage.Tags...),
		Dynamic:     false,
		CreatedBy:   "",
		Status:      WorkflowRunning,
	}
	if err := store.RecordStage(ctx, wfID, stageRecord); err != nil {
		t.Fatalf("record stage: %v", err)
	}
	actionRecord := ActionRecord{
		Name:        action.ID,
		Ref:         action.Ref,
		Description: action.Description,
		Tags:        append([]string(nil), action.Tags...),
		Dynamic:     false,
		CreatedBy:   "",
		Status:      WorkflowRunning,
	}
	if err := store.RecordAction(ctx, wfID, stage.ID, actionRecord); err != nil {
		t.Fatalf("record action: %v", err)
	}

	summaryCompletedAt := time.Now().UTC().Truncate(time.Millisecond)
	summary := ResultSummary{
		Success:        true,
		Output:         map[string]any{"status": "ok"},
		DisabledStages: map[string]bool{stage.ID: false},
		Attempt:        2,
		Duration:       1500 * time.Millisecond,
		CompletedAt:    summaryCompletedAt,
		Reason:         TerminationReasonSuccess,
	}

	go func() {
		// Ensure the waiter has been registered before storing the summary to
		// avoid racing with the synchronous fetch path.
		time.Sleep(10 * time.Millisecond)
		if err := store.StoreSummary(ctx, wfID, summary); err != nil {
			t.Errorf("store summary: %v", err)
		}
	}()

	res, err := store.WaitResult(ctx, wfID)
	if err != nil {
		t.Fatalf("wait result: %v", err)
	}
	if !res.Success || res.Output["status"] != "ok" {
		t.Fatalf("summary mismatch: %#v", res)
	}
	if res.Attempt != summary.Attempt {
		t.Fatalf("expected attempt %d, got %d", summary.Attempt, res.Attempt)
	}
	if res.Duration != summary.Duration {
		t.Fatalf("expected duration %s, got %s", summary.Duration, res.Duration)
	}
	if res.Reason != TerminationReasonSuccess {
		t.Fatalf("expected reason success, got %s", res.Reason)
	}
	if diff := res.CompletedAt.Sub(summaryCompletedAt); diff > time.Millisecond || diff < -time.Millisecond {
		t.Fatalf("expected completed_at within 1ms, got diff %s (%s vs %s)", diff, res.CompletedAt, summaryCompletedAt)
	}

	// Simulate a restart by constructing a fresh store and waiting again.
	storeRestart, err := NewSQLiteStore(db)
	if err != nil {
		t.Fatalf("new store restart: %v", err)
	}
	resRestart, err := storeRestart.WaitResult(ctx, wfID)
	if err != nil {
		t.Fatalf("wait result after restart: %v", err)
	}
	if !resRestart.Success || resRestart.Attempt != summary.Attempt || resRestart.Duration != summary.Duration {
		t.Fatalf("restart summary mismatch: %#v", resRestart)
	}
	if resRestart.Reason != TerminationReasonSuccess {
		t.Fatalf("expected restart reason success, got %s", resRestart.Reason)
	}
	if diff := resRestart.CompletedAt.Sub(summaryCompletedAt); diff > time.Millisecond || diff < -time.Millisecond {
		t.Fatalf("expected restart completed_at within 1ms, got diff %s (%s vs %s)", diff, resRestart.CompletedAt, summaryCompletedAt)
	}

	// Update workflow state to completed.
	completedAt := time.Now()
	duration := time.Second
	success := true
	errorMsg := ""
	if err := store.UpdateWorkflowStatus(ctx, WorkflowStatusUpdate{
		ID:          wfID,
		Status:      WorkflowCompleted,
		CompletedAt: &completedAt,
		Duration:    &duration,
		Success:     &success,
		Error:       &errorMsg,
	}); err != nil {
		t.Fatalf("record workflow update: %v", err)
	}

	summaryRow, err := reader.WorkflowSummary(ctx, wfID)
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	if summaryRow.State != WorkflowCompleted || !summaryRow.Success {
		t.Fatalf("summary state mismatch: %#v", summaryRow)
	}
	var storedStageDescription sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT description FROM stage_runs WHERE workflow_id = ? AND stage_id = ?`, wfID, stage.ID).Scan(&storedStageDescription); err != nil {
		t.Fatalf("fetch stage description: %v", err)
	}
	if storedStageDescription.String != stage.Description {
		t.Fatalf("expected stage description %q, got %q", stage.Description, storedStageDescription.String)
	}

	history, err := reader.ActionHistory(ctx, wfID)
	if err != nil {
		t.Fatalf("action history: %v", err)
	}
	if len(history) != 1 || history[0].ActionID != action.ID || history[0].StageID != stage.ID {
		t.Fatalf("unexpected history: %#v", history)
	}
	if history[0].Ref != action.Ref {
		t.Fatalf("expected history ref %q, got %q", action.Ref, history[0].Ref)
	}
	if history[0].Description != action.Description {
		t.Fatalf("expected history description %q, got %q", action.Description, history[0].Description)
	}
}
