package state

import (
	"context"
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

	stage := workflow.Stage{ID: "stage-1", Name: "Prepare"}
	if err := store.RecordStage(ctx, wfID, stage, false, "", WorkflowRunning); err != nil {
		t.Fatalf("record stage: %v", err)
	}
	action := workflow.Action{ID: "action-1", Ref: "prepare.ref"}
	if err := store.RecordAction(ctx, wfID, stage.ID, action, false, "", WorkflowRunning); err != nil {
		t.Fatalf("record action: %v", err)
	}

	done := make(chan ResultSummary, 1)
	errCh := make(chan error, 1)
	go func() {
		res, err := store.WaitResult(ctx, wfID)
		if err != nil {
			errCh <- err
			return
		}
		done <- res
	}()

	summary := ResultSummary{
		Success:        true,
		Output:         map[string]any{"status": "ok"},
		DisabledStages: map[string]bool{"stage-1": false},
	}
	if err := store.StoreSummary(ctx, wfID, summary); err != nil {
		t.Fatalf("store summary: %v", err)
	}

	select {
	case err := <-errCh:
		t.Fatalf("wait result: %v", err)
	case res := <-done:
		if !res.Success || res.Output["status"] != "ok" {
			t.Fatalf("summary mismatch: %#v", res)
		}
	}

	// Update workflow state to completed.
	rec.State = WorkflowCompleted
	rec.Success = true
	rec.CompletedAt = func() *time.Time { t := time.Now(); return &t }()
	rec.Duration = time.Second
	if err := store.RecordWorkflow(ctx, rec); err != nil {
		t.Fatalf("record workflow update: %v", err)
	}

	summaryRow, err := reader.WorkflowSummary(ctx, wfID)
	if err != nil {
		t.Fatalf("read summary: %v", err)
	}
	if summaryRow.State != WorkflowCompleted || !summaryRow.Success {
		t.Fatalf("summary state mismatch: %#v", summaryRow)
	}

	history, err := reader.ActionHistory(ctx, wfID)
	if err != nil {
		t.Fatalf("action history: %v", err)
	}
	if len(history) != 1 || history[0].ActionID != "action-1" {
		t.Fatalf("unexpected history: %#v", history)
	}
}
