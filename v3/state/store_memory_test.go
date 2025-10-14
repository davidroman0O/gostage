package state

import (
	"context"
	"testing"

	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestMemoryStoreWait(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	wfID := WorkflowID("wf")
	rec := WorkflowRecord{ID: wfID, Name: "demo"}
	if err := store.RecordWorkflow(ctx, rec); err != nil {
		t.Fatalf("record: %v", err)
	}

	done := make(chan ResultSummary, 1)
	go func() {
		res, err := store.WaitResult(ctx, wfID)
		if err != nil {
			t.Fatalf("wait result: %v", err)
		}
		done <- res
	}()

	summary := ResultSummary{Success: true}
	if err := store.StoreSummary(ctx, wfID, summary); err != nil {
		t.Fatalf("store summary: %v", err)
	}

	res := <-done
	if !res.Success {
		t.Fatalf("expected success")
	}

	// Cached path
	cached, err := store.WaitResult(ctx, wfID)
	if err != nil || !cached.Success {
		t.Fatalf("cached wait: %v, %#v", err, cached)
	}

	// Stage/action recording should not panic
	stage := workflow.Stage{ID: "stage"}
	if err := store.RecordStage(ctx, wfID, stage, false, "", WorkflowRunning); err != nil {
		t.Fatalf("record stage: %v", err)
	}
	action := workflow.Action{ID: "action", Ref: "ref"}
	if err := store.RecordAction(ctx, wfID, "stage", action, false, "", WorkflowRunning); err != nil {
		t.Fatalf("record action: %v", err)
	}
}
