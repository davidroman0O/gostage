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
	definition := workflow.Definition{Stages: []workflow.Stage{{Actions: []workflow.Action{{Ref: "ref"}}}}}
	normalized, _, err := workflow.EnsureIDs(definition)
	if err != nil {
		t.Fatalf("ensure ids: %v", err)
	}
	stage := normalized.Stages[0]
	action := stage.Actions[0]
	stageRecord := StageRecord{
		ID:     stage.ID,
		Name:   stage.Name,
		Tags:   append([]string(nil), stage.Tags...),
		Status: WorkflowRunning,
	}
	if err := store.RecordStage(ctx, wfID, stageRecord); err != nil {
		t.Fatalf("record stage: %v", err)
	}
	actionRecord := ActionRecord{
		Name:   action.ID,
		Ref:    action.Ref,
		Tags:   append([]string(nil), action.Tags...),
		Status: WorkflowRunning,
	}
	if err := store.RecordAction(ctx, wfID, stage.ID, actionRecord); err != nil {
		t.Fatalf("record action: %v", err)
	}
}
