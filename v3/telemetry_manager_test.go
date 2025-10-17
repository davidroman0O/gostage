package gostage

import (
	"context"
	"testing"

	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestTelemetryManagerEmitsWorkflowStatus(t *testing.T) {
	store := state.NewMemoryStore()
	manager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}

	dispatcher := node.NewTelemetryDispatcher(context.Background(), nil)
	defer dispatcher.Close()

	sink := telemetry.NewChannelSink(8)
	_ = dispatcher.Register(sink)

	wrapped := wrapWithTelemetry(manager, dispatcher)

	wf := state.WorkflowRecord{
		ID:   state.WorkflowID("wf-1"),
		Name: "test",
		Tags: []string{"alpha"},
	}
	if err := wrapped.WorkflowRegistered(context.Background(), wf); err != nil {
		t.Fatalf("workflow registered: %v", err)
	}
	<-sink.C() // drain registered event

	if err := wrapped.WorkflowStatus(context.Background(), string(wf.ID), state.WorkflowRunning); err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	evt := <-sink.C()
	if evt.Kind != telemetry.EventWorkflowStarted {
		t.Fatalf("expected %s, got %s", telemetry.EventWorkflowStarted, evt.Kind)
	}
	if evt.WorkflowID != string(wf.ID) {
		t.Fatalf("unexpected workflow id: %s", evt.WorkflowID)
	}
}

func TestTelemetryManagerEmitsActionProgress(t *testing.T) {
	store := state.NewMemoryStore()
	manager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}
	def := workflow.Definition{Stages: []workflow.Stage{{Name: "Stage", Actions: []workflow.Action{{Ref: "noop"}}}}}
	normalized, assignment, err := workflow.EnsureIDs(def)
	if err != nil {
		t.Fatalf("ensure ids: %v", err)
	}
	stageID := assignment.Stages[0].ID
	actionID := assignment.Stages[0].Actions[0].ID

	dispatcher := node.NewTelemetryDispatcher(context.Background(), nil)
	defer dispatcher.Close()

	sink := telemetry.NewChannelSink(4)
	_ = dispatcher.Register(sink)

	wrapped := wrapWithTelemetry(manager, dispatcher)

	wf := state.WorkflowRecord{
		ID:   state.WorkflowID("wf-2"),
		Name: "progress",
		Stages: map[string]*state.StageRecord{
			stageID: {
				ID:   normalized.Stages[0].ID,
				Name: normalized.Stages[0].Name,
				Actions: map[string]*state.ActionRecord{
					actionID: {Name: actionID},
				},
			},
		},
	}

	if err := wrapped.WorkflowRegistered(context.Background(), wf); err != nil {
		t.Fatalf("workflow registered: %v", err)
	}
	<-sink.C() // drain registration

	if err := wrapped.StageRegistered(context.Background(), string(wf.ID), *wf.Stages[stageID]); err != nil {
		t.Fatalf("stage registered: %v", err)
	}
	<-sink.C()

	if err := wrapped.ActionRegistered(context.Background(), string(wf.ID), stageID, *wf.Stages[stageID].Actions[actionID]); err != nil {
		t.Fatalf("action registered: %v", err)
	}
	<-sink.C()

	if err := wrapped.ActionProgress(context.Background(), string(wf.ID), stageID, actionID, 42, "halfway"); err != nil {
		t.Fatalf("action progress: %v", err)
	}
	evt := <-sink.C()
	if evt.Kind != telemetry.EventActionProgressKind {
		t.Fatalf("expected action.progress, got %s", evt.Kind)
	}
	if evt.Progress == nil || evt.Progress.Percent != 42 || evt.Progress.Message != "halfway" {
		t.Fatalf("unexpected progress payload %#v", evt.Progress)
	}
	if evt.Metadata["progress"] != 42 {
		t.Fatalf("expected progress 42, got %+v", evt.Metadata["progress"])
	}
	if evt.Metadata["message"] != "halfway" {
		t.Fatalf("expected message 'halfway', got %v", evt.Metadata["message"])
	}
}
