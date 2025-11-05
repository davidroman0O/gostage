package orchestrator

import (
	"context"
	"testing"
	"time"

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

	dispatcher := node.NewTelemetryDispatcher(context.Background(), nil, node.TelemetryDispatcherConfig{})
	defer dispatcher.Close()

	sink := telemetry.NewChannelSink(8)
	_ = dispatcher.Register(sink)

	wrapped := WrapWithTelemetry(manager, dispatcher)

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

	dispatcher := node.NewTelemetryDispatcher(context.Background(), nil, node.TelemetryDispatcherConfig{})
	defer dispatcher.Close()

	sink := telemetry.NewChannelSink(4)
	_ = dispatcher.Register(sink)

	wrapped := WrapWithTelemetry(manager, dispatcher)

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

func TestTelemetryManagerSuppressWorkflowEvents(t *testing.T) {
	store := state.NewMemoryStore()
	manager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}

	dispatcher := node.NewTelemetryDispatcher(context.Background(), nil, node.TelemetryDispatcherConfig{})
	defer dispatcher.Close()

	sink := telemetry.NewChannelSink(8)
	_ = dispatcher.Register(sink)

	wrapped := WrapWithTelemetry(manager, dispatcher)

	suppressor, ok := wrapped.(interface {
		SuppressWorkflowEvents(string, ...telemetry.EventKind)
	})
	if !ok {
		t.Fatalf("wrapped manager missing suppression support")
	}

	wf := state.WorkflowRecord{
		ID:   state.WorkflowID("wf-suppress"),
		Name: "suppress",
	}
	if err := wrapped.WorkflowRegistered(context.Background(), wf); err != nil {
		t.Fatalf("workflow registered: %v", err)
	}
	<-sink.C() // drain registration

	suppressor.SuppressWorkflowEvents(string(wf.ID), telemetry.EventWorkflowStarted, telemetry.EventWorkflowSummary)

	if err := wrapped.WorkflowStatus(context.Background(), string(wf.ID), state.WorkflowRunning); err != nil {
		t.Fatalf("workflow status running: %v", err)
	}
	select {
	case evt := <-sink.C():
		t.Fatalf("unexpected telemetry event during suppression: %s", evt.Kind)
	case <-time.After(50 * time.Millisecond):
	}

	report := state.ExecutionReport{
		WorkflowID:   string(wf.ID),
		WorkflowName: wf.Name,
		Status:       state.WorkflowCompleted,
		Success:      true,
	}
	if err := wrapped.StoreExecutionSummary(context.Background(), string(wf.ID), report); err != nil {
		t.Fatalf("store execution summary: %v", err)
	}
	select {
	case evt := <-sink.C():
		t.Fatalf("unexpected summary telemetry during suppression: %s", evt.Kind)
	case <-time.After(50 * time.Millisecond):
	}

	if err := wrapped.WorkflowStatus(context.Background(), string(wf.ID), state.WorkflowCompleted); err != nil {
		t.Fatalf("workflow status completed: %v", err)
	}
	evt := <-sink.C()
	if evt.Kind != telemetry.EventWorkflowCompleted {
		t.Fatalf("expected workflow.completed after suppression cleared, got %s", evt.Kind)
	}
}

func TestTelemetryManagerAutoSuppressesRemovalEvents(t *testing.T) {
	store := state.NewMemoryStore()
	manager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}

	dispatcher := node.NewTelemetryDispatcher(context.Background(), nil, node.TelemetryDispatcherConfig{})
	defer dispatcher.Close()

	sink := telemetry.NewChannelSink(4)
	_ = dispatcher.Register(sink)

	wrapped := WrapWithTelemetry(manager, dispatcher)

	wf := state.WorkflowRecord{ID: state.WorkflowID("wf-auto")}
	if err := wrapped.WorkflowRegistered(context.Background(), wf); err != nil {
		t.Fatalf("workflow registered: %v", err)
	}
	<-sink.C() // drain workflow.registered

	stage := state.StageRecord{ID: "stage-1"}
	if err := wrapped.StageRegistered(context.Background(), string(wf.ID), stage); err != nil {
		t.Fatalf("stage registered: %v", err)
	}
	<-sink.C()

	if err := wrapped.StageRemoved(context.Background(), string(wf.ID), stage.ID, "runner"); err != nil {
		t.Fatalf("stage removed: %v", err)
	}
	first := <-sink.C()
	if first.Kind != telemetry.EventStageRemoved {
		t.Fatalf("expected stage.removed, got %s", first.Kind)
	}

	if err := wrapped.StageRemoved(context.Background(), string(wf.ID), stage.ID, "store"); err != nil {
		t.Fatalf("duplicate stage removed: %v", err)
	}
	select {
	case evt := <-sink.C():
		t.Fatalf("unexpected duplicate telemetry event: %s", evt.Kind)
	case <-time.After(50 * time.Millisecond):
	}
}
