package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/runner"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestCancelWorkflowEmitsTelemetry(t *testing.T) {
	ctx := context.Background()
	tele := node.NewTelemetryDispatcher(ctx, nil, node.TelemetryDispatcherConfig{})
	defer tele.Close()

	sink := telemetry.NewChannelSink(2)
	cancelSink := tele.Register(sink)
	defer cancelSink()

	dispatcher := New(ctx, noopQueue{}, &fakeStore{}, &fakeManager{}, nil, tele, nil, nil, telemetry.NoopLogger{}, nil, Options{})

	id := state.WorkflowID("wf-1")
	dispatcher.CancelWorkflow(id)

	var evt telemetry.Event
	select {
	case evt = <-sink.C():
	case <-time.After(time.Second):
		t.Fatalf("expected telemetry event for pending cancel")
	}
	if evt.Kind != telemetry.EventWorkflowCancelRequest {
		t.Fatalf("expected cancel request, got %s", evt.Kind)
	}
	if pending, ok := evt.Metadata["pending"].(bool); !ok || !pending {
		t.Fatalf("expected pending metadata, got %#v", evt.Metadata)
	}

	triggered := make(chan struct{}, 1)
	dispatcher.RegisterCancel(id, func() { triggered <- struct{}{} })
	select {
	case <-triggered:
	case <-time.After(time.Second):
		t.Fatalf("expected registered cancel callback to run")
	}
	dispatcher.CancelWorkflow(id)

	select {
	case evt = <-sink.C():
	case <-time.After(time.Second):
		t.Fatalf("expected telemetry event for inflight cancel")
	}
	if inflight, ok := evt.Metadata["inflight"].(bool); !ok || !inflight {
		t.Fatalf("expected inflight metadata, got %#v", evt.Metadata)
	}
	select {
	case <-triggered:
	case <-time.After(time.Second):
		t.Fatalf("expected second cancel callback to run")
	}
}

func TestPublishHealthTransitions(t *testing.T) {
	ctx := context.Background()
	health := node.NewHealthDispatcher()
	events := make(chan node.HealthEvent, 3)
	unsub := health.Subscribe(func(evt node.HealthEvent) {
		events <- evt
	})
	defer unsub()

	pool := pools.NewLocal("pool-1", state.Selector{}, 1)
	bindings := []*Binding{{Pool: pool}}
	dispatcher := New(ctx, noopQueue{}, &fakeStore{}, &fakeManager{}, nil, nil, nil, health, telemetry.NoopLogger{}, bindings, Options{})

	dispatcher.PublishHealth(pool.Name(), node.HealthHealthy, "ready")
	dispatcher.PublishHealth(pool.Name(), node.HealthHealthy, "ready") // no-op
	dispatcher.PublishHealth(pool.Name(), node.HealthDegraded, "error")

	var first node.HealthEvent
	select {
	case first = <-events:
	case <-time.After(time.Second):
		t.Fatalf("expected initial health event")
	}
	if first.Status != node.HealthHealthy || first.Detail != "ready" {
		t.Fatalf("unexpected initial event: %+v", first)
	}
	var degraded node.HealthEvent
	select {
	case degraded = <-events:
	case <-time.After(time.Second):
		t.Fatalf("expected degraded health event")
	}
	if degraded.Status != node.HealthDegraded || degraded.Detail != "error" {
		t.Fatalf("unexpected degraded event: %+v", degraded)
	}
	select {
	case <-events:
		t.Fatalf("unexpected extra health event")
	default:
	}
}

func TestCompleteRemoteMissingReasonDiagnostic(t *testing.T) {
	ctx := context.Background()
	pool := pools.NewLocal("remote", state.Selector{}, 1)
	bindings := []*Binding{{Pool: pool}}

	diag := &diagCollector{}
	manager := &fakeManager{}
	store := &fakeStore{}
	tele := node.NewTelemetryDispatcher(ctx, diag, node.TelemetryDispatcherConfig{})
	defer tele.Close()

	dispatcher := New(ctx, noopQueue{}, store, manager, nil, tele, diag, nil, telemetry.NoopLogger{}, bindings, Options{
		FailurePolicy: bootstrap.FailurePolicyFunc(func(context.Context, bootstrap.FailureContext) bootstrap.FailureOutcome {
			return bootstrap.FailureOutcome{
				Action:     bootstrap.FailureActionAck,
				FinalState: runner.StatusFailed,
				Reason:     "",
			}
		}),
	})

	claimed := &state.ClaimedWorkflow{
		QueuedWorkflow: state.QueuedWorkflow{
			ID:         state.WorkflowID("wf"),
			Definition: workflow.Definition{ID: "wf"},
			Attempt:    1,
		},
	}
	dispatcher.BeginRemoteDispatch(claimed.ID, func() {})

	releaseCalled := false
	summary := state.ResultSummary{
		Success: false,
		Attempt: claimed.Attempt,
	}
	dispatcher.CompleteRemote(claimed, pool, summary, func() { releaseCalled = true })

	if !releaseCalled {
		t.Fatalf("expected release callback to be invoked")
	}
	events := diag.Events()
	if len(events) == 0 {
		t.Fatalf("expected diagnostics event for missing reason")
	}
	var reasonEvent diagnostics.Event
	foundReason := false
	for _, evt := range events {
		if evt.Component == "dispatcher.reason.missing" {
			reasonEvent = evt
			foundReason = true
			break
		}
	}
	if !foundReason {
		t.Fatalf("expected dispatcher.reason.missing diagnostic, got %v", events)
	}
	if reasonEvent.Metadata == nil {
		t.Fatalf("expected metadata on reason diagnostic")
	}
	workflowID, ok := reasonEvent.Metadata["workflow_id"].(string)
	if !ok || workflowID != "wf" {
		t.Fatalf("expected workflow metadata on reason diagnostic, got %+v", reasonEvent.Metadata)
	}
	if len(manager.summaries) != 1 {
		t.Fatalf("expected one stored summary, got %d", len(manager.summaries))
	}
	if manager.summaries[0].Reason != state.TerminationReasonFailure {
		t.Fatalf("expected failure reason, got %s", manager.summaries[0].Reason)
	}
}

func TestCompleteRemoteMissingTelemetryDiagnostic(t *testing.T) {
	ctx := context.Background()
	pool := pools.NewLocal("remote", state.Selector{}, 1)
	bindings := []*Binding{{Pool: pool}}

	diag := &diagCollector{}
	tele := node.NewTelemetryDispatcher(ctx, diag, node.TelemetryDispatcherConfig{})
	defer tele.Close()

	dispatcher := New(ctx, noopQueue{}, &fakeStore{}, &fakeManager{}, nil, tele, diag, nil, telemetry.NoopLogger{}, bindings, Options{})

	claimed := &state.ClaimedWorkflow{
		QueuedWorkflow: state.QueuedWorkflow{
			ID:         state.WorkflowID("wf-tele-missing"),
			Definition: workflow.Definition{ID: "wf"},
			Attempt:    1,
		},
	}
	dispatcher.BeginRemoteDispatch(claimed.ID, func() {})

	summary := state.ResultSummary{
		Success: true,
		Reason:  state.TerminationReasonSuccess,
		Attempt: claimed.Attempt,
	}
	dispatcher.CompleteRemote(claimed, pool, summary, func() {})

	var telemetryDiag diagnostics.Event
	found := false
	for _, evt := range diag.Events() {
		if evt.Component == "telemetry.missing" {
			telemetryDiag = evt
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected telemetry.missing diagnostic, got %v", diag.Events())
	}
	missing, ok := telemetryDiag.Metadata["missing"].([]string)
	if !ok {
		t.Fatalf("expected missing metadata slice, got %T", telemetryDiag.Metadata["missing"])
	}
	if len(missing) == 0 {
		t.Fatalf("expected at least one missing telemetry event")
	}
}

func TestCompleteRemoteTelemetryCoverageSatisfied(t *testing.T) {
	ctx := context.Background()
	pool := pools.NewLocal("remote", state.Selector{}, 1)
	bindings := []*Binding{{Pool: pool}}

	diag := &diagCollector{}
	tele := node.NewTelemetryDispatcher(ctx, diag, node.TelemetryDispatcherConfig{})
	defer tele.Close()

	dispatcher := New(ctx, noopQueue{}, &fakeStore{}, &fakeManager{}, nil, tele, diag, nil, telemetry.NoopLogger{}, bindings, Options{})

	claimed := &state.ClaimedWorkflow{
		QueuedWorkflow: state.QueuedWorkflow{
			ID:         state.WorkflowID("wf-tele-present"),
			Definition: workflow.Definition{ID: "wf"},
			Attempt:    1,
		},
	}
	dispatcher.BeginRemoteDispatch(claimed.ID, func() {})

	_ = tele.Dispatch(telemetry.Event{Kind: telemetry.EventWorkflowCompleted, WorkflowID: string(claimed.ID)})
	_ = tele.Dispatch(telemetry.Event{Kind: telemetry.EventWorkflowSummary, WorkflowID: string(claimed.ID)})

	summary := state.ResultSummary{
		Success: true,
		Reason:  state.TerminationReasonSuccess,
		Attempt: claimed.Attempt,
	}
	dispatcher.CompleteRemote(claimed, pool, summary, func() {})

	for _, evt := range diag.Events() {
		if evt.Component == "telemetry.missing" {
			t.Fatalf("unexpected telemetry.missing diagnostic when coverage satisfied: %+v", evt)
		}
	}
}

type noopQueue struct{}

func (noopQueue) Claim(context.Context, state.Selector, string) (*state.ClaimedWorkflow, error) {
	return nil, state.ErrNoPending
}
func (noopQueue) Release(context.Context, state.WorkflowID) error                  { return nil }
func (noopQueue) Ack(context.Context, state.WorkflowID, state.ResultSummary) error { return nil }
func (noopQueue) Cancel(context.Context, state.WorkflowID) error                   { return nil }
func (noopQueue) PendingCount(context.Context, state.Selector) (int, error)        { return 0, nil }
func (noopQueue) Stats(context.Context) (state.QueueStats, error)                  { return state.QueueStats{}, nil }

type fakeManager struct {
	mu        sync.Mutex
	summaries []state.ExecutionReport
}

func (m *fakeManager) WorkflowRegistered(context.Context, state.WorkflowRecord) error { return nil }
func (m *fakeManager) WorkflowStatus(context.Context, string, state.WorkflowState) error {
	return nil
}
func (m *fakeManager) StageRegistered(context.Context, string, state.StageRecord) error { return nil }
func (m *fakeManager) StageStatus(context.Context, string, string, state.WorkflowState) error {
	return nil
}
func (m *fakeManager) ActionRegistered(context.Context, string, string, state.ActionRecord) error {
	return nil
}
func (m *fakeManager) ActionStatus(context.Context, string, string, string, state.WorkflowState) error {
	return nil
}
func (m *fakeManager) ActionProgress(context.Context, string, string, string, int, string) error {
	return nil
}
func (m *fakeManager) ActionEvent(context.Context, string, string, string, string, string, map[string]any) error {
	return nil
}
func (m *fakeManager) ActionRemoved(context.Context, string, string, string, string) error {
	return nil
}
func (m *fakeManager) StageRemoved(context.Context, string, string, string) error { return nil }
func (m *fakeManager) StoreExecutionSummary(ctx context.Context, id string, summary state.ExecutionReport) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.summaries = append(m.summaries, summary)
	return nil
}

type fakeStore struct {
	mu        sync.Mutex
	summaries []state.ResultSummary
}

func (s *fakeStore) StoreSummary(ctx context.Context, id state.WorkflowID, summary state.ResultSummary) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.summaries = append(s.summaries, summary)
	return nil
}

type diagCollector struct {
	mu     sync.Mutex
	events []diagnostics.Event
}

func (d *diagCollector) Write(evt diagnostics.Event) {
	d.mu.Lock()
	d.events = append(d.events, evt)
	d.mu.Unlock()
}

func (d *diagCollector) Events() []diagnostics.Event {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]diagnostics.Event, len(d.events))
	copy(out, d.events)
	return out
}
