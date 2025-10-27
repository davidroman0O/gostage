package gostage

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/davidroman0O/gostage/v3/broker"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/process"
	processproto "github.com/davidroman0O/gostage/v3/process/proto"
	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/runner"
	"github.com/davidroman0O/gostage/v3/runtime/local"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

type diagRecorder struct {
	mu     sync.Mutex
	events []diagnostics.Event
}

func (r *diagRecorder) Write(evt diagnostics.Event) {
	r.mu.Lock()
	r.events = append(r.events, evt)
	r.mu.Unlock()
}

func (r *diagRecorder) Events() []diagnostics.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]diagnostics.Event, len(r.events))
	copy(out, r.events)
	return out
}

func TestRemoteCoordinatorDispatchLifecycle(t *testing.T) {
	t.Skip("remote coordinator integration pending")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queue := state.NewMemoryQueue()
	store := state.NewMemoryStore()
	manager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	base := node.New(ctx, nil, node.TelemetryDispatcherConfig{})
	health := node.NewHealthDispatcher()
	managerWithTelemetry := wrapWithTelemetry(manager, base.TelemetryDispatcher())

	brokerInstance := broker.NewLocal(managerWithTelemetry)
	runOpts := []runner.Option{runner.WithDefaultLogger(telemetry.NoopLogger{})}
	run := runner.New(local.Factory{}, registry.Default(), brokerInstance, runOpts...)

	binding := &poolBinding{
		pool: pools.NewLocal("remote", state.Selector{}, 1),
		remote: &remoteBinding{
			poolCfg: PoolConfig{Name: "remote", Slots: 1},
		},
	}

	dispatcher := newDispatcher(ctx, queue, store, managerWithTelemetry, run, base.TelemetryDispatcher(), base.DiagnosticsWriter(), health, telemetry.NoopLogger{}, 0, 0, 0, nil, []*poolBinding{binding})
	remoteCoord, err := newRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), base.DiagnosticsWriter(), health, telemetry.NoopLogger{}, []*poolBinding{binding})
	if err != nil {
		t.Fatalf("remote coordinator: %v", err)
	}
	dispatcher.start()
	defer dispatcher.stop()
	defer remoteCoord.shutdown()

	dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
	defer cancelDial()

	conn, err := grpc.DialContext(dialCtx, remoteCoord.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := processproto.NewProcessBridgeClient(conn)
	stream, err := client.Control(dialCtx)
	if err != nil {
		t.Fatalf("control: %v", err)
	}
	defer stream.CloseSend()

	register := &processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_Register{Register: &processproto.RegisterNode{
		NodeId: "child-1",
		Pools:  []*processproto.ChildPool{{Name: "remote", Slots: 1}},
	}}}
	if err := stream.Send(register); err != nil {
		t.Fatalf("send register: %v", err)
	}
	if ack, err := stream.Recv(); err != nil {
		t.Fatalf("recv ack: %v", err)
	} else if ack.GetRegisterAck() == nil {
		t.Fatalf("expected register ack, got %T", ack.GetBody())
	}

	def := workflow.Definition{
		ID:   "wf-remote",
		Name: "RemoteWorkflow",
		Stages: []workflow.Stage{{
			ID: "stage-1",
			Actions: []workflow.Action{{
				ID:  "action-1",
				Ref: "noop",
			}},
		}},
	}

	workflowID, err := queue.Enqueue(ctx, def, state.PriorityDefault, nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	dispatcher.pollOnce()

	grantMsg, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv grant: %v", err)
	}
	grant := grantMsg.GetLeaseGrant()
	if grant == nil {
		t.Fatalf("expected lease grant, got %T", grantMsg.GetBody())
	}
	if grant.WorkflowId != string(workflowID) {
		t.Fatalf("expected workflow %s, got %s", workflowID, grant.WorkflowId)
	}

	summary := &processproto.ResultSummary{Success: true}
	ack := &processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_LeaseAck{LeaseAck: &processproto.LeaseAck{
		WorkflowId: grant.WorkflowId,
		LeaseId:    grant.LeaseId,
		Summary:    summary,
	}}}
	if err := stream.Send(ack); err != nil {
		t.Fatalf("send ack: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	result, err := store.WaitResult(waitCtx, workflowID)
	if err != nil {
		t.Fatalf("wait result: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success result, got %+v", result)
	}

	stats, err := queue.Stats(ctx)
	if err != nil {
		t.Fatalf("queue stats: %v", err)
	}
	if stats.Pending != 0 || stats.Claimed != 0 {
		t.Fatalf("expected empty queue, got %+v", stats)
	}

	remoteCoord.mu.Lock()
	if len(remoteCoord.jobs) != 0 {
		t.Fatalf("expected jobs to be cleared, got %d", len(remoteCoord.jobs))
	}
	if binding.remote.pool.worker == nil || binding.remote.pool.worker.busy {
		t.Fatalf("expected worker to be idle")
	}
	remoteCoord.mu.Unlock()
}

func TestRemoteCoordinatorOnLogPublishesDiagnostics(t *testing.T) {
	rec := &diagRecorder{}
	rc := &remoteCoordinator{
		diagnostics: rec,
		logger:      telemetry.NoopLogger{},
		pools:       make(map[string]*remotePool),
	}

	entry := process.LogEntry{
		OccurredAt:  time.Now(),
		Level:       "info",
		Logger:      "child",
		Message:     "structured log",
		Attributes:  map[string]string{"key": "value"},
		Pool:        "remote",
		ChildNodeID: "child-1",
	}

	if err := rc.OnLog(context.Background(), nil, entry); err != nil {
		t.Fatalf("OnLog returned error: %v", err)
	}

	events := rec.Events()
	if len(events) == 0 {
		t.Fatalf("expected diagnostic event, got none")
	}
	logEvent := events[len(events)-1]
	if logEvent.Component != "remote.log.remote" {
		t.Fatalf("unexpected component %q", logEvent.Component)
	}
	if logEvent.Metadata["message"] != entry.Message {
		t.Fatalf("unexpected message metadata: %+v", logEvent.Metadata)
	}
	if logEvent.Metadata["structured"] != true {
		t.Fatalf("expected structured flag, got %+v", logEvent.Metadata)
	}
	if logEvent.Metadata["key"] != "value" {
		t.Fatalf("missing attributes: %+v", logEvent.Metadata)
	}
	if logEvent.Metadata["child_node_id"] != entry.ChildNodeID {
		t.Fatalf("missing child_node_id: %+v", logEvent.Metadata)
	}
}

func TestRemoteCoordinatorForwardChildLog(t *testing.T) {
	recorder := &diagRecorder{}
	rc := &remoteCoordinator{diagnostics: recorder}

	rc.forwardChildLog("pool-1", diagnostics.Event{
		Component: "spawner.process.stdout",
		Severity:  diagnostics.SeverityInfo,
		Metadata: map[string]any{
			"line": "child log line",
			"pid":  42,
		},
	})

	events := recorder.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 diagnostic event, got %d", len(events))
	}
	evt := events[0]
	if evt.Component != "remote.child.pool-1" {
		t.Fatalf("unexpected component %s", evt.Component)
	}
	if evt.Metadata["pool"] != "pool-1" {
		t.Fatalf("expected pool metadata, got %+v", evt.Metadata)
	}
	if evt.Metadata["line"] != "child log line" {
		t.Fatalf("expected log line metadata, got %+v", evt.Metadata)
	}
	if evt.Metadata["stream"] != "spawner.process.stdout" {
		t.Fatalf("expected stream metadata, got %+v", evt.Metadata)
	}
}
