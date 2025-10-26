package child

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/process"
	processproto "github.com/davidroman0O/gostage/v3/process/proto"
	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type testHandler struct {
	registerCount int
}

func (h *testHandler) OnRegister(ctx context.Context, conn *process.Connection, req *processproto.RegisterNode) (*processproto.RegisterAck, error) {
	h.registerCount++
	go func() {
		time.Sleep(50 * time.Millisecond)
		conn.Close("test")
	}()
	return &processproto.RegisterAck{}, nil
}

func (h *testHandler) OnLeaseAck(context.Context, *process.Connection, string, string, state.ResultSummary) error {
	return nil
}
func (h *testHandler) OnTelemetry(context.Context, *process.Connection, telemetry.Event) error {
	return nil
}
func (h *testHandler) OnDiagnostic(context.Context, *process.Connection, diagnostics.Event) {}
func (h *testHandler) OnHeartbeat(context.Context, *process.Connection, time.Time) error    { return nil }
func (h *testHandler) OnShutdown(context.Context, *process.Connection, string)              {}

type stubControlStream struct {
	mu   sync.Mutex
	sent []*processproto.ControlEnvelope
}

func (s *stubControlStream) Send(env *processproto.ControlEnvelope) error {
	s.mu.Lock()
	s.sent = append(s.sent, env)
	s.mu.Unlock()
	return nil
}

func (s *stubControlStream) Recv() (*processproto.ControlEnvelope, error) {
	return nil, io.EOF
}

func (s *stubControlStream) Header() (metadata.MD, error) { return nil, nil }

func (s *stubControlStream) Trailer() metadata.MD { return nil }

func (s *stubControlStream) CloseSend() error { return nil }

func (s *stubControlStream) Context() context.Context { return context.Background() }

func (s *stubControlStream) SendMsg(interface{}) error { return nil }

func (s *stubControlStream) RecvMsg(interface{}) error { return io.EOF }

func (s *stubControlStream) Snapshot() []*processproto.ControlEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*processproto.ControlEnvelope, len(s.sent))
	copy(out, s.sent)
	return out
}

func TestNodeRunConnectsAndEmitsDiagnostics(t *testing.T) {
	handler := &testHandler{}
	srv := grpc.NewServer()
	processproto.RegisterProcessBridgeServer(srv, process.NewServer(handler))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})

	node := NewNode(Config{Address: lis.Addr().String()})
	diag := node.Diagnostics()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- node.Run(ctx)
	}()

	var bootstrap diagnostics.Event
	select {
	case evt := <-diag:
		bootstrap = evt
	case <-time.After(time.Second):
		t.Fatalf("expected diagnostic event")
	}

	cancel()

	if err := <-runErr; err != nil {
		if status.Code(err) != codes.Canceled && !errors.Is(err, context.Canceled) {
			t.Fatalf("node run: %v", err)
		}
	}

	if bootstrap.Component != "child.bootstrap" {
		t.Fatalf("expected bootstrap diag, got %+v", bootstrap)
	}

	if handler.registerCount == 0 {
		t.Fatalf("expected register to be invoked")
	}
}

func TestHandleLeaseGrantExecutesWorkflow(t *testing.T) {
	if err := registry.Default().RegisterAction("child.exec", func(ctx rt.Context) error {
		return nil
	}, registry.ActionMetadata{}); err != nil {
		t.Fatalf("register action: %v", err)
	}

	n := NewNode(Config{})
	stub := &stubControlStream{}
	n.stream = stub

	def := workflow.Definition{
		Name: "RemoteTest",
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "child.exec"}},
		}},
	}
	defJSON, err := workflow.ToJSON(def)
	if err != nil {
		t.Fatalf("marshal definition: %v", err)
	}
	grant := &processproto.LeaseGrant{
		WorkflowId:     "wf-1",
		LeaseId:        "lease-1",
		Attempt:        1,
		DefinitionJson: defJSON,
	}

	if err := n.handleLeaseGrant(context.Background(), grant); err != nil {
		t.Fatalf("handle lease grant: %v", err)
	}

	var ack *processproto.LeaseAck
	deadline := time.After(2 * time.Second)
	for ack == nil {
		for _, env := range stub.Snapshot() {
			if env.GetLeaseAck() != nil {
				ack = env.GetLeaseAck()
			}
		}
		if ack != nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected lease ack, got %#v", stub.Snapshot())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !ack.GetSummary().GetSuccess() {
		t.Fatalf("expected success summary, got %+v", ack.GetSummary())
	}
}

func TestHandleLeaseGrantCancellation(t *testing.T) {
	if err := registry.Default().RegisterAction("child.wait", func(ctx rt.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			return nil
		}
	}, registry.ActionMetadata{}); err != nil {
		t.Fatalf("register action: %v", err)
	}

	n := NewNode(Config{})
	stub := &stubControlStream{}
	n.stream = stub

	def := workflow.Definition{
		Name: "RemoteCancel",
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "child.wait"}},
		}},
	}
	defJSON, err := workflow.ToJSON(def)
	if err != nil {
		t.Fatalf("marshal definition: %v", err)
	}
	grant := &processproto.LeaseGrant{
		WorkflowId:     "wf-cancel",
		LeaseId:        "lease-cancel",
		Attempt:        1,
		DefinitionJson: defJSON,
	}

	if err := n.handleLeaseGrant(context.Background(), grant); err != nil {
		t.Fatalf("handle lease grant: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	cancelMsg := &processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_Cancel{Cancel: &processproto.CancelWorkflow{WorkflowId: "wf-cancel"}}}
	if err := n.handleEnvelope(context.Background(), cancelMsg); err != nil {
		t.Fatalf("handle cancel: %v", err)
	}

	var ack *processproto.LeaseAck
	deadline := time.After(3 * time.Second)
	for ack == nil {
		for _, env := range stub.Snapshot() {
			if env.GetLeaseAck() != nil {
				ack = env.GetLeaseAck()
			}
		}
		if ack != nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected lease ack, got %#v", stub.Snapshot())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if ack.GetSummary().GetSuccess() {
		t.Fatalf("expected cancellation summary, got %+v", ack.GetSummary())
	}
	if ack.GetSummary().GetReason() != processproto.TerminationReason_TERMINATION_USER_CANCEL {
		t.Fatalf("expected user cancel reason, got %s", ack.GetSummary().GetReason())
	}
}
