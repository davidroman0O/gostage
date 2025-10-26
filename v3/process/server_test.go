package process

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	processproto "github.com/davidroman0O/gostage/v3/process/proto"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

type mockHandler struct {
	registerCh chan *processproto.RegisterNode
	ack        *processproto.RegisterAck
	leaseAckCh chan state.ResultSummary
}

func newMockHandler() *mockHandler {
	return &mockHandler{
		registerCh: make(chan *processproto.RegisterNode, 1),
		leaseAckCh: make(chan state.ResultSummary, 1),
		ack: &processproto.RegisterAck{
			HeartbeatInterval: durationpb.New(100 * time.Millisecond),
		},
	}
}

func (m *mockHandler) OnRegister(ctx context.Context, conn *Connection, req *processproto.RegisterNode) (*processproto.RegisterAck, error) {
	select {
	case m.registerCh <- req:
	default:
	}
	go func() {
		// Close the connection shortly after registration to end the test run.
		time.Sleep(100 * time.Millisecond)
		conn.Close("test shutdown")
	}()
	return m.ack, nil
}

func (m *mockHandler) OnLeaseAck(ctx context.Context, conn *Connection, workflowID string, leaseID string, summary state.ResultSummary) error {
	select {
	case m.leaseAckCh <- summary:
	default:
	}
	return nil
}

func (m *mockHandler) OnTelemetry(context.Context, *Connection, telemetry.Event) error { return nil }
func (m *mockHandler) OnDiagnostic(context.Context, *Connection, diagnostics.Event)    {}
func (m *mockHandler) OnHeartbeat(context.Context, *Connection, time.Time) error       { return nil }
func (m *mockHandler) OnShutdown(context.Context, *Connection, string)                 {}

func TestServerControlRegistersChild(t *testing.T) {
	handler := newMockHandler()
	srv := grpc.NewServer()
	processproto.RegisterProcessBridgeServer(srv, NewServer(handler))

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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := processproto.NewProcessBridgeClient(conn)
	stream, err := client.Control(ctx)
	if err != nil {
		t.Fatalf("control stream: %v", err)
	}

	register := &processproto.ControlEnvelope{
		Body: &processproto.ControlEnvelope_Register{Register: &processproto.RegisterNode{
			NodeId:    "node-1",
			ChildType: "default",
		}},
	}
	if err := stream.Send(register); err != nil {
		t.Fatalf("send register: %v", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv ack: %v", err)
	}
	if msg.GetRegisterAck() == nil {
		t.Fatalf("expected register ack, got %T", msg.GetBody())
	}

	select {
	case <-handler.registerCh:
	case <-ctx.Done():
		t.Fatalf("register not observed")
	}

	if err := stream.Send(&processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_LeaseAck{LeaseAck: &processproto.LeaseAck{
		WorkflowId: "wf-1",
		LeaseId:    "lease-1",
		Summary:    &processproto.ResultSummary{Success: true},
	}}}); err != nil {
		t.Fatalf("send lease ack: %v", err)
	}

	select {
	case <-handler.leaseAckCh:
	case <-ctx.Done():
		t.Fatalf("lease ack not observed")
	}
}
