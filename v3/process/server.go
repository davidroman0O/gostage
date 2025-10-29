package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	processproto "github.com/davidroman0O/gostage/v3/process/proto"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// Handler defines callbacks invoked by the parent bridge when processing
// messages from a child connection. Implementations should be concurrency safe.
type Handler interface {
	OnRegister(ctx context.Context, conn *Connection, req *processproto.RegisterNode) (*processproto.RegisterAck, error)
	OnLeaseAck(ctx context.Context, conn *Connection, workflowID string, leaseID string, summary state.ResultSummary) error
	OnTelemetry(ctx context.Context, conn *Connection, evt telemetry.Event) error
	OnDiagnostic(ctx context.Context, conn *Connection, evt diagnostics.Event)
	OnLog(ctx context.Context, conn *Connection, entry LogEntry) error
	OnHeartbeat(ctx context.Context, conn *Connection, ts time.Time) error
	OnShutdown(ctx context.Context, conn *Connection, reason string)
}

// LogEntry represents a structured log emitted by a child process.
type LogEntry struct {
	OccurredAt  time.Time
	Level       string
	Logger      string
	Message     string
	Attributes  map[string]string
	Pool        string
	ChildNodeID string
}

// Server hosts the gRPC bridge used by child processes to communicate with the
// parent node.
type Server struct {
	processproto.UnimplementedProcessBridgeServer

	handler Handler
}

// NewServer constructs a new bridge server.
func NewServer(handler Handler) *Server {
	if handler == nil {
		panic("process: handler must not be nil")
	}
	return &Server{handler: handler}
}

// Connection represents a registered child connection.
type Connection struct {
	info    ConnectionInfo
	sendCh  chan *processproto.ControlEnvelope
	close   sync.Once
	closed  chan struct{}
	onClose func(reason string)
}

// ConnectionInfo provides metadata about the child.
type ConnectionInfo struct {
	NodeID    string
	ChildType string
	Metadata  map[string]string
}

// Info returns immutable connection metadata.
func (c *Connection) Info() ConnectionInfo { return c.info }

// SendLeaseGrant sends a workflow lease to the child.
func (c *Connection) SendLeaseGrant(ctx context.Context, grant *processproto.LeaseGrant) error {
	if grant == nil {
		return errors.New("grant must not be nil")
	}
	return c.enqueue(ctx, &processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_LeaseGrant{LeaseGrant: grant}})
}

// SendCancel requests the child to cancel a workflow.
func (c *Connection) SendCancel(ctx context.Context, cancel *processproto.CancelWorkflow) error {
	if cancel == nil {
		return errors.New("cancel must not be nil")
	}
	return c.enqueue(ctx, &processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_Cancel{Cancel: cancel}})
}

// SendConfig pushes a configuration update to the child.
func (c *Connection) SendConfig(ctx context.Context, cfg *processproto.ChildConfigUpdate) error {
	if cfg == nil {
		return errors.New("cfg must not be nil")
	}
	return c.enqueue(ctx, &processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_ConfigUpdate{ConfigUpdate: cfg}})
}

// Close marks the connection as closed and triggers the shutdown callback.
func (c *Connection) Close(reason string) {
	c.close.Do(func() {
		close(c.closed)
		close(c.sendCh)
		if c.onClose != nil {
			c.onClose(reason)
		}
	})
}

func (c *Connection) enqueue(ctx context.Context, msg *processproto.ControlEnvelope) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return errors.New("connection closed")
	default:
	}
	if msg.MessageId == "" {
		msg.MessageId = uuid.NewString()
	}
	select {
	case c.sendCh <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return errors.New("connection closed")
	}
}

func newConnection(info ConnectionInfo) *Connection {
	return &Connection{
		info:   info,
		sendCh: make(chan *processproto.ControlEnvelope, 64),
		closed: make(chan struct{}),
	}
}

// Control handles the bidirectional parent/child stream.
func (s *Server) Control(stream processproto.ProcessBridge_ControlServer) error {
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "expected register message")
		}
		return err
	}
	register := first.GetRegister()
	if register == nil {
		return status.Error(codes.InvalidArgument, "expected register message")
	}
	if register.GetChildType() == "" {
		return status.Error(codes.InvalidArgument, "child_type required")
	}
	if register.GetNodeId() == "" {
		return status.Error(codes.InvalidArgument, "node_id required")
	}

	conn := newConnection(ConnectionInfo{
		NodeID:    register.GetNodeId(),
		ChildType: register.GetChildType(),
		Metadata:  cloneMetadata(register.GetMetadata()),
	})

	sendErr := make(chan error, 1)
	go runSendLoop(stream, conn, sendErr)

	conn.onClose = func(reason string) {
		s.handler.OnShutdown(ctx, conn, reason)
	}

	ack, err := s.handler.OnRegister(ctx, conn, register)
	if err != nil {
		conn.Close(err.Error())
		return err
	}
	if ack != nil {
		_ = conn.enqueue(ctx, &processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_RegisterAck{RegisterAck: ack}})
	}

	for {
		select {
		case err := <-sendErr:
			if err != nil {
				conn.Close(err.Error())
				return err
			}
			conn.Close("send loop closed")
			return nil
		default:
		}

		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				conn.Close("remote closed stream")
				return nil
			}
			conn.Close(err.Error())
			return err
		}
		switch body := msg.Body.(type) {
	case *processproto.ControlEnvelope_LeaseAck:
		summary, convErr := convertSummary(body.LeaseAck.GetSummary())
		if convErr != nil {
			conn.Close(convErr.Error())
			return convErr
		}
		if body.LeaseAck.GetWorkflowId() == "" || body.LeaseAck.GetLeaseId() == "" {
			err := status.Error(codes.InvalidArgument, "lease ack missing identifiers")
			conn.Close(err.Error())
			return err
		}
		if err := s.handler.OnLeaseAck(ctx, conn, body.LeaseAck.GetWorkflowId(), body.LeaseAck.GetLeaseId(), summary); err != nil {
			conn.Close(err.Error())
			return err
		}
		case *processproto.ControlEnvelope_Telemetry:
			evt, convErr := decodeTelemetry(body.Telemetry.GetEventJson())
			if convErr != nil {
				conn.Close(convErr.Error())
				return convErr
			}
			if err := s.handler.OnTelemetry(ctx, conn, evt); err != nil {
				conn.Close(err.Error())
				return err
			}
		case *processproto.ControlEnvelope_Diagnostic:
			diag, convErr := decodeDiagnostic(body.Diagnostic)
			if convErr != nil {
				conn.Close(convErr.Error())
				return convErr
			}
			s.handler.OnDiagnostic(ctx, conn, diag)
		case *processproto.ControlEnvelope_Log:
			entry, convErr := decodeLogEntry(body.Log, conn.info.NodeID)
			if convErr != nil {
				conn.Close(convErr.Error())
				return convErr
			}
			if err := s.handler.OnLog(ctx, conn, entry); err != nil {
				conn.Close(err.Error())
				return err
			}
		case *processproto.ControlEnvelope_Heartbeat:
			hb := body.Heartbeat
			ts := time.Now()
			if hb != nil && hb.GetObservedAt() != nil {
				ts = time.Unix(hb.GetObservedAt().GetSeconds(), int64(hb.GetObservedAt().GetNanos()))
			}
			if err := s.handler.OnHeartbeat(ctx, conn, ts); err != nil {
				conn.Close(err.Error())
				return err
			}
		case *processproto.ControlEnvelope_Shutdown:
			reason := body.Shutdown.GetReason()
			conn.Close(reason)
			return nil
		case *processproto.ControlEnvelope_Register:
			conn.Close("duplicate register message")
			return status.Error(codes.InvalidArgument, "duplicate register message")
		default:
			conn.Close("unsupported message")
			return status.Errorf(codes.InvalidArgument, "unsupported message: %T", body)
		}
	}
}

func runSendLoop(stream processproto.ProcessBridge_ControlServer, conn *Connection, errCh chan<- error) {
	for msg := range conn.sendCh {
		if msg.MessageId == "" {
			msg.MessageId = uuid.NewString()
		}
		if err := stream.Send(msg); err != nil {
			errCh <- err
			return
		}
	}
	errCh <- nil
}

func convertSummary(in *processproto.ResultSummary) (state.ResultSummary, error) {
	if in == nil {
		return state.ResultSummary{}, errors.New("nil result summary")
	}
	res := state.ResultSummary{
		Success:         in.GetSuccess(),
		Error:           in.GetError(),
		Attempt:         int(in.GetAttempt()),
		DisabledStages:  cloneBoolMap(in.GetDisabledStages()),
		DisabledActions: cloneBoolMap(in.GetDisabledActions()),
		RemovedStages:   cloneStringMap(in.GetRemovedStages()),
		RemovedActions:  cloneStringMap(in.GetRemovedActions()),
		Reason:          convertReason(in.GetReason()),
	}
	if dur := in.GetDuration(); dur != nil {
		res.Duration = dur.AsDuration()
	}
	if ts := in.GetCompletedAt(); ts != nil {
		res.CompletedAt = time.Unix(ts.GetSeconds(), int64(ts.GetNanos()))
	}
	if len(in.GetOutputJson()) > 0 {
		if err := json.Unmarshal(in.GetOutputJson(), &res.Output); err != nil {
			return state.ResultSummary{}, fmt.Errorf("decode summary output: %w", err)
		}
	} else {
		res.Output = make(map[string]any)
	}
	if stageStatuses := in.GetStageStatuses(); len(stageStatuses) > 0 {
		res.StageStatuses = make([]state.StageStatusRecord, 0, len(stageStatuses))
		for _, st := range stageStatuses {
			if st == nil {
				continue
			}
			rec := state.StageStatusRecord{
				ID:          st.GetStageId(),
				Name:        st.GetName(),
				Description: st.GetDescription(),
				Tags:        append([]string(nil), st.GetTags()...),
				Dynamic:     st.GetDynamic(),
				CreatedBy:   st.GetCreatedBy(),
				Status:      state.WorkflowState(st.GetStatus()),
			}
			res.StageStatuses = append(res.StageStatuses, rec)
		}
	}
	if actionStatuses := in.GetActionStatuses(); len(actionStatuses) > 0 {
		res.ActionStatuses = make([]state.ActionStatusRecord, 0, len(actionStatuses))
		for _, act := range actionStatuses {
			if act == nil {
				continue
			}
			rec := state.ActionStatusRecord{
				StageID:     act.GetStageId(),
				ActionID:    act.GetActionId(),
				Name:        act.GetName(),
				Description: act.GetDescription(),
				Tags:        append([]string(nil), act.GetTags()...),
				Dynamic:     act.GetDynamic(),
				CreatedBy:   act.GetCreatedBy(),
				Status:      state.WorkflowState(act.GetStatus()),
			}
			res.ActionStatuses = append(res.ActionStatuses, rec)
		}
	}
	return res, nil
}

func convertReason(r processproto.TerminationReason) state.TerminationReason {
	switch r {
	case processproto.TerminationReason_TERMINATION_SUCCESS:
		return state.TerminationReasonSuccess
	case processproto.TerminationReason_TERMINATION_FAILURE:
		return state.TerminationReasonFailure
	case processproto.TerminationReason_TERMINATION_USER_CANCEL:
		return state.TerminationReasonUserCancel
	case processproto.TerminationReason_TERMINATION_POLICY_CANCEL:
		return state.TerminationReasonPolicyCancel
	case processproto.TerminationReason_TERMINATION_TIMEOUT:
		return state.TerminationReasonTimeout
	default:
		return state.TerminationReasonUnknown
	}
}

func decodeTelemetry(raw []byte) (telemetry.Event, error) {
	if len(raw) == 0 {
		return telemetry.Event{}, errors.New("empty telemetry payload")
	}
	var evt telemetry.Event
	if err := json.Unmarshal(raw, &evt); err != nil {
		return telemetry.Event{}, fmt.Errorf("decode telemetry: %w", err)
	}
	return evt, nil
}

func decodeDiagnostic(msg *processproto.DiagnosticForward) (diagnostics.Event, error) {
	if msg == nil {
		return diagnostics.Event{}, errors.New("nil diagnostic message")
	}
	var metadata map[string]any
	if len(msg.GetMetadataJson()) > 0 {
		if err := json.Unmarshal(msg.GetMetadataJson(), &metadata); err != nil {
			return diagnostics.Event{}, fmt.Errorf("decode diagnostic metadata: %w", err)
		}
	}
	occurred := time.Now()
	if msg.GetOccurredAt() != nil {
		occurred = time.Unix(msg.GetOccurredAt().GetSeconds(), int64(msg.GetOccurredAt().GetNanos()))
	}
	evt := diagnostics.Event{
		OccurredAt: occurred,
		Component:  msg.GetComponent(),
		Severity:   diagnostics.Severity(msg.GetSeverity()),
		Metadata:   metadata,
	}
	if msg.GetError() != "" {
		evt.Err = errors.New(msg.GetError())
	}
	return evt, nil
}

func decodeLogEntry(msg *processproto.LogForward, fallbackNodeID string) (LogEntry, error) {
	if msg == nil {
		return LogEntry{}, errors.New("nil log message")
	}
	occurred := time.Now()
	if msg.GetOccurredAt() != nil {
		occurred = time.Unix(msg.GetOccurredAt().GetSeconds(), int64(msg.GetOccurredAt().GetNanos()))
	}
	entry := LogEntry{
		OccurredAt:  occurred,
		Level:       msg.GetLevel(),
		Logger:      msg.GetLogger(),
		Message:     msg.GetMessage(),
		Attributes:  cloneStringMap(msg.GetAttributes()),
		Pool:        msg.GetPool(),
		ChildNodeID: msg.GetChildNodeId(),
	}
	if entry.ChildNodeID == "" {
		entry.ChildNodeID = fallbackNodeID
	}
	return entry, nil
}

func cloneMetadata(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneBoolMap(src map[string]bool) map[string]bool {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
