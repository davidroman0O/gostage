package child

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/internal/locks"
	"github.com/davidroman0O/gostage/v3/node"
	processproto "github.com/davidroman0O/gostage/v3/process/proto"
	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/runner"
	"github.com/davidroman0O/gostage/v3/runtime/local"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

const logBufferSize = 256

// Node represents the child process runtime that communicates with the parent bridge.
type Node struct {
	cfg Config

	conn   *grpc.ClientConn
	stream processproto.ProcessBridge_ControlClient

	diagHub *diagnostics.Hub
	diagW   *diagWriter

	telemetry *node.TelemetryDispatcher

	logs      chan *processproto.LogForward
	logOnce   sync.Once
	logCancel context.CancelFunc
	logWG     sync.WaitGroup

	heartbeatInterval time.Duration

	sendMu  locks.Mutex
	closeMu sync.Mutex
	closed  bool

	cancel context.CancelFunc
	nodeID string

	runner   *runner.Runner
	activeMu locks.Mutex
	active   map[string]context.CancelFunc
	pending  map[string]struct{}
}

type childLogger struct {
	node *Node
	name string
}

func (l childLogger) Debug(format string, args ...interface{}) { l.log("debug", format, args...) }
func (l childLogger) Info(format string, args ...interface{})  { l.log("info", format, args...) }
func (l childLogger) Warn(format string, args ...interface{})  { l.log("warn", format, args...) }
func (l childLogger) Error(format string, args ...interface{}) { l.log("error", format, args...) }

func (l childLogger) log(level, format string, args ...interface{}) {
	if l.node == nil {
		return
	}

	attrs := extractAttributeMap(&args)
	message := format
	if format == "" && len(args) > 0 {
		message = fmt.Sprint(args...)
		args = nil
	} else if len(args) > 0 {
		message = fmt.Sprintf(format, args...)
	}
	if message == "" {
		return
	}
	l.node.emitLog(level, l.name, message, attrs)
}

// NewNode prepares a child node using the provided configuration. Call Run to
// establish the bridge connection.
func NewNode(cfg Config) *Node {
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}
	diagHub := diagnostics.NewHub()
	n := &Node{
		cfg:               cfg,
		diagHub:           diagHub,
		heartbeatInterval: cfg.HeartbeatInterval,
		nodeID:            uuid.NewString(),
		active:            make(map[string]context.CancelFunc),
		pending:           make(map[string]struct{}),
	}
	n.diagW = &diagWriter{node: n}
	n.telemetry = node.NewTelemetryDispatcher(context.Background(), n.diagW, node.TelemetryDispatcherConfig{})
	return n
}

func (n *Node) startLogLoop(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	n.logOnce.Do(func() {
		loopCtx, cancel := context.WithCancel(ctx)
		n.logCancel = cancel
		n.logs = make(chan *processproto.LogForward, logBufferSize)
		n.logWG.Add(1)
		go n.logLoop(loopCtx)
	})
}

func (n *Node) logLoop(ctx context.Context) {
	defer n.logWG.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case entry := <-n.logs:
			if entry == nil {
				continue
			}
			msg := &processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_Log{Log: entry}}
			if err := n.sendEnvelope(ctx, msg); err != nil {
				n.publishDiagnostic(diagnostics.Event{
					Component: "child.log",
					Severity:  diagnostics.SeverityWarning,
					Err:       fmt.Errorf("send log: %w", err),
					Metadata: map[string]any{
						"level":   entry.GetLevel(),
						"message": entry.GetMessage(),
					},
				})
			}
		}
	}
}

func (n *Node) enqueueLog(entry *processproto.LogForward) {
	if entry == nil || n.logs == nil {
		return
	}
	select {
	case n.logs <- entry:
	default:
		n.publishDiagnostic(diagnostics.Event{
			Component: "child.log",
			Severity:  diagnostics.SeverityWarning,
			Metadata: map[string]any{
				"dropped": true,
				"level":   entry.GetLevel(),
				"message": entry.GetMessage(),
				"logger":  entry.GetLogger(),
			},
		})
	}
}

// Run connects to the parent bridge and starts processing control messages until
// the context is cancelled or the stream terminates.
func (n *Node) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if n.isClosed() {
		return errors.New("child: node closed")
	}

	runCtx, cancel := context.WithCancel(ctx)
	n.cancel = cancel

	if err := n.connect(runCtx); err != nil {
		cancel()
		return err
	}
	defer n.cleanup()

	tickerStop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n.heartbeatLoop(runCtx, tickerStop)
	}()

	err := n.recvLoop(runCtx)
	close(tickerStop)
	wg.Wait()

	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// Diagnostics returns a channel that receives diagnostic events emitted by the child.
func (n *Node) Diagnostics() <-chan diagnostics.Event {
	return n.diagHub.Subscribe(64)
}

// StreamTelemetry attaches a telemetry handler that receives events emitted by the child runtime.
func (n *Node) StreamTelemetry(ctx context.Context, fn func(telemetry.Event)) func() {
	if ctx == nil {
		ctx = context.Background()
	}
	if fn == nil {
		return func() {}
	}
	return n.telemetry.Register(telemetry.SinkFunc(func(evt telemetry.Event) {
		select {
		case <-ctx.Done():
			return
		default:
			fn(evt)
		}
	}))
}

// Close terminates the connection to the parent and releases resources.
func (n *Node) Close() error {
	n.closeMu.Lock()
	if n.closed {
		n.closeMu.Unlock()
		return nil
	}
	n.closed = true
	n.closeMu.Unlock()

	if n.cancel != nil {
		n.cancel()
	}

	if n.logCancel != nil {
		n.logCancel()
		n.logWG.Wait()
		n.logs = nil
	}

	if n.stream != nil {
		_ = n.sendEnvelope(context.Background(), &processproto.ControlEnvelope{
			Body: &processproto.ControlEnvelope_Shutdown{Shutdown: &processproto.Shutdown{Reason: "child closing"}},
		})
	}

	if n.conn != nil {
		_ = n.conn.Close()
	}

	n.telemetry.Close()
	n.diagHub.Close()
	return nil
}

func (n *Node) isClosed() bool {
	n.closeMu.Lock()
	defer n.closeMu.Unlock()
	return n.closed
}

func (n *Node) connect(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, n.cfg.DialTimeout)
	defer cancel()

	dialOpts := []grpc.DialOption{grpc.WithBlock()}
	creds, err := n.credentials()
	if err != nil {
		return err
	}
	dialOpts = append(dialOpts, creds)

	conn, err := grpc.DialContext(dialCtx, n.cfg.Address, dialOpts...)
	if err != nil {
		return fmt.Errorf("child: dial parent: %w", err)
	}

	client := processproto.NewProcessBridgeClient(conn)
	stream, err := client.Control(ctx)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("child: open control stream: %w", err)
	}

	register := &processproto.RegisterNode{
		NodeId:    n.nodeID,
		ChildType: n.cfg.ChildType,
		AuthToken: n.cfg.AuthToken,
		Metadata:  n.cfg.Metadata,
		Pools:     make([]*processproto.ChildPool, 0, len(n.cfg.Pools)),
		Runtime: &processproto.RuntimeInfo{
			Version:   runtime.Version(),
			GoVersion: runtime.Version(),
			Os:        runtime.GOOS,
			Arch:      runtime.GOARCH,
			Binary:    runtimeExecutable(),
		},
	}
	for _, pool := range n.cfg.Pools {
		register.Pools = append(register.Pools, &processproto.ChildPool{
			Name:     pool.Name,
			Slots:    pool.Slots,
			Tags:     append([]string(nil), pool.Tags...),
			Metadata: copyStringMap(pool.Metadata),
		})
	}

	if err := stream.Send(&processproto.ControlEnvelope{
		MessageId: uuid.NewString(),
		Body:      &processproto.ControlEnvelope_Register{Register: register},
	}); err != nil {
		_ = stream.CloseSend()
		_ = conn.Close()
		return fmt.Errorf("child: send register: %w", err)
	}

	// expect register ack before other messages
	msg, err := stream.Recv()
	if err != nil {
		_ = stream.CloseSend()
		_ = conn.Close()
		return fmt.Errorf("child: await register ack: %w", err)
	}
	if ack := msg.GetRegisterAck(); ack != nil {
		if interval := ack.GetHeartbeatInterval(); interval != nil {
			if d := interval.AsDuration(); d > 0 {
				n.heartbeatInterval = d
			}
		}
		// consume ack, continue receiving remaining messages in normal loop
	} else {
		// unexpected first message, handle within loop asynchronously
		go func(m *processproto.ControlEnvelope) {
			_ = n.handleEnvelope(ctx, m)
		}(msg)
	}

	n.conn = conn
	n.stream = stream

	n.publishDiagnostic(diagnostics.Event{
		Component: "child.bootstrap",
		Severity:  diagnostics.SeverityInfo,
		Metadata: map[string]any{
			"address":    n.cfg.Address,
			"child_type": n.cfg.ChildType,
		},
	})

	n.startLogLoop(ctx)

	return nil
}

func (n *Node) recvLoop(ctx context.Context) error {
	for {
		msg, err := n.stream.Recv()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return context.Canceled
			}
			if !errors.Is(err, io.EOF) {
				n.publishDiagnostic(diagnostics.Event{
					Component: "child.recv",
					Severity:  diagnostics.SeverityError,
					Err:       err,
				})
			}
			return err
		}
		if err := n.handleEnvelope(ctx, msg); err != nil {
			return err
		}
	}
}

func (n *Node) ensureRuntime() error {
	if n.runner != nil {
		return nil
	}
	br := newChildBroker()
	n.runner = runner.New(local.Factory{}, registry.Default(), br, runner.WithDefaultLogger(childLogger{node: n, name: "runtime"}))
	return nil
}

func (n *Node) trackWorkflow(id string, cancel context.CancelFunc) {
	if id == "" || cancel == nil {
		return
	}
	n.activeMu.Lock()
	n.active[id] = cancel
	if _, pending := n.pending[id]; pending {
		delete(n.pending, id)
		go cancel()
	}
	n.activeMu.Unlock()
}

func (n *Node) releaseWorkflow(id string) {
	if id == "" {
		return
	}
	n.activeMu.Lock()
	delete(n.active, id)
	delete(n.pending, id)
	n.activeMu.Unlock()
}

func (n *Node) cancelWorkflow(id string) {
	if id == "" {
		return
	}
	n.activeMu.Lock()
	cancel := n.active[id]
	if cancel == nil {
		n.pending[id] = struct{}{}
	}
	n.activeMu.Unlock()
	if cancel != nil {
		cancel()
		n.publishDiagnostic(diagnostics.Event{
			Component: "child.cancel",
			Severity:  diagnostics.SeverityInfo,
			Metadata:  map[string]any{"workflow_id": id},
		})
	} else {
		n.publishDiagnostic(diagnostics.Event{
			Component: "child.cancel",
			Severity:  diagnostics.SeverityInfo,
			Metadata:  map[string]any{"workflow_id": id, "pending": true},
		})
	}
}

func (n *Node) handleEnvelope(ctx context.Context, msg *processproto.ControlEnvelope) error {
	switch body := msg.GetBody().(type) {
	case *processproto.ControlEnvelope_LeaseGrant:
		return n.handleLeaseGrant(ctx, body.LeaseGrant)
	case *processproto.ControlEnvelope_Cancel:
		n.cancelWorkflow(body.Cancel.GetWorkflowId())
		return nil
	case *processproto.ControlEnvelope_ConfigUpdate:
		n.handleConfigUpdate(body.ConfigUpdate)
		return nil
	case *processproto.ControlEnvelope_Shutdown:
		n.publishDiagnostic(diagnostics.Event{
			Component: "child.shutdown",
			Severity:  diagnostics.SeverityInfo,
			Metadata:  map[string]any{"reason": body.Shutdown.GetReason()},
		})
		return context.Canceled
	default:
		n.publishDiagnostic(diagnostics.Event{
			Component: "child.recv",
			Severity:  diagnostics.SeverityWarning,
			Metadata: map[string]any{
				"message": fmt.Sprintf("unexpected message %T", body),
			},
		})
		return nil
	}
}

func (n *Node) handleLeaseGrant(ctx context.Context, grant *processproto.LeaseGrant) error {
	if grant == nil {
		return nil
	}
	cloned := proto.Clone(grant)
	lease, ok := cloned.(*processproto.LeaseGrant)
	if !ok {
		return n.respondFailure(ctx, grant, fmt.Errorf("child: unexpected lease type %T", cloned))
	}
	go n.executeLease(ctx, lease)
	return nil
}

func (n *Node) executeLease(ctx context.Context, grant *processproto.LeaseGrant) {
	if grant == nil {
		return
	}
	if err := n.ensureRuntime(); err != nil {
		_ = n.respondFailure(ctx, grant, fmt.Errorf("child: init runtime: %w", err))
		return
	}

	n.startLogLoop(ctx)

	definition, err := workflow.FromJSON(grant.GetDefinitionJson())
	if err != nil {
		_ = n.respondFailure(ctx, grant, fmt.Errorf("child: decode definition: %w", err))
		return
	}
	definition.ID = grant.GetWorkflowId()

	wf, err := workflow.Materialize(definition, registry.Default())
	if err != nil {
		_ = n.respondFailure(ctx, grant, fmt.Errorf("child: materialize workflow: %w", err))
		return
	}

	var initialStore map[string]any
	if len(grant.GetStoreJson()) > 0 {
		if err := json.Unmarshal(grant.GetStoreJson(), &initialStore); err != nil {
			_ = n.respondFailure(ctx, grant, fmt.Errorf("child: decode initial store: %w", err))
			return
		}
	}

	execCtx, cancel := context.WithCancel(ctx)
	n.trackWorkflow(grant.GetWorkflowId(), cancel)
	defer n.releaseWorkflow(grant.GetWorkflowId())

	n.dispatchTelemetry(telemetry.Event{
		Kind:       telemetry.EventWorkflowStarted,
		WorkflowID: grant.GetWorkflowId(),
		Attempt:    int(grant.GetAttempt()),
		Timestamp:  time.Now(),
	})

	start := time.Now()
	result := n.runner.Run(wf, runner.RunOptions{
		Context:      execCtx,
		Logger:       childLogger{node: n, name: "runtime"},
		InitialStore: initialStore,
		Attempt:      int(grant.GetAttempt()),
	})

	finalState := runner.StatusCompleted
	reason := state.TerminationReasonSuccess
	if !result.Success {
		if errors.Is(result.Error, context.Canceled) || errors.Is(execCtx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			finalState = runner.StatusCancelled
			reason = state.TerminationReasonUserCancel
		} else {
			finalState = runner.StatusFailed
			reason = state.TerminationReasonFailure
		}
	}
	stageStatuses := make([]state.StageStatusRecord, 0, len(result.Stages))
	for _, stage := range result.Stages {
		stageStatuses = append(stageStatuses, state.StageStatusRecord{
			ID:          stage.ID,
			Name:        stage.Name,
			Description: stage.Description,
			Tags:        append([]string(nil), stage.Tags...),
			Dynamic:     stage.Dynamic,
			CreatedBy:   stage.CreatedBy,
			Status:      executionStatusToWorkflow(stage.Status),
		})
	}
	actionStatuses := make([]state.ActionStatusRecord, 0, len(result.Actions))
	for _, action := range result.Actions {
		actionStatuses = append(actionStatuses, state.ActionStatusRecord{
			StageID:     action.StageID,
			ActionID:    action.Name,
			Name:        action.Name,
			Description: action.Description,
			Tags:        append([]string(nil), action.Tags...),
			Dynamic:     action.Dynamic,
			CreatedBy:   action.CreatedBy,
			Status:      executionStatusToWorkflow(action.Status),
		})
	}

	summary := state.ResultSummary{
		Success:         result.Success,
		Error:           errorString(result.Error),
		Attempt:         int(grant.GetAttempt()),
		Output:          copyAnyMap(result.FinalStore),
		Duration:        durationOrNow(result.Duration, start),
		CompletedAt:     time.Now(),
		DisabledStages:  copyBoolMap(result.DisabledStages),
		DisabledActions: copyBoolMap(result.DisabledActions),
		RemovedStages:   copyStringMap(result.RemovedStages),
		RemovedActions:  copyStringMap(result.RemovedActions),
		Reason:          reason,
		StageStatuses:   stageStatuses,
		ActionStatuses:  actionStatuses,
	}

	n.dispatchTelemetry(telemetry.Event{
		Kind:       telemetry.EventWorkflowExecution,
		WorkflowID: grant.GetWorkflowId(),
		Attempt:    int(grant.GetAttempt()),
		Timestamp:  time.Now(),
		Error:      summary.Error,
		Metadata: map[string]any{
			"success": summary.Success,
		},
	})
	if finalState == runner.StatusCancelled {
		n.dispatchTelemetry(telemetry.Event{
			Kind:       telemetry.EventWorkflowCancelled,
			WorkflowID: grant.GetWorkflowId(),
			Attempt:    int(grant.GetAttempt()),
			Timestamp:  time.Now(),
			Metadata: map[string]any{
				"reason": reason,
			},
		})
	}

	stateValue := executionStatusToWorkflow(finalState)
	n.dispatchTelemetry(telemetry.Event{
		Kind:       telemetry.EventWorkflowSummary,
		WorkflowID: grant.GetWorkflowId(),
		Attempt:    int(grant.GetAttempt()),
		Timestamp:  time.Now(),
		Metadata: map[string]any{
			"success": summary.Success,
			"status":  stateValue,
			"reason":  string(summary.Reason),
		},
		Error: summary.Error,
	})

	protoSummary, err := encodeProtoSummary(summary)
	if err != nil {
		_ = n.respondFailure(ctx, grant, fmt.Errorf("child: encode summary: %w", err))
		return
	}

	env := &processproto.ControlEnvelope{
		Body: &processproto.ControlEnvelope_LeaseAck{LeaseAck: &processproto.LeaseAck{
			WorkflowId: grant.GetWorkflowId(),
			LeaseId:    grant.GetLeaseId(),
			Summary:    protoSummary,
		}},
	}
	if err := n.sendEnvelope(ctx, env); err != nil {
		n.publishDiagnostic(diagnostics.Event{
			Component: "child.lease",
			Severity:  diagnostics.SeverityError,
			Err:       fmt.Errorf("child: send lease ack: %w", err),
			Metadata:  map[string]any{"workflow_id": grant.GetWorkflowId()},
		})
	}
}

func (n *Node) handleConfigUpdate(update *processproto.ChildConfigUpdate) {
	if update == nil {
		return
	}
	if len(update.GetMetadataJson()) == 0 {
		return
	}
	var metadata map[string]string
	if err := json.Unmarshal(update.GetMetadataJson(), &metadata); err != nil {
		n.publishDiagnostic(diagnostics.Event{
			Component: "child.config",
			Severity:  diagnostics.SeverityError,
			Err:       fmt.Errorf("decode metadata: %w", err),
		})
		return
	}
	n.cfg.Metadata = metadata
	n.publishDiagnostic(diagnostics.Event{
		Component: "child.config",
		Severity:  diagnostics.SeverityInfo,
		Metadata:  map[string]any{"updated": metadata},
	})
}

func (n *Node) heartbeatLoop(ctx context.Context, stop <-chan struct{}) {
	interval := n.heartbeatInterval
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			hb := &processproto.Heartbeat{
				ObservedAt: timestampProtoNow(),
				NodeId:     n.nodeID,
			}
			env := &processproto.ControlEnvelope{Body: &processproto.ControlEnvelope_Heartbeat{Heartbeat: hb}}
			if err := n.sendEnvelope(ctx, env); err != nil {
				n.publishDiagnostic(diagnostics.Event{
					Component: "child.heartbeat",
					Severity:  diagnostics.SeverityError,
					Err:       err,
				})
				return
			}
		}
	}
}

func (n *Node) sendEnvelope(ctx context.Context, env *processproto.ControlEnvelope) error {
	if env == nil {
		return errors.New("nil envelope")
	}
	if env.MessageId == "" {
		env.MessageId = uuid.NewString()
	}
	n.sendMu.Lock()
	defer n.sendMu.Unlock()
	if n.stream == nil {
		return errors.New("stream not initialised")
	}
	return n.stream.Send(env)
}

func (n *Node) publishDiagnostic(evt diagnostics.Event) {
	if evt.OccurredAt.IsZero() {
		evt.OccurredAt = time.Now()
	}
	n.diagHub.Publish(evt)

	metadataJSON, _ := json.Marshal(evt.Metadata)
	msg := &processproto.ControlEnvelope{
		Body: &processproto.ControlEnvelope_Diagnostic{Diagnostic: &processproto.DiagnosticForward{
			OccurredAt:   timestampProto(evt.OccurredAt),
			Component:    evt.Component,
			Severity:     string(evt.Severity),
			Error:        errorString(evt.Err),
			MetadataJson: metadataJSON,
		}},
	}
	_ = n.sendEnvelope(context.Background(), msg)
}

func (n *Node) cleanup() {
	_ = n.Close()
}

func (n *Node) credentials() (grpc.DialOption, error) {
	if n.cfg.TLS.CertPath == "" && n.cfg.TLS.KeyPath == "" && n.cfg.TLS.CAPath == "" {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	}
	cert, err := tls.LoadX509KeyPair(n.cfg.TLS.CertPath, n.cfg.TLS.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("child: load client cert: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if n.cfg.TLS.CAPath != "" {
		pem, err := os.ReadFile(n.cfg.TLS.CAPath)
		if err != nil {
			return nil, fmt.Errorf("child: read CA file: %w", err)
		}
		if !caCertPool.AppendCertsFromPEM(pem) {
			return nil, errors.New("child: failed to append CA cert")
		}
	}
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	})
	return grpc.WithTransportCredentials(creds), nil
}

func copyStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

type diagWriter struct {
	node *Node
}

func (w *diagWriter) Write(evt diagnostics.Event) {
	if w == nil || w.node == nil {
		return
	}
	w.node.publishDiagnostic(evt)
}

func (n *Node) dispatchTelemetry(evt telemetry.Event) {
	if err := n.telemetry.Dispatch(evt); err != nil {
		n.publishDiagnostic(diagnostics.Event{
			Component: "child.telemetry",
			Severity:  diagnostics.SeverityWarning,
			Err:       err,
		})
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		n.publishDiagnostic(diagnostics.Event{
			Component: "child.telemetry",
			Severity:  diagnostics.SeverityError,
			Err:       fmt.Errorf("marshal telemetry: %w", err),
		})
		return
	}
	msg := &processproto.ControlEnvelope{
		Body: &processproto.ControlEnvelope_Telemetry{Telemetry: &processproto.TelemetryForward{EventJson: payload}},
	}
	_ = n.sendEnvelope(context.Background(), msg)
}

func (n *Node) respondFailure(ctx context.Context, grant *processproto.LeaseGrant, cause error) error {
	if grant == nil {
		return cause
	}
	n.publishDiagnostic(diagnostics.Event{
		Component: "child.lease",
		Severity:  diagnostics.SeverityError,
		Err:       cause,
		Metadata:  map[string]any{"workflow_id": grant.GetWorkflowId()},
	})
	summary := &processproto.ResultSummary{
		Success: false,
		Error:   errorString(cause),
		Attempt: grant.GetAttempt(),
		Reason:  processproto.TerminationReason_TERMINATION_FAILURE,
	}
	env := &processproto.ControlEnvelope{
		Body: &processproto.ControlEnvelope_LeaseAck{LeaseAck: &processproto.LeaseAck{
			WorkflowId: grant.GetWorkflowId(),
			LeaseId:    grant.GetLeaseId(),
			Summary:    summary,
		}},
	}
	if err := n.sendEnvelope(ctx, env); err != nil {
		return err
	}
	return nil
}

func (n *Node) emitLog(level, loggerName, msg string, attrs map[string]string) {
	if n == nil || msg == "" {
		return
	}
	entry := &processproto.LogForward{
		OccurredAt:  timestampProtoNow(),
		Level:       strings.ToLower(level),
		Logger:      loggerName,
		Message:     msg,
		Attributes:  attrs,
		ChildNodeId: n.nodeID,
	}
	n.enqueueLog(entry)
}

func extractAttributeMap(args *[]interface{}) map[string]string {
	if args == nil || len(*args) == 0 {
		return nil
	}
	last := (*args)[len(*args)-1]
	switch v := last.(type) {
	case map[string]string:
		*args = (*args)[:len(*args)-1]
		return copyStringMap(v)
	case map[string]any:
		attrs := make(map[string]string, len(v))
		for k, val := range v {
			attrs[k] = fmt.Sprint(val)
		}
		*args = (*args)[:len(*args)-1]
		return attrs
	default:
		return nil
	}
}

func encodeProtoSummary(sum state.ResultSummary) (*processproto.ResultSummary, error) {
	var outputJSON []byte
	var err error
	if len(sum.Output) > 0 {
		outputJSON, err = json.Marshal(sum.Output)
		if err != nil {
			return nil, err
		}
	}
	proto := &processproto.ResultSummary{
		Success:         sum.Success,
		Error:           sum.Error,
		Attempt:         int32(sum.Attempt),
		OutputJson:      outputJSON,
		DisabledStages:  copyBoolMap(sum.DisabledStages),
		DisabledActions: copyBoolMap(sum.DisabledActions),
		RemovedStages:   copyStringMap(sum.RemovedStages),
		RemovedActions:  copyStringMap(sum.RemovedActions),
		Reason:          encodeTerminationReason(sum.Reason),
	}
	if sum.Duration > 0 {
		proto.Duration = durationpb.New(sum.Duration)
	}
	if !sum.CompletedAt.IsZero() {
		proto.CompletedAt = timestampProto(sum.CompletedAt)
	}
	if len(sum.StageStatuses) > 0 {
		proto.StageStatuses = make([]*processproto.StageStatus, 0, len(sum.StageStatuses))
		for _, st := range sum.StageStatuses {
			proto.StageStatuses = append(proto.StageStatuses, &processproto.StageStatus{
				StageId:     st.ID,
				Name:        st.Name,
				Description: st.Description,
				Tags:        append([]string(nil), st.Tags...),
				Dynamic:     st.Dynamic,
				CreatedBy:   st.CreatedBy,
				Status:      string(st.Status),
			})
		}
	}
	if len(sum.ActionStatuses) > 0 {
		proto.ActionStatuses = make([]*processproto.ActionStatus, 0, len(sum.ActionStatuses))
		for _, act := range sum.ActionStatuses {
			proto.ActionStatuses = append(proto.ActionStatuses, &processproto.ActionStatus{
				StageId:     act.StageID,
				ActionId:    act.ActionID,
				Name:        act.Name,
				Description: act.Description,
				Tags:        append([]string(nil), act.Tags...),
				Dynamic:     act.Dynamic,
				CreatedBy:   act.CreatedBy,
				Status:      string(act.Status),
			})
		}
	}
	return proto, nil
}

func encodeTerminationReason(reason state.TerminationReason) processproto.TerminationReason {
	switch reason {
	case state.TerminationReasonSuccess:
		return processproto.TerminationReason_TERMINATION_SUCCESS
	case state.TerminationReasonUserCancel:
		return processproto.TerminationReason_TERMINATION_USER_CANCEL
	case state.TerminationReasonPolicyCancel:
		return processproto.TerminationReason_TERMINATION_POLICY_CANCEL
	case state.TerminationReasonTimeout:
		return processproto.TerminationReason_TERMINATION_TIMEOUT
	case state.TerminationReasonFailure:
		return processproto.TerminationReason_TERMINATION_FAILURE
	default:
		return processproto.TerminationReason_TERMINATION_UNKNOWN
	}
}

func executionStatusToWorkflow(status runner.ExecutionStatus) state.WorkflowState {
	switch status {
	case runner.StatusPending:
		return state.WorkflowPending
	case runner.StatusRunning:
		return state.WorkflowRunning
	case runner.StatusCompleted:
		return state.WorkflowCompleted
	case runner.StatusFailed:
		return state.WorkflowFailed
	case runner.StatusSkipped:
		return state.WorkflowSkipped
	case runner.StatusRemoved:
		return state.WorkflowRemoved
	case runner.StatusCancelled:
		return state.WorkflowCancelled
	default:
		return state.WorkflowPending
	}
}

func durationOrNow(d time.Duration, start time.Time) time.Duration {
	if d > 0 {
		return d
	}
	if !start.IsZero() {
		return time.Since(start)
	}
	return 0
}

func copyBoolMap(src map[string]bool) map[string]bool {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyAnyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func runtimeExecutable() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	return exe
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func timestampProtoNow() *timestamppb.Timestamp {
	return timestamppb.New(time.Now())
}

func timestampProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
