package node

import (
	"context"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/internal/locks"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// Node represents a running orchestrator instance.
type Node struct {
	ctx        context.Context
	cancel     context.CancelFunc
	telemetry  *TelemetryDispatcher
	diagHub    *DiagnosticsHub
	diagStream <-chan diagnostics.Event
	closeOnce  locks.Once
}

// New creates a Node with pre-wired telemetry and diagnostics plumbing.
func New(ctx context.Context, sinks []telemetry.Sink, cfg TelemetryDispatcherConfig) *Node {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	diagHub := NewDiagnosticsHub()
	teleDispatcher := NewTelemetryDispatcher(ctx, diagHub, cfg)
	for _, sink := range sinks {
		_ = teleDispatcher.Register(sink)
	}
	diagCh := diagHub.Subscribe()
	return &Node{
		ctx:        ctx,
		cancel:     cancel,
		telemetry:  teleDispatcher,
		diagHub:    diagHub,
		diagStream: diagCh,
	}
}

// Diagnostics returns the diagnostics stream for this node.
func (n *Node) Diagnostics() <-chan diagnostics.Event {
	return n.diagStream
}

// TelemetryDispatcher exposes the dispatcher for internal wiring.
func (n *Node) TelemetryDispatcher() *TelemetryDispatcher {
	return n.telemetry
}

// DiagnosticsWriter exposes the diagnostics hub for internal components.
func (n *Node) DiagnosticsWriter() DiagnosticsWriter {
	return n.diagHub
}

// Close terminates the node telemetry/diagnostic infrastructure.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.cancel()
		if n.telemetry != nil {
			n.telemetry.Close()
		}
		if n.diagHub != nil {
			n.diagHub.Close()
		}
	})
	return nil
}
