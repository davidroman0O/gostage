package scheduler

import (
	"context"
	"time"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/runner"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// MetadataInitialStore is the queue metadata key that carries the initial store payload.
const MetadataInitialStore = "gostage.initial_store"

// Queue is the minimal scheduling queue contract required by the dispatcher.
type Queue interface {
	Claim(ctx context.Context, selector state.Selector, pool string) (*state.ClaimedWorkflow, error)
	Release(ctx context.Context, id state.WorkflowID) error
	Ack(ctx context.Context, id state.WorkflowID, summary state.ResultSummary) error
	Cancel(ctx context.Context, id state.WorkflowID) error
	PendingCount(ctx context.Context, selector state.Selector) (int, error)
	Stats(ctx context.Context) (state.QueueStats, error)
}

// SummaryStore persists workflow summaries for callers that rely on Wait.
type SummaryStore interface {
	StoreSummary(ctx context.Context, id state.WorkflowID, summary state.ResultSummary) error
}

// ExecutionManager persists execution updates surfaced by the dispatcher.
type ExecutionManager = state.Writer

// WorkflowRunner runs workflow definitions and returns execution results.
type WorkflowRunner interface {
	Run(workflow rt.Workflow, options runner.RunOptions) runner.RunResult
}

// TelemetryDispatcher publishes telemetry events.
type TelemetryDispatcher interface {
	Dispatch(telemetry.Event) error
	Coverage(workflowID string) map[telemetry.EventKind]int
	ClearCoverage(workflowID string)
}

// DiagnosticsWriter records diagnostics events.
type DiagnosticsWriter interface {
	Write(diagnostics.Event)
}

// HealthPublisher publishes health events for pools.
type HealthPublisher interface {
	Publish(node.HealthEvent)
}

// Binding associates a local pool with an optional remote executor.
type Binding struct {
	Pool   *pools.Local
	Remote RemoteExecutor
}

// RemoteExecutor forwards claimed workflows to a remote coordinator.
type RemoteExecutor interface {
	Ready() bool
	Dispatch(claimed *state.ClaimedWorkflow, release func()) error
}

// Options configure dispatcher behaviour.
type Options struct {
	ClaimInterval time.Duration
	Jitter        time.Duration
	MaxInFlight   int
	FailurePolicy bootstrap.FailurePolicy
	Clock         func() time.Time
}
