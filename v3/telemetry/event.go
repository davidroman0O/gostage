package telemetry

import "time"

// EventKind enumerates known telemetry event identifiers.
type EventKind string

const (
	// EventWorkflowRegistered indicates a workflow was registered.
	EventWorkflowRegistered EventKind = "workflow.registered"
	// EventWorkflowClaimed indicates a workflow was claimed for execution.
	EventWorkflowClaimed EventKind = "workflow.claimed"
	// EventWorkflowStarted indicates a workflow execution has started.
	EventWorkflowStarted EventKind = "workflow.started"
	// EventWorkflowCompleted indicates a workflow execution completed successfully.
	EventWorkflowCompleted EventKind = "workflow.completed"
	// EventWorkflowFailed indicates a workflow execution failed.
	EventWorkflowFailed EventKind = "workflow.failed"
	// EventWorkflowSkipped indicates a workflow execution was skipped.
	EventWorkflowSkipped EventKind = "workflow.skipped"
	// EventWorkflowCancelled indicates a workflow execution was cancelled.
	EventWorkflowCancelled EventKind = "workflow.cancelled"
	// EventWorkflowRemoved indicates a workflow execution was removed.
	EventWorkflowRemoved EventKind = "workflow.removed"
	// EventWorkflowExecution indicates a workflow execution event.
	EventWorkflowExecution EventKind = "workflow.execution"
	// EventWorkflowRetry indicates a workflow retry event.
	EventWorkflowRetry EventKind = "workflow.retry"
	// EventWorkflowCancelRequest indicates a workflow cancel request event.
	EventWorkflowCancelRequest EventKind = "workflow.cancel_requested"
	// EventWorkflowSummary indicates a workflow execution summary event.
	EventWorkflowSummary EventKind = "workflow.summary"

	// EventStageRegistered indicates a stage registration event.
	EventStageRegistered EventKind = "stage.registered"
	// EventStageStarted indicates a stage execution started event.
	EventStageStarted EventKind = "stage.started"
	// EventStageCompleted indicates a stage execution completed event.
	EventStageCompleted EventKind = "stage.completed"
	// EventStageFailed indicates a stage execution failed event.
	EventStageFailed EventKind = "stage.failed"
	// EventStageCancelled indicates a stage execution cancelled event.
	EventStageCancelled EventKind = "stage.cancelled"
	// EventStageSkipped indicates a stage execution skipped event.
	EventStageSkipped EventKind = "stage.skipped"
	// EventStageRemoved indicates a stage execution removed event.
	EventStageRemoved EventKind = "stage.removed"

	// EventActionRegistered indicates an action registration event.
	EventActionRegistered EventKind = "action.registered"
	// EventActionStarted indicates an action execution started event.
	EventActionStarted EventKind = "action.started"
	// EventActionCompleted indicates an action execution completed event.
	EventActionCompleted EventKind = "action.completed"
	// EventActionFailed indicates an action execution failed event.
	EventActionFailed EventKind = "action.failed"
	// EventActionCancelled indicates an action execution cancelled event.
	EventActionCancelled EventKind = "action.cancelled"
	// EventActionSkipped indicates an action execution skipped event.
	EventActionSkipped EventKind = "action.skipped"
	// EventActionRemoved indicates an action execution removed event.
	EventActionRemoved EventKind = "action.removed"
	// EventActionProgress indicates an action progress event.
	EventActionProgress EventKind = "action.progress"
	// EventRemoteDiagnostic indicates a remote child diagnostic line or structured log forwarded to telemetry.
	EventRemoteDiagnostic EventKind = "remote.diagnostic"
	// EventRetentionCycle indicates a retention maintenance cycle (SQLite only).
	EventRetentionCycle EventKind = "retention.cycle"

	// EventDebugWorkflowState indicates a debug event for workflow state (emitted only when debug mode is enabled).
	EventDebugWorkflowState EventKind = "debug.workflow.state"
	// EventDebugPoolSelection indicates a debug event for pool selection (emitted only when debug mode is enabled).
	EventDebugPoolSelection EventKind = "debug.pool.selection"
	// EventDebugSelectorMatch indicates a debug event for selector matching (emitted only when debug mode is enabled).
	EventDebugSelectorMatch EventKind = "debug.selector.match"
)

// Progress describes an action progress update.
type Progress struct {
	Percent int
	Message string
}

// Event represents a workflow lifecycle event emitted by the engine.
type Event struct {
	// SchemaVersion allows sinks to evolve decoding logic while maintaining
	// backward compatibility. Zero means the default event schema version.
	SchemaVersion int
	Kind          EventKind
	WorkflowID    string
	StageID       string
	ActionID      string
	Attempt       int
	Timestamp     time.Time
	Message       string
	Progress      *Progress
	Error         string
	Metadata      map[string]any
}

// DefaultEventSchema is the current event schema version used by emitters.
const DefaultEventSchema = 1

// SinkFunc provides a function-based implementation of Sink.
type SinkFunc func(Event)

// Record implements the Sink interface by calling the function.
func (f SinkFunc) Record(evt Event) {
	f(evt)
}
