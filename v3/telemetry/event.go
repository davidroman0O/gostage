package telemetry

import "time"

// EventKind enumerates known telemetry event identifiers.
type EventKind string

const (
	EventWorkflowRegistered    EventKind = "workflow.registered"
	EventWorkflowClaimed       EventKind = "workflow.claimed"
	EventWorkflowStarted       EventKind = "workflow.started"
	EventWorkflowCompleted     EventKind = "workflow.completed"
	EventWorkflowFailed        EventKind = "workflow.failed"
	EventWorkflowSkipped       EventKind = "workflow.skipped"
	EventWorkflowCancelled     EventKind = "workflow.cancelled"
	EventWorkflowRemoved       EventKind = "workflow.removed"
	EventWorkflowExecution     EventKind = "workflow.execution"
	EventWorkflowRetry         EventKind = "workflow.retry"
	EventWorkflowCancelRequest EventKind = "workflow.cancel_requested"
	EventWorkflowSummary       EventKind = "workflow.summary"

	EventStageRegistered EventKind = "stage.registered"
	EventStageStarted    EventKind = "stage.started"
	EventStageCompleted  EventKind = "stage.completed"
	EventStageFailed     EventKind = "stage.failed"
	EventStageCancelled  EventKind = "stage.cancelled"
	EventStageSkipped    EventKind = "stage.skipped"
	EventStageRemoved    EventKind = "stage.removed"

	EventActionRegistered   EventKind = "action.registered"
	EventActionStarted      EventKind = "action.started"
	EventActionCompleted    EventKind = "action.completed"
	EventActionFailed       EventKind = "action.failed"
	EventActionCancelled    EventKind = "action.cancelled"
	EventActionSkipped      EventKind = "action.skipped"
	EventActionRemoved      EventKind = "action.removed"
	EventActionProgress     EventKind = "action.progress"
	// Remote child diagnostic line or structured log forwarded to telemetry.
	EventRemoteDiagnostic   EventKind = "remote.diagnostic"
	// Retention maintenance cycle (SQLite only).
	EventRetentionCycle     EventKind = "retention.cycle"
	
	// Debug event kinds (emitted only when debug mode is enabled):
	EventDebugWorkflowState EventKind = "debug.workflow.state"
	EventDebugPoolSelection EventKind = "debug.pool.selection"
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
    Kind       EventKind
    WorkflowID string
    StageID    string
    ActionID   string
    Attempt    int
    Timestamp  time.Time
    Message    string
    Progress   *Progress
    Error      string
    Metadata   map[string]any
}

// DefaultEventSchema is the current event schema version used by emitters.
const DefaultEventSchema = 1

// Sink is defined in interfaces.go.
// SinkFunc provides a function-based implementation.
type SinkFunc func(Event)

func (f SinkFunc) Record(evt Event) {
	f(evt)
}
