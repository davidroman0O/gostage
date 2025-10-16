package telemetry

import "time"

// EventKind enumerates known telemetry event identifiers.
type EventKind string

const (
	EventWorkflowRegistered    EventKind = "workflow.registered"
	EventWorkflowClaimed       EventKind = "workflow.claimed"
	EventWorkflowStarted       EventKind = "workflow.started"
	EventWorkflowExecution     EventKind = "workflow.execution"
	EventWorkflowRetry         EventKind = "workflow.retry"
	EventWorkflowCancelRequest EventKind = "workflow.cancel_requested"
	EventWorkflowCancelled     EventKind = "workflow.cancelled"
	EventWorkflowSummary       EventKind = "workflow.summary"
	EventStageRegistered       EventKind = "stage.registered"
	EventStageRemoved          EventKind = "stage.removed"
	EventActionRegistered      EventKind = "action.registered"
	EventActionRemoved         EventKind = "action.removed"
	EventActionProgressKind    EventKind = "action.progress"
)

// Progress describes an action progress update.
type Progress struct {
	Percent int
	Message string
}

// Event represents a workflow lifecycle event emitted by the engine.
type Event struct {
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

// Sink consumes telemetry events.
type Sink interface {
	Record(Event)
}

type SinkFunc func(Event)

func (f SinkFunc) Record(evt Event) {
	f(evt)
}
