package telemetry

import "time"

// Event represents a workflow lifecycle event emitted by the engine.
type Event struct {
	Kind       string
	WorkflowID string
	StageID    string
	ActionID   string
	Attempt    int
	Timestamp  time.Time
	Message    string
	Metadata   map[string]any
	Err        error
}

// Sink consumes telemetry events.
type Sink interface {
	Record(Event)
}

type SinkFunc func(Event)

func (f SinkFunc) Record(evt Event) {
	f(evt)
}
