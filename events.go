package gostage

import "time"

// EventType identifies the kind of engine event.
type EventType int

const (
	// EventRunCreated fires after a new run record is saved to persistence.
	EventRunCreated EventType = iota + 1
	// EventRunCompleted fires when a run finishes successfully.
	EventRunCompleted
	// EventRunFailed fires when a run fails after retries.
	EventRunFailed
	// EventRunBailed fires when a run exits early via Bail.
	EventRunBailed
	// EventRunSuspended fires when a run suspends waiting for external input.
	EventRunSuspended
	// EventRunSleeping fires when a run enters a timed sleep.
	EventRunSleeping
	// EventRunCancelled fires when a run is cancelled via Cancel or context.
	EventRunCancelled
	// EventRunDeleted fires after a run and its state are removed from persistence.
	EventRunDeleted
	// EventRunWoken fires when a sleeping run's timer fires and execution resumes.
	EventRunWoken
)

// EngineEvent carries information about an engine lifecycle event.
type EngineEvent struct {
	Type       EventType
	RunID      RunID
	WorkflowID string
	Status     Status
	Timestamp  time.Time
}

// EventHandler receives engine lifecycle events.
// Implementations must be safe for concurrent use — events may fire
// from multiple goroutines simultaneously.
type EventHandler interface {
	OnEvent(event EngineEvent)
}

// emitEvent dispatches an event to all registered handlers.
// Panics in handlers are recovered and logged.
func (e *Engine) emitEvent(event EngineEvent) {
	for _, h := range e.eventHandlers {
		func() {
			defer func() {
				if r := recover(); r != nil {
					e.logger.Error("event handler panicked for event type %d: %v", event.Type, r)
				}
			}()
			h.OnEvent(event)
		}()
	}
}
