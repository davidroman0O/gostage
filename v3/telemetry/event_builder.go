package telemetry

import (
	"time"

	"github.com/davidroman0O/gostage/v3/internal/clock"
)

// EventBuilder provides a fluent API for constructing telemetry events with consistent structure.
// This ensures all events follow the same schema and include required fields.
type EventBuilder struct {
	event Event
}

// NewEvent creates a new event builder with the given kind.
// The event will have DefaultEventSchema version and current timestamp.
func NewEvent(kind EventKind) *EventBuilder {
	return &EventBuilder{
		event: Event{
			SchemaVersion: DefaultEventSchema,
			Kind:          kind,
			Timestamp:     clock.DefaultClock().Now(),
			Metadata:      make(map[string]any),
		},
	}
}

// WithWorkflowID sets the workflow ID for the event.
func (b *EventBuilder) WithWorkflowID(id string) *EventBuilder {
	b.event.WorkflowID = id
	return b
}

// WithStageID sets the stage ID for the event.
func (b *EventBuilder) WithStageID(id string) *EventBuilder {
	b.event.StageID = id
	return b
}

// WithActionID sets the action ID for the event.
func (b *EventBuilder) WithActionID(id string) *EventBuilder {
	b.event.ActionID = id
	return b
}

// WithAttempt sets the attempt number for the event.
func (b *EventBuilder) WithAttempt(attempt int) *EventBuilder {
	b.event.Attempt = attempt
	return b
}

// WithMessage sets the message for the event.
func (b *EventBuilder) WithMessage(msg string) *EventBuilder {
	b.event.Message = msg
	return b
}

// WithError sets the error message for the event.
func (b *EventBuilder) WithError(err error) *EventBuilder {
	if err != nil {
		b.event.Error = err.Error()
	}
	return b
}

// WithErrorString sets the error message from a string.
func (b *EventBuilder) WithErrorString(errMsg string) *EventBuilder {
	b.event.Error = errMsg
	return b
}

// WithProgress sets the progress information for the event.
func (b *EventBuilder) WithProgress(percent int, message string) *EventBuilder {
	b.event.Progress = &Progress{
		Percent: percent,
		Message: message,
	}
	return b
}

// WithMetadata sets metadata for the event. This replaces any existing metadata.
func (b *EventBuilder) WithMetadata(metadata map[string]any) *EventBuilder {
	if metadata != nil {
		b.event.Metadata = make(map[string]any, len(metadata))
		for k, v := range metadata {
			b.event.Metadata[k] = v
		}
	}
	return b
}

// AddMetadata adds a single metadata key-value pair to the event.
func (b *EventBuilder) AddMetadata(key string, value any) *EventBuilder {
	if b.event.Metadata == nil {
		b.event.Metadata = make(map[string]any)
	}
	b.event.Metadata[key] = value
	return b
}

// WithTimestamp sets a custom timestamp for the event.
func (b *EventBuilder) WithTimestamp(ts time.Time) *EventBuilder {
	b.event.Timestamp = ts
	return b
}

// Build returns the constructed event.
func (b *EventBuilder) Build() Event {
	// Ensure metadata is never nil
	if b.event.Metadata == nil {
		b.event.Metadata = make(map[string]any)
	}
	// Ensure timestamp is set
	if b.event.Timestamp.IsZero() {
		b.event.Timestamp = clock.DefaultClock().Now()
	}
	// Ensure schema version is set
	if b.event.SchemaVersion == 0 {
		b.event.SchemaVersion = DefaultEventSchema
	}
	return b.event
}

// Standard metadata keys used across events for consistency.
const (
	// MetadataKeyPool is the metadata key for pool name.
	MetadataKeyPool = "pool"
	// MetadataKeyReason is the metadata key for termination reason.
	MetadataKeyReason = "reason"
	// MetadataKeySuccess is the metadata key for success status.
	MetadataKeySuccess = "success"
	// MetadataKeyDuration is the metadata key for execution duration.
	MetadataKeyDuration = "duration"
	// MetadataKeyCreatedBy is the metadata key for who created a dynamic component.
	MetadataKeyCreatedBy = "created_by"
	// MetadataKeyRemovedBy is the metadata key for who removed a component.
	MetadataKeyRemovedBy = "removed_by"
)

// Helper functions for common event patterns

// NewWorkflowEvent creates a workflow-level event with standard fields.
func NewWorkflowEvent(kind EventKind, workflowID string, attempt int) *EventBuilder {
	return NewEvent(kind).
		WithWorkflowID(workflowID).
		WithAttempt(attempt)
}

// NewStageEvent creates a stage-level event with standard fields.
func NewStageEvent(kind EventKind, workflowID, stageID string) *EventBuilder {
	return NewEvent(kind).
		WithWorkflowID(workflowID).
		WithStageID(stageID)
}

// NewActionEvent creates an action-level event with standard fields.
func NewActionEvent(kind EventKind, workflowID, stageID, actionID string) *EventBuilder {
	return NewEvent(kind).
		WithWorkflowID(workflowID).
		WithStageID(stageID).
		WithActionID(actionID)
}

// NormalizeEvent ensures an event has all required fields set with defaults.
// This is useful when receiving events from external sources or legacy code.
func NormalizeEvent(evt Event) Event {
	if evt.SchemaVersion == 0 {
		evt.SchemaVersion = DefaultEventSchema
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = clock.DefaultClock().Now()
	}
	if evt.Metadata == nil {
		evt.Metadata = make(map[string]any)
	}
	return evt
}
