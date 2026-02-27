package gostage

import (
	"errors"
	"time"
)

// HandlerID is an opaque identifier returned by OnMessage.
// Pass it to OffMessage to deregister the handler.
type HandlerID int64

// Params is a convenience alias for map[string]any.
// It's used for passing parameters, initial store data, and results.
//
//	engine.RunSync(ctx, wf, gostage.Params{"order_id": "ORD-123"})
type Params = map[string]any

// RunID uniquely identifies a workflow execution run.
type RunID string

// Status represents the current state of a workflow run.
type Status string

const (
	// Pending means the run is queued but not yet started.
	Pending Status = "pending"
	// Running means the run is currently executing.
	Running Status = "running"
	// Completed means all steps finished successfully.
	Completed Status = "completed"
	// Failed means a step failed after retries.
	Failed Status = "failed"
	// Suspended means the run is waiting for external input.
	Suspended Status = "suspended"
	// Sleeping means the run is waiting for a time condition.
	Sleeping Status = "sleeping"
	// Bailed means the run exited early (not an error).
	Bailed Status = "bailed"
	// Cancelled means engine.Cancel() was called.
	Cancelled Status = "cancelled"
)

// Result holds the outcome of a workflow run.
type Result struct {
	// RunID is the unique identifier for this execution.
	RunID RunID
	// Status is the final status of the run.
	Status Status
	// Error holds the error if the run failed.
	Error error
	// BailReason holds the reason if the run bailed.
	BailReason string
	// Store contains the final store state after execution.
	Store map[string]any
	// SuspendData holds data when Status == Suspended.
	SuspendData map[string]any
}

// BailError is returned when a task calls Bail().
type BailError struct {
	Reason string
}

func (e *BailError) Error() string {
	return "bail: " + e.Reason
}

// SuspendError is returned when a task calls Suspend().
type SuspendError struct {
	Data map[string]any
}

func (e *SuspendError) Error() string {
	return "suspended"
}

// sleepError is returned internally when a persistent sleep step executes.
// This type is internal to the sleep step and not available to general task functions.
type sleepError struct {
	wakeAt time.Time
}

func (e *sleepError) Error() string {
	return "sleeping until " + e.wakeAt.Format(time.RFC3339)
}

// ResultGet extracts a typed value from a Result's store map.
// Returns the value and true if the key exists and can be coerced to T.
// Returns the zero value and false if the key is missing or the type is incompatible.
func ResultGet[T any](r *Result, key string) (T, bool) {
	var zero T
	if r == nil || r.Store == nil {
		return zero, false
	}
	val, ok := r.Store[key]
	if !ok {
		return zero, false
	}
	return coerce[T](val)
}

// Sentinel errors for programmatic error checking via errors.Is.
var (
	ErrEngineClosed     = errors.New("engine is closed")
	ErrRunNotFound      = errors.New("run not found")
	ErrRunNotSuspended  = errors.New("run is not in suspended status")
	ErrWorkflowMismatch = errors.New("workflow ID does not match run")
	ErrRunAlreadyActive = errors.New("run is already active")
	ErrNilWorkflow      = errors.New("workflow is nil")
	ErrEmptyName        = errors.New("name must not be empty")
	ErrStateLimitExceeded = errors.New("state entry limit exceeded")
)
