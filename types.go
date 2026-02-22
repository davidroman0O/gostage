package gostage

import "time"

// P is a convenience alias for map[string]any.
// It's used for passing parameters, initial store data, and results.
//
//	engine.RunSync(ctx, wf, gostage.P{"order_id": "ORD-123"})
type P = map[string]any

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

// SleepError is returned internally when a persistent sleep step executes.
type SleepError struct {
	WakeAt time.Time
}

func (e *SleepError) Error() string {
	return "sleeping until " + e.WakeAt.Format(time.RFC3339)
}
