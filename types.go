package gostage

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
	// StatusPendingRun means the run is queued but not yet started.
	StatusPendingRun Status = "pending"
	// StatusRunningRun means the run is currently executing.
	StatusRunningRun Status = "running"
	// StatusCompletedRun means all steps finished successfully.
	StatusCompletedRun Status = "completed"
	// StatusFailedRun means a step failed after retries.
	StatusFailedRun Status = "failed"
	// StatusSuspended means the run is waiting for external input.
	StatusSuspended Status = "suspended"
	// StatusSleeping means the run is waiting for a time condition.
	StatusSleeping Status = "sleeping"
	// StatusBailed means the run exited early (not an error).
	StatusBailed Status = "bailed"
	// StatusCancelled means engine.Cancel() was called.
	StatusCancelled Status = "cancelled"
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
