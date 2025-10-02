package coordinator

import (
	"context"
	"time"

	"github.com/davidroman0O/gostage/v2/state"
	"github.com/davidroman0O/gostage/v2/types"
	"github.com/davidroman0O/gostage/v2/workerhost"
)

// FailureAction describes how the coordinator should treat a failed workflow run.
type FailureAction int

const (
	// FailureActionComplete marks the workflow as completed with an error.
	FailureActionComplete FailureAction = iota
	// FailureActionRelease returns the workflow to the queue for retry.
	FailureActionRelease
	// FailureActionCancel marks the workflow as cancelled.
	FailureActionCancel
)

// FailureInfo provides context to a FailurePolicy callback.
type FailureInfo struct {
	WorkflowID string
	HostID     HostID
	LeaseID    string
	Result     workerhost.Event
}

// FailurePolicy decides how to react to a failure event.
type FailurePolicy func(info FailureInfo) FailureAction

// Config holds dependencies and knobs for a coordinator instance.
type WorkflowFactory func(def state.SubWorkflowDef) (types.Workflow, error)

type HostID string

type HostSelector func(hosts []HostSnapshot) HostID

type Config struct {
	Manager         state.Manager
	Logger          types.Logger
	WorkerID        string
	WorkerType      string
	Filter          state.WorkflowFilter
	ClaimInterval   time.Duration
	MaxInFlight     int
	FailurePolicy   FailurePolicy
	WorkflowFactory WorkflowFactory
	Selector        HostSelector
}

// Snapshot captures coordinator metrics for observability.
type Snapshot struct {
	Claimed   int
	Started   int
	Completed int
	Failed    int
	Cancelled int
	Released  int
	InFlight  int
}

type HostSnapshot struct {
	ID        HostID
	Stats     workerhost.Stats
	InFlight  int
	Healthy   bool
	LastError error
}

// EventType enumerates coordinator-level lifecycle events.
type EventType int

const (
	EventClaimed EventType = iota
	EventStarted
	EventSucceeded
	EventFailed
	EventReleased
	EventCancelled
)

// Event exposes coordinator lifecycle information to observers.
type Event struct {
	Type      EventType
	Workflow  string
	HostID    HostID
	LeaseID   string
	Timestamp time.Time
	Err       error
}

// Coordinator defines the public API for the control loop that feeds the worker host.
type Coordinator interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	RegisterHost(id HostID, host workerhost.Host) error
	UnregisterHost(id HostID) error
	ScaleHost(id HostID, delta int) error
	Stats() Snapshot
	HostStats() map[HostID]HostSnapshot
	Events() <-chan Event
}
