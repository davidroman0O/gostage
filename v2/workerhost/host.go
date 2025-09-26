package workerhost

import (
	"context"
	"errors"
	"time"

	"github.com/davidroman0O/gostage/v2/runner"
	"github.com/davidroman0O/gostage/v2/types"
)

var errNotImplemented = errors.New("workerhost: not implemented")

// Host exposes the public API for managing runner slots.
type Host interface {
	Acquire(ctx context.Context) (Lease, error)
	Submit(job Job) error
	Events() <-chan Event
	Stats() Stats
	Scale(delta int) error
	Stop(ctx context.Context) error
}

// Lease represents a reserved runner slot managed by the host.
type Lease interface {
	ID() string
	Start(job Job) error
	Release() error
}

// Job encapsulates workflow execution metadata for the host.
type Job struct {
	ID           string
	Workflow     types.Workflow
	LeaseID      string
	InitialStore map[string]interface{}
	Context      context.Context
	Metadata     map[string]interface{}
}

// Stats summarises current slot utilisation.
type Stats struct {
	TotalSlots int
	BusySlots  int
	IdleSlots  int
	Inflight   int
}

// EventType enumerates lifecycle events emitted by the host.
type EventType int

const (
	EventAssigned EventType = iota
	EventStarted
	EventCompleted
	EventFailed
	EventCancelled
	EventSlotAdded
	EventSlotRemoved
)

// Event captures lifecycle changes for slots and jobs.
type Event struct {
	Type      EventType
	SlotID    string
	JobID     string
	LeaseID   string
	Result    runner.RunResult
	Err       error
	Metadata  map[string]interface{}
	Timestamp time.Time
}

// New creates a WorkerHost based on the provided configuration.
func New(cfg Config) (Host, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return newHost(cfg)
}
