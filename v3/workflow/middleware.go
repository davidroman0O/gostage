package workflow

import (
	"context"

	"github.com/davidroman0O/gostage/v3/internal/store"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// ActionFunc is the executable unit produced by the registry.
type ActionFunc func(RuntimeContext) error

// WorkflowMiddleware wraps action execution across the entire workflow.
type WorkflowMiddleware func(ActionFunc) ActionFunc

// StageMiddleware wraps action execution for a specific stage.
type StageMiddleware func(ActionFunc) ActionFunc

// ActionMiddleware wraps an individual action.
type ActionMiddleware func(ActionFunc) ActionFunc

// RuntimeContext is the contract exposed to workflow actions.
type RuntimeContext interface {
	context.Context
	Workflow() Definition
	Stage() StageRuntime
	Action() ActionRuntime
	Store() *store.KVStore
	Logger() telemetry.Logger
	Metadata() map[string]any
	Attempt() int
	Stages() StageMutation
	Actions() ActionMutation
	Broker() Broker
}

// StageRuntime exposes the currently executing stage metadata.
type StageRuntime interface {
	ID() string
	Name() string
	Tags() []string
}

// ActionRuntime exposes the currently executing action metadata.
type ActionRuntime interface {
	ID() string
	Ref() string
	Tags() []string
}

// Broker is implemented by the runtime to forward progress events.
type Broker interface {
	Progress(stageID, actionID string, percent int, message string)
	Event(kind string, metadata map[string]any)
}
