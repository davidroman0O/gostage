package runtime

import (
	"context"
	"time"

	store "github.com/davidroman0O/gostage/v3/store"
)

// StageMutation defines runtime mutation operations available at the stage level.
type StageMutation interface {
	// Add inserts a new stage into the workflow after the current stage and
	// returns the identifier associated with the inserted stage. If the stage
	// did not previously have an identifier, a generated value is returned.
	Add(stage Stage) string
	Remove(id string) bool
	RemoveByTags(tags []string) int
	Disable(id string)
	DisableByTags(tags []string) int
	Enable(id string)
	EnableByTags(tags []string) int
	IsEnabled(id string) bool
}

// ActionMutation defines runtime mutation operations available at the action level.
type ActionMutation interface {
	// Add inserts a new action after the current action and returns the
	// identifier associated with the inserted action. If the action did not
	// have an identifier, a generated value is returned.
	Add(action Action) string
	Remove(id string) bool
	RemoveByTags(tags []string) int
	Disable(id string)
	DisableByTags(tags []string) int
	Enable(id string)
	EnableByTags(tags []string) int
	IsEnabled(id string) bool
}

// DisableSnapshotProvider exposes initial disabled stage/action state for a workflow.
type DisableSnapshotProvider interface {
	DisabledSnapshot() (map[string]bool, map[string]bool)
}

// Broker represents the side channel exposed to actions for emitting progress or custom telemetry.
type Broker interface {
	Progress(percent int, message string) error
	Event(kind, message string, metadata map[string]any) error
}

// Context is the per-action execution context surfaced to runtime actions.
type Context interface {
	Deadline() (deadline time.Time, ok bool)
	Done() <-chan struct{}
	Err() error
	Value(key any) any

	Stages() StageMutation
	Actions() ActionMutation

	Workflow() Workflow
	Stage() Stage
	Action() Action
	ActionIndex() int
	IsLastAction() bool

	Store() store.Handle
	Logger() Logger
	Broker() Broker
}

// ActionRunnerFunc is the execution signature for an action.
type ActionRunnerFunc func(ctx Context, action Action, index int, isLast bool) error

// ActionMiddleware wraps an ActionRunnerFunc.
type ActionMiddleware func(next ActionRunnerFunc) ActionRunnerFunc

// Action is the runtime contract for executable workflow actions.
type Action interface {
	Name() string
	Description() string
	Tags() []string
	Execute(ctx Context) error
}

// StageRunnerFunc is the execution signature for a stage.
type StageRunnerFunc func(ctx context.Context, stage Stage, workflow Workflow, logger Logger) error

// StageMiddleware wraps stage execution.
type StageMiddleware func(next StageRunnerFunc) StageRunnerFunc

// Stage represents a workflow stage at runtime.
type Stage interface {
	ID() string
	Name() string
	Description() string
	Actions() ActionMutation
	ActionList() []Action
	Tags() []string
	InitialStore() store.Handle
	Middlewares() []StageMiddleware
	ActionMiddlewares() []ActionMiddleware
}

// WorkflowStageRunnerFunc is the execution signature for workflow processing.
type WorkflowStageRunnerFunc func(ctx context.Context, stage Stage, workflow Workflow, logger Logger) error

// WorkflowMiddleware wraps workflow execution.
type WorkflowMiddleware func(next WorkflowStageRunnerFunc) WorkflowStageRunnerFunc

// Workflow is the runtime workflow contract executed by the runner.
type Workflow interface {
	ID() string
	Name() string
	Description() string
	Tags() []string
	InitialStore() store.Handle
	Store() store.Handle
	Stages() []Stage
	Metadata() map[string]interface{}
	Middlewares() []WorkflowMiddleware
}

// RuntimeStageRecorder captures runtime-only mutations for stages.
type RuntimeStageRecorder interface {
	RecordDynamicAction(action Action, createdBy string)
	RecordActionDisabled(actionID, createdBy string)
	RecordActionEnabled(actionID, createdBy string)
	RecordActionRemoved(actionID, createdBy string)
}

// RuntimeWorkflowRecorder captures runtime-only mutations for workflows.
type RuntimeWorkflowRecorder interface {
	RecordDynamicStage(stage Stage, createdBy string)
	RecordStageDisabled(stageID, createdBy string)
	RecordStageEnabled(stageID, createdBy string)
	RecordStageRemoved(stageID, createdBy string)
}

// TypedWorkflow exposes optional metadata used for observability.
type TypedWorkflow interface {
	WorkflowType() string
	WorkflowPayload() map[string]interface{}
}

// Logger is the structured logging contract used during workflow execution.
type Logger interface {
	Debug(format string, args ...interface{})
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
}
