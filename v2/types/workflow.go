package types

import (
	"context"
	"time"

	"github.com/davidroman0O/gostage/store"
)

// Runtime mutation intended for stages and actions
type mutation[T any] interface {
	// Adds a new entity to be inserted after the current one.
	// This allows for dynamic workflow modification during execution.
	// The entity will be executed immediately after the current entity completes.
	Add(entity T)

	// Remove removes an entity from its stage by id
	Remove(entityID string) bool

	// RemoveByTags removes all entities with the specified tags
	RemoveByTags(tags []string) int

	// Enable enables an entity by ID.
	// Enabled stages will be executed during workflow execution.
	Enable(entityID string)

	// EnableByTags enables all entities with the specified tags
	EnableByTags(tags []string) int

	// Disable disables an entity by ID.
	// Disabled stages will be skipped during workflow execution.
	Disable(entityID string)

	// DisableByTags disables all entities with the specified tags
	DisableByTags(tags []string) int

	// IsEnabled checks if an entity is enabled.
	// Returns true if the stage is enabled or not found in the disabled stages map.
	IsEnabled(entityID string) bool
}

type StageActionMutation mutation[Stage]

type ActionMutation mutation[Action]

type BrokerCall interface {
	// Low-level message calling
	Call(msgType MessageType, payload []byte) error

	Publish(key string, value []byte, metadata map[string]interface{}) error

	// Relative to the current action
	Progress(percent int) error

	ProgressCause(message string, percent int) error

	Log(level LogLevel, message string, fields ...LogField) error
}

// Context interface for Action
//
// Context interface using those functions: `Deadline`, `Done`, `Err`, `Value`
type Context interface {
	Deadline() (deadline time.Time, ok bool)
	Done() <-chan struct{}
	Err() error
	Value(key any) any

	// Mutate the workflow at the stage level
	Stages() StageActionMutation

	// Mutate the workflow at the action level
	Actions() ActionMutation

	// Execution state helpers
	Workflow() Workflow
	Stage() Stage
	Action() Action
	ActionIndex() int
	IsLastAction() bool

	// Shared resources
	Store() *store.KVStore
	Logger() Logger

	// Communication
	Broker() BrokerCall
}

// ActionRunnerFunc is the core function type for executing an action.
type ActionRunnerFunc func(ctx Context, action Action, index int, isLast bool) error

// ActionMiddleware represents a function that wraps action execution.
// It allows performing operations before and after an action executes,
// with information about the action's position in the execution sequence.
type ActionMiddleware func(next ActionRunnerFunc) ActionRunnerFunc

// Action is a single unit of work within a stage.
// Actions are the building blocks of workflows and represent individual tasks
// that need to be executed. Actions can be organized using tags and can be
// dynamically enabled or disabled at runtime.
type Action interface {
	// Name returns the action's name
	Name() string

	// Description returns a human-readable description of the action
	Description() string

	// Tags returns the action's tags for organization and filtering
	Tags() []string

	// Execute performs the action's work.
	// The ActionContext provides access to the workflow environment,
	// including the store for state management and the logger for output.
	Execute(ctx Context) error
}

// ActionState tracks whether an action is enabled.
// This is used to represent the runtime state of actions within a workflow.
type ActionState struct {
	// Action is a reference to the action
	Action Action
	// Enabled indicates whether the action is enabled and will be executed
	Enabled bool
}

type ActionInfo struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

// StageRunnerFunc is the core function type for executing a stage.
// It follows the same pattern as RunnerFunc for workflow execution.
type StageRunnerFunc func(ctx context.Context, stage Stage, workflow Workflow, logger Logger) error

// StageMiddleware represents a function that wraps stage execution.
// It allows performing operations before and after a stage executes.
type StageMiddleware func(next StageRunnerFunc) StageRunnerFunc

// Stage is a logical phase within a workflow that contains a sequence of actions.
// Stages provide organization and grouping of related actions and can be
// dynamically enabled, disabled, or generated during workflow execution.
type Stage interface {
	// ID is the unique identifier for the stage
	ID() string

	// Name is a human-readable name for the stage
	Name() string

	// Description provides details about the stage's purpose
	Description() string

	// Mutate the workflow at the action level
	Actions() ActionMutation

	// ActionList returns the actions attached to the stage in execution order
	ActionList() []Action

	// Tags for organization and filtering
	Tags() []string

	// initialStore contains key-value data available at the start of stage execution
	InitialStore() *store.KVStore

	// middleware contains the middleware functions to apply during stage execution
	Middlewares() []StageMiddleware
}

// StageInfo holds serializable stage information for persistence and transmission.
// This is used when storing stage data in the workflow's key-value store.
type StageInfo struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	ActionIDs   []string `json:"actionIds"`
}

// StageState tracks whether a stage is enabled.
// This is used to represent the runtime state of stages within a workflow.
type StageState struct {
	// Stage is a reference to the stage
	Stage *Stage
	// Enabled indicates whether the stage is enabled and will be executed
	Enabled bool
}

// WorkflowStageRunnerFunc is the core function type for executing a stage within a workflow.
type WorkflowStageRunnerFunc func(ctx context.Context, stage Stage, workflow Workflow, logger Logger) error

// WorkflowMiddleware represents a function that wraps stage execution within a workflow.
// It allows performing operations before and after each stage executes.
type WorkflowMiddleware func(next WorkflowStageRunnerFunc) WorkflowStageRunnerFunc

// Workflow is a sequence of stages forming a complete process.
// It provides the top-level coordination for executing a series of stages
// and maintaining their shared state and context.
// Workflows can be dynamically modified during execution, allowing for
// flexible and adaptable processes.
type Workflow interface {
	// ID is the unique identifier for the workflow
	ID() string

	// Name is a human-readable name for the workflow
	Name() string

	// Description provides details about the workflow's purpose
	Description() string

	// Tags for organization and filtering
	Tags() []string

	// Store is the central key-value store for workflow data
	// It stores workflow metadata, stage information, and execution data
	// That's the initial store, once the workflow is running you have zero guarantee to have the latest values
	InitialStore() *store.KVStore

	// Store returns the runtime key-value store backing this workflow execution
	Store() *store.KVStore

	// Stages contains all the workflow's stages in execution order
	// This provides direct access during execution
	Stages() []Stage

	// Stores arbitrary data for use during workflow execution
	// Implementation-specific tools and state can be stored here
	Metadata() map[string]interface{}

	// middleware contains workflow-level middleware that wraps stage execution
	Middlewares() []WorkflowMiddleware
}

type WorkflowInfo struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	StageIDs    []string `json:"stageIds"`
}
