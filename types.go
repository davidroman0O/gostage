package gostage

import (
	"context"
	"encoding/json"
	"time"
)

// MessageType is a string that defines the purpose of a message.
type MessageType string

const (
	// MessageTypeLog is for sending log messages between processes.
	MessageTypeLog MessageType = "log"
	// MessageTypeStorePut is for synchronizing a single store.Put operation.
	MessageTypeStorePut MessageType = "store_put"
	// MessageTypeStoreDelete is for synchronizing a single store.Delete operation.
	MessageTypeStoreDelete MessageType = "store_delete"
	// MessageTypeWorkflowStart is the initial message from parent to child to start execution.
	MessageTypeWorkflowStart MessageType = "workflow_start"
	// MessageTypeWorkflowResult is the final message from child to parent with the outcome.
	MessageTypeWorkflowResult MessageType = "workflow_result"
	// MessageTypeFinalStore is sent from child to parent with the complete final store state.
	MessageTypeFinalStore MessageType = "final_store"
)

// Message is the standard unit of communication between a parent and child process.
// This is kept for backwards compatibility with existing JSON-based communication.
type Message struct {
	Type    MessageType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// MessageHandler is a function that processes a received message.
type MessageHandler func(msgType MessageType, payload json.RawMessage) error

// ContextMessageHandler processes messages with full context metadata
type ContextMessageHandler func(msgType MessageType, payload json.RawMessage, context MessageContext) error

// MessageContext provides comprehensive information about message source
type MessageContext struct {
	WorkflowID     string
	StageID        string
	ActionName     string
	ProcessID      int32
	IsChildProcess bool
	ActionIndex    int32
	IsLastAction   bool
	SessionID      string
	SequenceNumber int64
}

// HandlerRegistration defines how a handler should be registered
type HandlerRegistration struct {
	MessageType MessageType
	Handler     MessageHandler
	WorkflowID  string // Empty string means global handler
	StageID     string // Empty string means any stage
	ActionName  string // Empty string means any action
}

// ActionFactory is a function that creates a new instance of an Action.
// It's used by the registry to instantiate actions from their IDs.
type ActionFactory func() Action

// ActionRunnerFunc is the core function type for executing an action.
type ActionRunnerFunc func(ctx *ActionContext, action Action, index int, isLast bool) error

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
	Execute(ctx *ActionContext) error
}

// ActionState tracks whether an action is enabled.
// This is used to represent the runtime state of actions within a workflow.
type ActionState struct {
	// Action is a reference to the action
	Action Action
	// Enabled indicates whether the action is enabled and will be executed
	Enabled bool
}

// StageState tracks whether a stage is enabled.
// This is used to represent the runtime state of stages within a workflow.
type StageState struct {
	// Stage is a reference to the stage
	Stage *Stage
	// Enabled indicates whether the stage is enabled and will be executed
	Enabled bool
}

// Logger provides a simple interface for workflow logging
type Logger interface {
	// Debug logs a message at debug level
	Debug(format string, args ...interface{})

	// Info logs a message at info level
	Info(format string, args ...interface{})

	// Warn logs a message at warning level
	Warn(format string, args ...interface{})

	// Error logs a message at error level
	Error(format string, args ...interface{})
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

// StageRunnerFunc is the core function type for executing a stage.
// It follows the same pattern as RunnerFunc for workflow execution.
type StageRunnerFunc func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error

// StageMiddleware represents a function that wraps stage execution.
// It allows performing operations before and after a stage executes.
type StageMiddleware func(next StageRunnerFunc) StageRunnerFunc

// WorkflowStageRunnerFunc is the core function type for executing a stage within a workflow.
type WorkflowStageRunnerFunc func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error

// WorkflowMiddleware represents a function that wraps stage execution within a workflow.
// This allows for workflow-level operations that apply to all stages.
type WorkflowMiddleware func(next WorkflowStageRunnerFunc) WorkflowStageRunnerFunc

// WorkflowInfo holds serializable workflow information for persistence and transmission.
type WorkflowInfo struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	StageIDs    []string `json:"stageIds"`
	CreatedAt   string   `json:"createdAt"`
	UpdatedAt   string   `json:"updatedAt"`
}

// MergeStrategy defines how to handle conflicts when merging workflows.
type MergeStrategy int

// RunnerFunc is the core function type for executing a workflow.
type RunnerFunc func(ctx context.Context, workflow *Workflow, logger Logger) error

// Middleware represents a function that wraps workflow execution.
// Middleware can perform actions before and after workflow execution,
// inject data into the workflow store, modify the context, or even
// skip execution entirely.
type Middleware func(next RunnerFunc) RunnerFunc

// RunOptions contains options for workflow execution
type RunOptions struct {
	// Logger to use for the workflow execution
	Logger Logger

	// Context to use for the workflow execution
	Context context.Context

	// Whether to ignore workflow errors and continue execution
	IgnoreErrors bool

	// InitialStore contains key-value pairs to populate the workflow store before execution
	InitialStore map[string]interface{}
}

// RunResult contains the result of a workflow execution
type RunResult struct {
	WorkflowID    string
	Success       bool
	Error         error
	ExecutionTime time.Duration
	// FinalStore contains the workflow's store state after execution
	FinalStore map[string]interface{}
}

// RunnerOption is a function that configures a Runner
type RunnerOption func(*Runner)

// SpawnResult contains the result of a spawned workflow execution
type SpawnResult struct {
	Success    bool
	Error      error
	FinalStore map[string]interface{}
}

// IPCMiddleware allows customization of inter-process communication
type IPCMiddleware interface {
	// ProcessOutbound is called before sending a message from child to parent
	ProcessOutbound(msgType MessageType, payload interface{}) (MessageType, interface{}, error)

	// ProcessInbound is called when parent receives a message from child
	ProcessInbound(msgType MessageType, payload json.RawMessage) (MessageType, json.RawMessage, error)
}

// IPCHandler handles IPC messages with middleware support
type IPCHandler func(msgType MessageType, payload json.RawMessage) error

// SpawnMiddleware provides hooks for spawn process lifecycle and communication
type SpawnMiddleware interface {
	// BeforeSpawn is called before creating a child process
	BeforeSpawn(ctx context.Context, def SubWorkflowDef) (context.Context, SubWorkflowDef, error)

	// AfterSpawn is called after child process completes (success or failure)
	AfterSpawn(ctx context.Context, def SubWorkflowDef, err error) error

	// OnChildMessage is called when parent receives any message from child
	OnChildMessage(msgType MessageType, payload json.RawMessage) error
}

// IPCMiddlewareFunc is a function adapter for IPCMiddleware
type IPCMiddlewareFunc struct {
	ProcessOutboundFunc func(MessageType, interface{}) (MessageType, interface{}, error)
	ProcessInboundFunc  func(MessageType, json.RawMessage) (MessageType, json.RawMessage, error)
}

func (f IPCMiddlewareFunc) ProcessOutbound(msgType MessageType, payload interface{}) (MessageType, interface{}, error) {
	if f.ProcessOutboundFunc != nil {
		return f.ProcessOutboundFunc(msgType, payload)
	}
	return msgType, payload, nil
}

func (f IPCMiddlewareFunc) ProcessInbound(msgType MessageType, payload json.RawMessage) (MessageType, json.RawMessage, error) {
	if f.ProcessInboundFunc != nil {
		return f.ProcessInboundFunc(msgType, payload)
	}
	return msgType, payload, nil
}

// SpawnMiddlewareFunc is a function adapter for SpawnMiddleware
type SpawnMiddlewareFunc struct {
	BeforeSpawnFunc    func(context.Context, SubWorkflowDef) (context.Context, SubWorkflowDef, error)
	AfterSpawnFunc     func(context.Context, SubWorkflowDef, error) error
	OnChildMessageFunc func(MessageType, json.RawMessage) error
}

func (f SpawnMiddlewareFunc) BeforeSpawn(ctx context.Context, def SubWorkflowDef) (context.Context, SubWorkflowDef, error) {
	if f.BeforeSpawnFunc != nil {
		return f.BeforeSpawnFunc(ctx, def)
	}
	return ctx, def, nil
}

func (f SpawnMiddlewareFunc) AfterSpawn(ctx context.Context, def SubWorkflowDef, err error) error {
	if f.AfterSpawnFunc != nil {
		return f.AfterSpawnFunc(ctx, def, err)
	}
	return nil
}

func (f SpawnMiddlewareFunc) OnChildMessage(msgType MessageType, payload json.RawMessage) error {
	if f.OnChildMessageFunc != nil {
		return f.OnChildMessageFunc(msgType, payload)
	}
	return nil
}

// ActionDef is a serializable representation of an Action.
// It uses a registered ID to identify the action type and can hold
// arbitrary parameters for execution.
type ActionDef struct {
	// ID is the unique identifier of the action as registered in the ActionRegistry.
	ID string `json:"id"`
	// Name overrides the default name of the registered action, if provided.
	Name string `json:"name,omitempty"`
	// Description overrides the default description of the registered action, if provided.
	Description string `json:"description,omitempty"`
	// Tags will be merged with the default tags of the registered action.
	Tags []string `json:"tags,omitempty"`
	// Params are arbitrary key-value pairs that can be passed to the action
	// via the ActionContext's store.
	Params map[string]interface{} `json:"params,omitempty"`
}

// StageDef is a serializable representation of a Stage.
type StageDef struct {
	// ID is the unique identifier for the stage.
	ID string `json:"id"`
	// Name is a human-readable name for the stage.
	Name string `json:"name,omitempty"`
	// Description provides details about the stage's purpose.
	Description string `json:"description,omitempty"`
	// Tags for organization and filtering.
	Tags []string `json:"tags,omitempty"`
	// Actions is an ordered list of action definitions for this stage.
	Actions []ActionDef `json:"actions"`
}

// SubWorkflowDef is a serializable representation of a Workflow.
// This structure is designed to be passed to a child process to define
// the work it needs to perform.
type SubWorkflowDef struct {
	// ID is the unique identifier for the workflow.
	ID string `json:"id"`
	// Name is a human-readable name for the workflow.
	Name string `json:"name,omitempty"`
	// Description provides details about the workflow's purpose.
	Description string `json:"description,omitempty"`
	// Tags for organization and filtering.
	Tags []string `json:"tags,omitempty"`
	// Stages contains all the workflow's stage definitions in execution order.
	Stages []StageDef `json:"stages"`
	// InitialStore contains key-value data that will be loaded into the
	// workflow's store before execution begins. Values must be JSON-serializable.
	InitialStore map[string]interface{} `json:"initialStore,omitempty"`
}

// Store key prefixes for organizing different entities in the store
const (
	// PrefixWorkflow is used for workflow metadata
	PrefixWorkflow = "workflow:"

	// PrefixStage is used for stage metadata
	PrefixStage = "stage:"

	// PrefixAction is used for action metadata
	PrefixAction = "action:"

	// PrefixConfig is used for workflow configuration items
	PrefixConfig = "config:"

	// PrefixData is used for user data in the workflow
	PrefixData = "data:"

	// PrefixTemp is used for temporary data that shouldn't persist between executions
	PrefixTemp = "temp:"
)

// Common tags used across the workflow system
const (
	// TagSystem identifies system-managed entities
	TagSystem = "system"

	// TagCore identifies core/required components
	TagCore = "core"

	// TagDynamic identifies dynamically generated components
	TagDynamic = "dynamic"

	// TagDisabled identifies disabled components
	TagDisabled = "disabled"

	// TagTemporary identifies temporary components
	TagTemporary = "temporary"
)

// Common property keys used in metadata
const (
	// PropCreatedBy tracks who/what created an entity
	PropCreatedBy = "createdBy"

	// PropDependencies lists dependencies of an entity
	PropDependencies = "dependencies"

	// PropOrder tracks execution order for components
	PropOrder = "order"

	// PropStatus tracks the current status
	PropStatus = "status"

	// PropType indicates the type of an entity
	PropType = "type"
)

// Status values for workflow components
const (
	// StatusPending means not yet started
	StatusPending = "pending"

	// StatusRunning means currently in progress
	StatusRunning = "running"

	// StatusCompleted means successfully finished
	StatusCompleted = "completed"

	// StatusFailed means execution failed
	StatusFailed = "failed"

	// StatusSkipped means execution was skipped
	StatusSkipped = "skipped"
)
