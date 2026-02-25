package gostage

import "context"

// EngineMiddleware wraps the entire workflow execution.
type EngineMiddleware func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error

// StepInfo exposes step metadata to middleware implementations.
type StepInfo struct {
	ID       string
	Name     string
	Kind     StepKind
	Tags     []string
	Disabled bool
	TaskName string
}

// StepMiddleware wraps individual step execution.
type StepMiddleware func(ctx context.Context, info StepInfo, runID RunID, next func() error) error

// TaskMiddleware wraps task function invocation.
type TaskMiddleware func(tctx *Ctx, taskName string, next func() error) error

// ChildMiddleware wraps child process execution.
// The SpawnJob fields expose job information to external Plugin implementations.
type ChildMiddleware func(ctx context.Context, job *SpawnJob, next func() error) error

// Plugin registers middleware at multiple levels.
type Plugin interface {
	// EngineMiddleware returns engine-level middleware, or nil.
	EngineMiddleware() EngineMiddleware
	// StepMiddleware returns step-level middleware, or nil.
	StepMiddleware() StepMiddleware
	// TaskMiddleware returns task-level middleware, or nil.
	TaskMiddleware() TaskMiddleware
	// ChildMiddleware returns child-process-level middleware, or nil.
	ChildMiddleware() ChildMiddleware
}

// MessageHandler is called when a child process sends an IPC message.
type MessageHandler func(msgType string, payload map[string]any)
