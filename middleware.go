package gostage

import "context"

// EngineMiddleware wraps the entire workflow execution.
type EngineMiddleware func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error

// StepMiddleware wraps individual step execution.
type StepMiddleware func(ctx context.Context, s *step, runID RunID, next func() error) error

// TaskMiddleware wraps task function invocation.
type TaskMiddleware func(tctx *Ctx, taskName string, next func() error) error

// ChildMiddleware wraps child process execution.
type ChildMiddleware func(ctx context.Context, job *spawnJob, next func() error) error

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
