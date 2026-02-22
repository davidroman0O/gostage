package gostage

import (
	"context"
)

// Ctx is the execution context passed to every task function.
// It provides access to the workflow's state, logging, and control flow.
type Ctx struct {
	// Log provides structured logging within a task.
	Log Logger

	goCtx context.Context
	state *runState

	// sendFn is the IPC hook for child process communication.
	// nil when not running as a child process.
	sendFn func(msgType string, payload any) error

	// resuming indicates this execution is a resume from a suspended state.
	resuming bool

	// forEachItem holds the current item during ForEach iteration.
	forEachItem any
	// forEachIndex holds the current index during ForEach iteration.
	forEachIndex int

	// mutations is the workflow's mutation queue for dynamic step modifications.
	mutations *mutationQueue

	// workflow is the executing workflow (for tag queries and mutations).
	workflow *Workflow

	// engine is the executing engine (for local IPC routing).
	engine *Engine
}

func newCtx(goCtx context.Context, s *runState, logger Logger) *Ctx {
	return &Ctx{
		Log:   logger,
		goCtx: goCtx,
		state: s,
	}
}

// Context returns the underlying context.Context for deadline/cancellation checks.
func (c *Ctx) Context() context.Context { return c.goCtx }

// --- Top-level store functions ---

// Get retrieves a typed value from the workflow store.
//
//	name := gostage.Get[string](ctx, "user.name")
func Get[T any](ctx *Ctx, key string) T {
	val, ok := ctx.state.Get(key)
	if !ok {
		var zero T
		return zero
	}
	typed, ok := val.(T)
	if !ok {
		var zero T
		return zero
	}
	return typed
}

// GetOr retrieves a typed value from the workflow store, returning def if not found.
//
//	name := gostage.GetOr[string](ctx, "user.name", "World")
func GetOr[T any](ctx *Ctx, key string, def T) T {
	val, ok := ctx.state.Get(key)
	if !ok {
		return def
	}
	typed, ok := val.(T)
	if !ok {
		return def
	}
	return typed
}

// Set stores a value in the workflow store.
//
//	gostage.Set(ctx, "result", 42)
func Set(ctx *Ctx, key string, value any) {
	ctx.state.Set(key, value)
}

// --- Control flow functions ---

// Bail signals the workflow to exit early (not an error).
// The bail reason is recorded in the result.
//
//	return gostage.Bail(ctx, "Must be 18+")
func Bail(ctx *Ctx, reason string) error {
	return &BailError{Reason: reason}
}

// Suspend pauses the workflow, persists state, and waits for external input.
// The data map is stored and available when the workflow is resumed.
//
//	return gostage.Suspend(ctx, gostage.P{"reason": "needs approval"})
func Suspend(ctx *Ctx, data P) error {
	return &SuspendError{Data: data}
}

// IsResuming returns true if the current execution is a resume from a suspended state.
func IsResuming(ctx *Ctx) bool {
	return ctx.resuming
}

// ResumeData retrieves a typed value from the resume data provided by engine.Resume().
func ResumeData[T any](ctx *Ctx, key string) T {
	return Get[T](ctx, "__resume:"+key)
}

// --- ForEach helpers ---

// Item retrieves the current ForEach iteration item with type safety.
//
//	track := gostage.Item[Track](ctx)
func Item[T any](ctx *Ctx) T {
	if ctx.forEachItem == nil {
		var zero T
		return zero
	}
	val, ok := ctx.forEachItem.(T)
	if !ok {
		var zero T
		return zero
	}
	return val
}

// ItemIndex returns the current ForEach iteration index.
func ItemIndex(ctx *Ctx) int {
	return ctx.forEachIndex
}

// --- Tag queries ---

// FindStepsByTag returns the IDs of all steps in the workflow that have the given tag.
func FindStepsByTag(ctx *Ctx, tag string) []string {
	if ctx.workflow == nil {
		return nil
	}
	var ids []string
	for i := range ctx.workflow.steps {
		for _, t := range ctx.workflow.steps[i].tags {
			if t == tag {
				ids = append(ids, ctx.workflow.steps[i].id)
				break
			}
		}
	}
	return ids
}

// DisableByTag queues mutations to disable all steps with the given tag.
func DisableByTag(ctx *Ctx, tag string) {
	ids := FindStepsByTag(ctx, tag)
	for _, id := range ids {
		DisableStep(ctx, id)
	}
}

// EnableByTag queues mutations to enable all steps with the given tag.
func EnableByTag(ctx *Ctx, tag string) {
	ids := FindStepsByTag(ctx, tag)
	for _, id := range ids {
		EnableStep(ctx, id)
	}
}

// --- Dynamic mutations ---

// InsertAfter queues a mutation to insert a new task step after the current step.
func InsertAfter(ctx *Ctx, taskName string) {
	if ctx.mutations != nil {
		ctx.mutations.Push(Mutation{
			Kind:     MutInsertAfter,
			TaskName: taskName,
		})
	}
}

// DisableStep queues a mutation to disable a step by ID or name.
func DisableStep(ctx *Ctx, stepID string) {
	if ctx.mutations != nil {
		ctx.mutations.Push(Mutation{
			Kind:     MutDisableStep,
			TargetID: stepID,
		})
	}
}

// EnableStep queues a mutation to re-enable a previously disabled step.
func EnableStep(ctx *Ctx, stepID string) {
	if ctx.mutations != nil {
		ctx.mutations.Push(Mutation{
			Kind:     MutEnableStep,
			TargetID: stepID,
		})
	}
}

// --- IPC ---

// Send sends a message via IPC.
// In child processes, the message goes over gRPC to the parent.
// In the parent process, the message routes through the engine's handler system.
//
//	gostage.Send(ctx, "progress", gostage.P{"pct": 50})
func Send(ctx *Ctx, msgType string, payload any) error {
	if ctx.sendFn != nil {
		return ctx.sendFn(msgType, payload)
	}
	// Local routing: dispatch through engine handlers
	if ctx.engine != nil {
		payloadMap, ok := payload.(map[string]any)
		if !ok {
			payloadMap = map[string]any{"data": payload}
		}
		ctx.engine.dispatchMessage(msgType, payloadMap)
	}
	return nil
}
