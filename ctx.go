package gostage

import (
	"context"

	"github.com/davidroman0O/gostage/store"
)

// Ctx is the execution context passed to function-based tasks.
// It provides simplified access to the workflow store, logging, IPC,
// and dynamic workflow modifications.
type Ctx struct {
	// Log provides logging capabilities.
	Log Logger

	// inner is the underlying ActionContext from the existing engine.
	inner *ActionContext
}

// newCtxFromActionContext wraps an ActionContext in the simplified Ctx.
func newCtxFromActionContext(actCtx *ActionContext) *Ctx {
	return &Ctx{
		Log:   actCtx.Logger,
		inner: actCtx,
	}
}

// Context returns the underlying Go context.
func (c *Ctx) Context() context.Context {
	return c.inner.GoContext
}

// ActionContext returns the underlying ActionContext for advanced usage.
// This provides access to dynamic workflow modifications (AddDynamicAction,
// AddDynamicStage, Enable/Disable, etc.).
func (c *Ctx) ActionContext() *ActionContext {
	return c.inner
}

// store returns the workflow's KV store.
func (c *Ctx) store() *store.KVStore {
	return c.inner.Workflow.Store
}

// --- Top-level store access functions ---

// Get retrieves a typed value from the workflow store.
// Returns the zero value of T if the key doesn't exist or the type doesn't match.
//
//	name := gostage.Get[string](ctx, "user.name")
func Get[T any](ctx *Ctx, key string) T {
	val, err := store.Get[T](ctx.store(), key)
	if err != nil {
		var zero T
		return zero
	}
	return val
}

// GetOr retrieves a typed value from the workflow store, returning the
// default value if the key doesn't exist or the type doesn't match.
//
//	name := gostage.GetOr[string](ctx, "user.name", "World")
func GetOr[T any](ctx *Ctx, key string, defaultVal T) T {
	val, err := store.Get[T](ctx.store(), key)
	if err != nil {
		return defaultVal
	}
	return val
}

// Set stores a value in the workflow store.
//
//	gostage.Set(ctx, "result", 42)
func Set(ctx *Ctx, key string, value any) {
	ctx.store().Put(key, value)
}

// Send sends a message through the IPC broker.
// This is used for parent-child communication during spawned workflows.
//
//	gostage.Send(ctx, "progress", gostage.P{"pct": 50})
func Send(ctx *Ctx, msgType string, payload any) error {
	return ctx.inner.Send(MessageType(msgType), payload)
}

// Bail signals an early exit from the workflow. This is not an error —
// it's a deliberate decision to stop processing (e.g., validation failure).
//
//	return gostage.Bail(ctx, "Must be 18+")
func Bail(ctx *Ctx, reason string) error {
	return &BailError{Reason: reason}
}

// Suspend pauses the workflow and persists its state, waiting for
// external input to resume. The data map describes what input is needed.
//
//	return gostage.Suspend(ctx, gostage.P{"reason": "Needs approval"})
func Suspend(ctx *Ctx, data P) error {
	return &SuspendError{Data: data}
}

// IsResuming returns true if the current execution is a resume from a
// previously suspended state.
func IsResuming(ctx *Ctx) bool {
	val, err := store.Get[bool](ctx.store(), "__resuming")
	if err != nil {
		return false
	}
	return val
}

// ResumeData retrieves typed data that was provided when resuming a
// suspended workflow.
//
//	approved := gostage.ResumeData[bool](ctx, "approved")
func ResumeData[T any](ctx *Ctx, key string) T {
	return Get[T](ctx, "__resume:"+key)
}

// Item retrieves the current iteration item in a ForEach loop.
//
//	track := gostage.Item[Track](ctx)
func Item[T any](ctx *Ctx) T {
	return Get[T](ctx, "__foreach_item")
}

// ItemIndex returns the current iteration index in a ForEach loop.
func ItemIndex(ctx *Ctx) int {
	return Get[int](ctx, "__foreach_index")
}
