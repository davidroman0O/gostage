package gostage

import (
	"context"
	"math"
	"reflect"
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
	ctx := &Ctx{
		Log:   logger,
		goCtx: goCtx,
		state: s,
	}
	// Propagate ForEach item/index from context chain (set by executeForEachItem).
	// This is safe for concurrent ForEach because each goroutine gets its own context.
	if fe, ok := goCtx.Value(forEachCtxKey{}).(*forEachCtxData); ok {
		ctx.forEachItem = fe.item
		ctx.forEachIndex = fe.index
	}
	return ctx
}

// Context returns the underlying context.Context for deadline/cancellation checks.
func (c *Ctx) Context() context.Context { return c.goCtx }

// --- Top-level store functions ---

// coerce attempts to convert val to type T.
// Direct type assertion is tried first (fast path). If that fails and T is
// a numeric type, reflect-based conversion is used. This handles the common
// case where JSON decoding turns int into float64.
func coerce[T any](val any) (T, bool) {
	if typed, ok := val.(T); ok {
		return typed, true
	}
	var zero T
	targetType := reflect.TypeOf((*T)(nil)).Elem()
	if targetType.Kind() < reflect.Int || targetType.Kind() > reflect.Float64 {
		return zero, false // T is not numeric — no coercion possible
	}
	srcVal := reflect.ValueOf(val)
	if !srcVal.IsValid() || !srcVal.Type().ConvertibleTo(targetType) {
		return zero, false
	}
	// Guard: float → integer must not lose fractional part
	if srcVal.Kind() == reflect.Float64 || srcVal.Kind() == reflect.Float32 {
		f := srcVal.Float()
		if targetType.Kind() >= reflect.Int && targetType.Kind() <= reflect.Uint64 {
			if f != math.Trunc(f) {
				return zero, false
			}
		}
	}
	return srcVal.Convert(targetType).Interface().(T), true
}

// Get retrieves a typed value from the workflow store.
// Numeric coercion is applied automatically (e.g. float64 → int after JSON round-trip).
//
//	name := gostage.Get[string](ctx, "user.name")
func Get[T any](ctx *Ctx, key string) T {
	val, ok := ctx.state.Get(key)
	if !ok {
		var zero T
		return zero
	}
	typed, ok := coerce[T](val)
	if !ok {
		var zero T
		return zero
	}
	return typed
}

// GetOr retrieves a typed value from the workflow store, returning def if not found.
// Numeric coercion is applied automatically.
//
//	name := gostage.GetOr[string](ctx, "user.name", "World")
func GetOr[T any](ctx *Ctx, key string, def T) T {
	val, ok := ctx.state.Get(key)
	if !ok {
		return def
	}
	typed, ok := coerce[T](val)
	if !ok {
		return def
	}
	return typed
}

// Set stores a typed value in the workflow store.
//
//	gostage.Set(ctx, "result", 42)
func Set[T any](ctx *Ctx, key string, value T) {
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
	item := ctx.forEachItem
	// Fallback: read from state when inside a sub-workflow within ForEach.
	// The executor sets "__foreach_item" in state for sub-workflow iterations.
	if item == nil && ctx.state != nil {
		if v, ok := ctx.state.Get("__foreach_item"); ok {
			item = v
		}
	}
	if item == nil {
		var zero T
		return zero
	}
	val, ok := coerce[T](item)
	if !ok {
		var zero T
		return zero
	}
	return val
}

// ItemIndex returns the current ForEach iteration index.
func ItemIndex(ctx *Ctx) int {
	if ctx.forEachItem != nil {
		return ctx.forEachIndex
	}
	// Fallback: read from state when inside a sub-workflow within ForEach.
	if ctx.state != nil {
		if v, ok := ctx.state.Get("__foreach_index"); ok {
			switch idx := v.(type) {
			case int:
				return idx
			case float64:
				// JSON deserialization produces float64 for numbers (e.g. in spawn)
				return int(idx)
			}
		}
	}
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
