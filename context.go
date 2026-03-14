package gostage

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
)

// Ctx is the execution context passed to every task function during a workflow run.
// It provides access to the shared key-value store, structured logging, control flow
// primitives (Bail, Suspend), dynamic workflow mutations (InsertAfter, DisableStep),
// and ForEach iteration state.
//
// Ctx is not a context.Context. Call ctx.Context() to obtain the underlying
// context.Context for deadline and cancellation checks.
//
// In concurrent ForEach iterations, each goroutine receives its own Ctx with
// independent ForEach item/index values, but all share the same underlying state.
// Write to unique keys (e.g. fmt.Sprintf("result_%d", gostage.ItemIndex(ctx)))
// to avoid data races.
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

// Context returns the underlying context.Context, which carries the run's
// deadline and cancellation signal. Use this for passing to libraries that
// accept context.Context (e.g. HTTP clients, database drivers).
func (c *Ctx) Context() context.Context { return c.goCtx }

// --- Top-level store functions ---

// coerce attempts to convert val to type T.
// Direct type assertion is tried first (fast path). If that fails and T is
// a numeric type, reflect-based conversion is used. This handles the common
// case where JSON decoding turns int into float64.
// For composite types (structs, slices, maps), JSON re-marshal is used to
// restore the original type after persistence or spawn round-trips.
func coerce[T any](val any) (T, bool) {
	if typed, ok := val.(T); ok {
		return typed, true
	}
	var zero T
	targetType := reflect.TypeOf((*T)(nil)).Elem()

	// Numeric coercion: handles JSON float64 → int, etc.
	if targetType.Kind() >= reflect.Int && targetType.Kind() <= reflect.Float64 {
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

	// JSON re-marshal for composite types.
	// After persistence/spawn round-trip, JSON unmarshal into any produces
	// map[string]interface{} for objects and []interface{} for arrays.
	// Re-marshal to JSON and unmarshal into T to restore the original type.
	targetKind := targetType.Kind()
	if targetKind == reflect.Struct || targetKind == reflect.Map ||
		targetKind == reflect.Slice || targetKind == reflect.Ptr {
		jsonBytes, err := json.Marshal(val)
		if err != nil {
			return zero, false
		}
		result := reflect.New(targetType).Interface()
		if err := json.Unmarshal(jsonBytes, result); err != nil {
			return zero, false
		}
		return reflect.ValueOf(result).Elem().Interface().(T), true
	}

	return zero, false
}

// Get retrieves a typed value from the workflow's key-value store.
// If the key is missing or the stored value cannot be coerced to T, the zero
// value of T is returned. Use [GetOk] to distinguish a missing key from a
// stored zero value.
//
// Type coercion is applied automatically to handle JSON round-trip effects:
// numeric types are converted (e.g. float64 to int), and composite types
// (structs, slices, maps) are recovered via JSON re-marshal when a direct
// type assertion fails.
//
//	name := gostage.Get[string](ctx, "user.name")
//	count := gostage.Get[int](ctx, "retry_count") // works even if stored as float64
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

// GetOr retrieves a typed value from the workflow store, returning def if the
// key is missing or the stored value cannot be coerced to T. The same type
// coercion rules as [Get] apply (numeric conversion, JSON re-marshal for
// composite types).
//
//	name := gostage.GetOr[string](ctx, "user.name", "World")
//	limit := gostage.GetOr[int](ctx, "page_size", 25)
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

// GetOk retrieves a typed value and reports whether the key exists and was
// successfully coerced to T. Returns (zero, false) if the key is missing or
// the stored value is incompatible with T. This allows callers to distinguish
// a missing key from a stored zero value, which [Get] cannot do.
//
//	val, ok := gostage.GetOk[string](ctx, "user.name")
//	if !ok {
//	    // key does not exist or is not a string
//	}
func GetOk[T any](ctx *Ctx, key string) (T, bool) {
	val, ok := ctx.state.Get(key)
	if !ok {
		var zero T
		return zero, false
	}
	return coerce[T](val)
}

// Set stores a typed value in the workflow's key-value store. The value must
// be JSON-serializable; types containing channels, functions, unsafe pointers,
// or complex numbers are rejected with a descriptive error. Maps must have
// string keys.
//
// Set returns [ErrStateLimitExceeded] if the engine's state entry limit is
// reached and the key is new. Updating an existing key always succeeds
// regardless of the limit.
//
// Values are stored by reference, not deep-copied. Mutable types (slices,
// maps, structs with pointer fields) should not be modified after storing
// unless the caller is the sole writer. In concurrent ForEach, write to
// unique keys to avoid data races:
//
//	gostage.Set(ctx, fmt.Sprintf("result_%d", gostage.ItemIndex(ctx)), val)
func Set[T any](ctx *Ctx, key string, value T) error {
	t := reflect.TypeOf(value)
	if t != nil && !isJSONSerializable(t) {
		return fmt.Errorf("gostage.Set: type %T is not JSON-serializable", value)
	}
	return ctx.state.setWithLimit(key, value)
}

// Delete removes a key from the workflow store. The deletion is durable: it
// is recorded as a tombstone and flushed to the persistence layer at the next
// step boundary. After a crash and resume, the key does not reappear.
// Deleting a key that does not exist is a no-op.
//
//	gostage.Delete(ctx, "temp_data")
func Delete(ctx *Ctx, key string) {
	ctx.state.Delete(key)
}

// --- Control flow functions ---

// Bail signals the workflow to exit early with an intentional, non-error
// termination. The engine catches the returned [BailError], sets the run
// status to [Bailed], and records the reason in [Result].BailReason.
// Bail should be returned as the task's error value, not called and discarded.
//
//	if age < 18 {
//	    return gostage.Bail(ctx, "Must be 18+")
//	}
func Bail(ctx *Ctx, reason string) error {
	return &BailError{Reason: reason}
}

// Suspend pauses the workflow and persists its current state so it can be
// resumed later via [Engine.Resume]. The engine catches the returned
// [SuspendError], sets the run status to [Suspended], and stores data in
// [Result].SuspendData. The data map is informational and is not automatically
// merged into the store; callers typically use it to communicate what input
// is needed for resumption.
//
// Suspend should be returned as the task's error value:
//
//	return gostage.Suspend(ctx, gostage.Params{"reason": "needs approval"})
//
// When the workflow is resumed, all tasks re-execute from the beginning.
// Use [IsResuming] to skip already-completed work and [ResumeData] to read
// the data provided to Engine.Resume.
func Suspend(ctx *Ctx, data Params) error {
	return &SuspendError{Data: data}
}

// IsResuming reports whether the current execution is a resume from a
// previously suspended run. When true, the task should skip any work that
// was already completed before suspension and use [ResumeData] to read the
// external input provided to [Engine.Resume].
//
//	if gostage.IsResuming(ctx) {
//	    approved := gostage.ResumeData[bool](ctx, "approved")
//	    // handle approval decision
//	}
func IsResuming(ctx *Ctx) bool {
	return ctx.resuming
}

// ResumeData retrieves a typed value from the data map that was passed to
// [Engine.Resume]. Internally, resume data is stored under "__resume:" prefixed
// keys in the workflow store, so this is equivalent to Get[T](ctx, "__resume:"+key).
// Returns the zero value of T if the key is missing or the type does not match.
//
//	approved := gostage.ResumeData[bool](ctx, "approved")
func ResumeData[T any](ctx *Ctx, key string) T {
	return Get[T](ctx, "__resume:"+key)
}

// --- ForEach helpers ---

// Item retrieves the current ForEach iteration item, coerced to type T.
// Returns the zero value of T if called outside a ForEach context or if the
// stored item cannot be coerced to T. When called inside a sub-workflow
// nested within ForEach, Item falls back to reading the "__foreach_item"
// key from the store.
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

// ItemIndex returns the zero-based index of the current ForEach iteration.
// Returns 0 if called outside a ForEach context. When called inside a
// sub-workflow nested within ForEach, ItemIndex falls back to reading the
// "__foreach_index" key from the store, handling JSON round-trip float64
// conversion automatically.
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

// FindStepsByTag returns the IDs of all steps in the executing workflow that
// carry the given tag. Returns nil if the workflow reference is unavailable
// or no steps match. Tags are assigned via [WithStepTags] during workflow
// construction.
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

// DisableByTag queues mutations to disable all steps carrying the given tag.
// Disabled steps are skipped during execution. The mutations are applied at
// the next step boundary and survive suspend/resume cycles.
func DisableByTag(ctx *Ctx, tag string) {
	ids := FindStepsByTag(ctx, tag)
	for _, id := range ids {
		DisableStep(ctx, id)
	}
}

// EnableByTag queues mutations to re-enable all previously disabled steps
// carrying the given tag. The mutations are applied at the next step boundary.
func EnableByTag(ctx *Ctx, tag string) {
	ids := FindStepsByTag(ctx, tag)
	for _, id := range ids {
		EnableStep(ctx, id)
	}
}

// --- Dynamic mutations ---

// InsertAfter queues a mutation to dynamically insert a new task step
// immediately after the currently executing step. The taskName must refer to
// a task registered in the task registry. The mutation is applied at the next
// step boundary and the inserted step survives suspend/resume cycles. This
// is a no-op if the mutation queue is not initialized (e.g. outside engine
// execution).
//
//	gostage.InsertAfter(ctx, "send.confirmation.email")
func InsertAfter(ctx *Ctx, taskName string) {
	if ctx.mutations != nil {
		ctx.mutations.Push(Mutation{
			Kind:     MutInsertAfter,
			TaskName: taskName,
		})
	}
}

// DisableStep queues a mutation to disable a step, identified by its ID or
// name. Disabled steps are skipped during execution. If multiple steps share
// the same name, all matching steps are disabled. The mutation is applied at
// the next step boundary and survives suspend/resume cycles.
func DisableStep(ctx *Ctx, stepID string) {
	if ctx.mutations != nil {
		ctx.mutations.Push(Mutation{
			Kind:     MutDisableStep,
			TargetID: stepID,
		})
	}
}

// EnableStep queues a mutation to re-enable a previously disabled step,
// identified by its ID or name. If multiple steps share the same name, all
// matching steps are re-enabled. The mutation is applied at the next step
// boundary.
func EnableStep(ctx *Ctx, stepID string) {
	if ctx.mutations != nil {
		ctx.mutations.Push(Mutation{
			Kind:     MutEnableStep,
			TargetID: stepID,
		})
	}
}

// --- IPC ---

// Send dispatches an IPC message from the current task. The routing depends
// on the execution context:
//
//   - In a child process (spawned via [WithSpawn]), the message is sent over
//     gRPC to the parent engine.
//   - In the parent process, the message is routed locally through the
//     engine's registered message handlers (see [Engine.OnMessage]).
//   - If neither a send function nor an engine reference is available, the
//     call is silently ignored.
//
// The payload is converted to map[string]any for handler dispatch. If the
// payload is not already a map[string]any, it is wrapped as {"data": payload}.
//
//	gostage.Send(ctx, "progress", gostage.Params{"pct": 50})
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
		ctx.engine.dispatchMessage(msgType, payloadMap, ctx.state.runID)
	}
	return nil
}
