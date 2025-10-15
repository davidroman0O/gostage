package workflow

import (
	"fmt"
	"reflect"

	rt "github.com/davidroman0O/gostage/v3/runtime"
	deadlock "github.com/sasha-s/go-deadlock"
)

type middlewareRegistry[T any] struct {
	mu      deadlock.RWMutex
	entries map[string]T
}

func newMiddlewareRegistry[T any]() *middlewareRegistry[T] {
	return &middlewareRegistry[T]{
		entries: make(map[string]T),
	}
}

func (r *middlewareRegistry[T]) register(id string, mw T) error {
	if id == "" {
		return fmt.Errorf("workflow: middleware id required")
	}
	if isNil(mw) {
		return fmt.Errorf("workflow: middleware %s is nil", id)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[id] = mw
	return nil
}

func (r *middlewareRegistry[T]) resolve(id string) (T, error) {
	var zero T
	if id == "" {
		return zero, fmt.Errorf("workflow: middleware id required")
	}
	r.mu.RLock()
	mw, ok := r.entries[id]
	r.mu.RUnlock()
	if !ok {
		return zero, fmt.Errorf("workflow: middleware %s not registered", id)
	}
	return mw, nil
}

func isNil[T any](v T) bool {
	val := reflect.ValueOf(v)
	switch val.Kind() {
	case reflect.Func, reflect.Map, reflect.Pointer, reflect.Interface, reflect.Slice:
		return val.IsNil()
	default:
		return false
	}
}

var (
	workflowMiddlewareRegistry = newMiddlewareRegistry[rt.WorkflowMiddleware]()
	stageMiddlewareRegistry    = newMiddlewareRegistry[rt.StageMiddleware]()
	actionMiddlewareRegistry   = newMiddlewareRegistry[rt.ActionMiddleware]()
)

// RegisterWorkflowMiddleware registers a workflow-level middleware by identifier.
func RegisterWorkflowMiddleware(id string, mw rt.WorkflowMiddleware) error {
	return workflowMiddlewareRegistry.register(id, mw)
}

// MustRegisterWorkflowMiddleware registers a workflow middleware and panics on error.
func MustRegisterWorkflowMiddleware(id string, mw rt.WorkflowMiddleware) {
	if err := RegisterWorkflowMiddleware(id, mw); err != nil {
		panic(err)
	}
}

// RegisterStageMiddleware registers a stage-level middleware by identifier.
func RegisterStageMiddleware(id string, mw rt.StageMiddleware) error {
	return stageMiddlewareRegistry.register(id, mw)
}

// MustRegisterStageMiddleware registers a stage middleware and panics on error.
func MustRegisterStageMiddleware(id string, mw rt.StageMiddleware) {
	if err := RegisterStageMiddleware(id, mw); err != nil {
		panic(err)
	}
}

// RegisterActionMiddleware registers an action-level middleware by identifier.
func RegisterActionMiddleware(id string, mw rt.ActionMiddleware) error {
	return actionMiddlewareRegistry.register(id, mw)
}

// MustRegisterActionMiddleware registers an action middleware and panics on error.
func MustRegisterActionMiddleware(id string, mw rt.ActionMiddleware) {
	if err := RegisterActionMiddleware(id, mw); err != nil {
		panic(err)
	}
}

func resolveWorkflowMiddleware(id string) (rt.WorkflowMiddleware, error) {
	return workflowMiddlewareRegistry.resolve(id)
}

func resolveStageMiddleware(id string) (rt.StageMiddleware, error) {
	return stageMiddlewareRegistry.resolve(id)
}

func resolveActionMiddleware(id string) (rt.ActionMiddleware, error) {
	return actionMiddlewareRegistry.resolve(id)
}

// resetMiddlewareRegistries is used by tests to restore a clean state.
func resetMiddlewareRegistries() {
	workflowMiddlewareRegistry = newMiddlewareRegistry[rt.WorkflowMiddleware]()
	stageMiddlewareRegistry = newMiddlewareRegistry[rt.StageMiddleware]()
	actionMiddlewareRegistry = newMiddlewareRegistry[rt.ActionMiddleware]()
}
