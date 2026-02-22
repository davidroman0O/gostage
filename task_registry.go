package gostage

import (
	"fmt"
	"time"
)

// TaskFunc is the function signature for function-based tasks.
// It receives a Ctx and returns an error.
type TaskFunc func(ctx *Ctx) error

// TaskOption configures a registered task.
type TaskOption func(*taskDef)

// taskDef holds the internal definition of a registered task.
type taskDef struct {
	name        string
	fn          TaskFunc
	retries     int
	retryDelay  time.Duration
	description string
	tags        []string
}

// taskRegistry holds all registered function-based tasks.
var taskRegistry = make(map[string]*taskDef)

// Task registers a function-based task with the given name and options.
// Tasks must be registered before building a workflow that references them.
//
//	gostage.Task("greet", func(ctx *gostage.Ctx) error {
//	    name := gostage.GetOr[string](ctx, "user.name", "World")
//	    ctx.Log.Info("Hello, %s!", name)
//	    return nil
//	})
func Task(name string, fn TaskFunc, opts ...TaskOption) {
	if _, exists := taskRegistry[name]; exists {
		panic(fmt.Sprintf("task %q is already registered", name))
	}

	def := &taskDef{
		name: name,
		fn:   fn,
	}

	for _, opt := range opts {
		opt(def)
	}

	taskRegistry[name] = def

	// Also register in the action registry so child processes can instantiate it.
	// We wrap the TaskFunc in an Action adapter.
	RegisterAction(name, func() Action {
		return &taskAction{
			BaseAction: NewBaseAction(def.name, def.description),
			fn:         def.fn,
			tags:       def.tags,
		}
	})
}

// lookupTask returns the taskDef for the given name, or nil.
func lookupTask(name string) *taskDef {
	return taskRegistry[name]
}

// taskAction adapts a TaskFunc to the Action interface.
// This allows function-based tasks to be used wherever Actions are expected,
// including in child processes via the action registry.
type taskAction struct {
	BaseAction
	fn         TaskFunc
	tags       []string
	retries    int
	retryDelay time.Duration
}

func (t *taskAction) Tags() []string {
	if len(t.tags) > 0 {
		return t.tags
	}
	return t.BaseAction.Tags()
}

func (t *taskAction) Execute(actCtx *ActionContext) error {
	ctx := newCtxFromActionContext(actCtx)

	err := t.fn(ctx)
	if err == nil || t.retries <= 0 {
		return err
	}

	// Don't retry bail or suspend errors — those are intentional control flow.
	if isBailOrSuspend(err) {
		return err
	}

	for attempt := 1; attempt <= t.retries; attempt++ {
		if t.retryDelay > 0 {
			time.Sleep(t.retryDelay)
		}
		err = t.fn(ctx)
		if err == nil {
			return nil
		}
		if isBailOrSuspend(err) {
			return err
		}
	}

	return fmt.Errorf("after %d retries: %w", t.retries, err)
}

// isBailOrSuspend checks if an error is a BailError or SuspendError.
func isBailOrSuspend(err error) bool {
	if _, ok := err.(*BailError); ok {
		return true
	}
	if _, ok := err.(*SuspendError); ok {
		return true
	}
	return false
}

// WithRetry configures the number of retry attempts for a task.
func WithRetry(n int) TaskOption {
	return func(d *taskDef) {
		d.retries = n
	}
}

// WithRetryDelay sets the delay between retry attempts.
func WithRetryDelay(d time.Duration) TaskOption {
	return func(def *taskDef) {
		def.retryDelay = d
	}
}

// WithDescription sets a human-readable description for the task.
func WithDescription(desc string) TaskOption {
	return func(d *taskDef) {
		d.description = desc
	}
}

// WithTags adds tags to the task for organization and filtering.
func WithTags(tags ...string) TaskOption {
	return func(d *taskDef) {
		d.tags = append(d.tags, tags...)
	}
}

// ResetTaskRegistry clears the task registry. Intended for testing only.
func ResetTaskRegistry() {
	taskRegistry = make(map[string]*taskDef)
	actionRegistry = make(map[string]ActionFactory)
}
