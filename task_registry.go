package gostage

import (
	"fmt"
	"sync"
	"time"
)

// TaskFunc is the function signature for task implementations.
//
//	gostage.Task("greet", func(ctx *gostage.Ctx) error {
//	    name := gostage.GetOr[string](ctx, "name", "World")
//	    ctx.Log.Info("Hello, %s!", name)
//	    return nil
//	})
type TaskFunc func(ctx *Ctx) error

// taskDef holds the definition of a registered task.
type taskDef struct {
	name        string
	fn          TaskFunc
	retries     int
	retryDelay  time.Duration
	tags        []string
	description string
}

var (
	taskRegistryMu sync.RWMutex
	taskRegistry   = make(map[string]*taskDef)
)

// Task registers a named task function in the global registry.
// Panics if a task with the same name is already registered.
//
//	gostage.Task("greet", func(ctx *gostage.Ctx) error {
//	    return nil
//	}, gostage.WithRetry(3), gostage.WithRetryDelay(time.Second))
func Task(name string, fn TaskFunc, opts ...TaskOption) {
	taskRegistryMu.Lock()
	defer taskRegistryMu.Unlock()

	if _, exists := taskRegistry[name]; exists {
		panic(fmt.Sprintf("gostage: task %q already registered", name))
	}

	td := &taskDef{
		name: name,
		fn:   fn,
	}
	for _, opt := range opts {
		opt(td)
	}
	taskRegistry[name] = td

	// Auto-bridge: register action factory so child processes can reconstruct workflows
	registerTaskAsAction(name, fn, td)
}

// lookupTask returns the task definition for the given name, or nil if not found.
func lookupTask(name string) *taskDef {
	taskRegistryMu.RLock()
	defer taskRegistryMu.RUnlock()
	return taskRegistry[name]
}

// ResetTaskRegistry clears all registered tasks and action factories. Used in tests.
func ResetTaskRegistry() {
	taskRegistryMu.Lock()
	defer taskRegistryMu.Unlock()
	taskRegistry = make(map[string]*taskDef)

	// Also clear action factories since they're auto-bridged from tasks
	ResetActionFactories()
}

// TaskOption configures a task definition.
type TaskOption func(*taskDef)

// WithRetry sets the number of retry attempts for a task.
//
//	gostage.Task("flaky", handler, gostage.WithRetry(3))
func WithRetry(n int) TaskOption {
	return func(td *taskDef) {
		td.retries = n
	}
}

// WithRetryDelay sets the delay between retry attempts.
//
//	gostage.Task("flaky", handler, gostage.WithRetry(3), gostage.WithRetryDelay(2*time.Second))
func WithRetryDelay(d time.Duration) TaskOption {
	return func(td *taskDef) {
		td.retryDelay = d
	}
}

// WithTags attaches tags to a task for querying and conditional execution.
//
//	gostage.Task("send.email", handler, gostage.WithTags("notification", "async"))
func WithTags(tags ...string) TaskOption {
	return func(td *taskDef) {
		td.tags = tags
	}
}

// WithDescription sets a human-readable description for a task.
//
//	gostage.Task("charge", handler, gostage.WithDescription("Process payment"))
func WithDescription(desc string) TaskOption {
	return func(td *taskDef) {
		td.description = desc
	}
}

// ListTasksByTag returns names of all tasks that have the given tag.
func ListTasksByTag(tag string) []string {
	taskRegistryMu.RLock()
	defer taskRegistryMu.RUnlock()

	var names []string
	for name, td := range taskRegistry {
		for _, t := range td.tags {
			if t == tag {
				names = append(names, name)
				break
			}
		}
	}
	return names
}
