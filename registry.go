package gostage

import (
	"fmt"
	"sync"
)

// Registry holds task, condition, and map function registrations.
// Each Engine instance owns a Registry. The package-level default
// registry preserves backward compatibility with the global registration API.
type Registry struct {
	taskMu sync.RWMutex
	tasks  map[string]*taskDef

	condMu     sync.RWMutex
	conditions map[string]ConditionFunc

	mapFnMu  sync.RWMutex
	mapFns   map[string]MapFunc
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		tasks:      make(map[string]*taskDef),
		conditions: make(map[string]ConditionFunc),
		mapFns:     make(map[string]MapFunc),
	}
}

// defaultRegistry is the package-level registry used by the global
// Task(), Condition(), and MapFn() convenience functions.
var defaultRegistry = NewRegistry()

// RegisterTask adds a named task to the registry.
// Panics if a task with the same name is already registered.
func (r *Registry) RegisterTask(name string, fn TaskFunc, opts ...TaskOption) {
	if name == "" {
		panic("gostage.Task: " + ErrEmptyName.Error())
	}

	r.taskMu.Lock()
	defer r.taskMu.Unlock()

	if _, exists := r.tasks[name]; exists {
		panic(fmt.Sprintf("gostage: task %q already registered", name))
	}

	td := &taskDef{
		name:    name,
		fn:      fn,
		retries: -1,
	}
	for _, opt := range opts {
		opt(td)
	}
	r.tasks[name] = td
}

// lookupTask returns the task definition for the given name, or nil.
func (r *Registry) lookupTask(name string) *taskDef {
	r.taskMu.RLock()
	defer r.taskMu.RUnlock()
	return r.tasks[name]
}

// RegisterCondition adds a named condition to the registry.
// Panics if a condition with the same name is already registered.
func (r *Registry) RegisterCondition(name string, fn ConditionFunc) {
	r.condMu.Lock()
	defer r.condMu.Unlock()

	if _, exists := r.conditions[name]; exists {
		panic(fmt.Sprintf("gostage: condition %q already registered", name))
	}
	r.conditions[name] = fn
}

// lookupCondition returns the condition function for the given name, or nil.
func (r *Registry) lookupCondition(name string) ConditionFunc {
	r.condMu.RLock()
	defer r.condMu.RUnlock()
	return r.conditions[name]
}

// RegisterMapFn adds a named map function to the registry.
// Panics if a map function with the same name is already registered.
func (r *Registry) RegisterMapFn(name string, fn MapFunc) {
	r.mapFnMu.Lock()
	defer r.mapFnMu.Unlock()

	if _, exists := r.mapFns[name]; exists {
		panic(fmt.Sprintf("gostage: map function %q already registered", name))
	}
	r.mapFns[name] = fn
}

// lookupMapFn returns the map function for the given name, or nil.
func (r *Registry) lookupMapFn(name string) MapFunc {
	r.mapFnMu.RLock()
	defer r.mapFnMu.RUnlock()
	return r.mapFns[name]
}

// listTasksByTag returns names of all tasks that have the given tag.
func (r *Registry) listTasksByTag(tag string) []string {
	r.taskMu.RLock()
	defer r.taskMu.RUnlock()

	var names []string
	for name, td := range r.tasks {
		for _, t := range td.tags {
			if t == tag {
				names = append(names, name)
				break
			}
		}
	}
	return names
}

// Reset clears all registrations. Used in tests.
func (r *Registry) Reset() {
	r.taskMu.Lock()
	r.tasks = make(map[string]*taskDef)
	r.taskMu.Unlock()

	r.condMu.Lock()
	r.conditions = make(map[string]ConditionFunc)
	r.condMu.Unlock()

	r.mapFnMu.Lock()
	r.mapFns = make(map[string]MapFunc)
	r.mapFnMu.Unlock()
}

// WithRegistry configures the engine to use a specific registry
// instead of the package-level default.
func WithRegistry(r *Registry) EngineOption {
	return func(e *Engine) error {
		e.registry = r
		return nil
	}
}
