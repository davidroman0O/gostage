package gostage

import (
	"fmt"
	"sync"
)

// Action is the struct-based alternative to TaskFunc.
// It provides metadata (name, description, tags) and an Execute method.
type Action interface {
	Name() string
	Description() string
	Tags() []string
	Execute(ctx *Ctx) error
}

// BaseAction provides default implementations for the Action interface.
// Embed it in custom actions and override only what you need.
//
//	type MyAction struct {
//	    gostage.BaseAction
//	}
//	func (a *MyAction) Execute(ctx *gostage.Ctx) error { return nil }
type BaseAction struct {
	ActionName        string
	ActionDescription string
	ActionTags        []string
}

func (a *BaseAction) Name() string        { return a.ActionName }
func (a *BaseAction) Description() string  { return a.ActionDescription }
func (a *BaseAction) Tags() []string       { return a.ActionTags }
func (a *BaseAction) Execute(_ *Ctx) error { return nil }

// ActionFactory creates an Action instance by name.
// Used by child processes to reconstruct workflows from serialized definitions.
type ActionFactory func() Action

var (
	actionFactoryMu sync.RWMutex
	actionFactories = make(map[string]ActionFactory)
)

// RegisterAction registers an action factory by name.
// Panics if a factory with the same name already exists.
//
//	gostage.RegisterAction("my.action", func() gostage.Action {
//	    return &MyAction{}
//	})
func RegisterAction(name string, factory ActionFactory) {
	actionFactoryMu.Lock()
	defer actionFactoryMu.Unlock()

	if _, exists := actionFactories[name]; exists {
		panic(fmt.Sprintf("gostage: action factory %q already registered", name))
	}
	actionFactories[name] = factory
}

// LookupActionFactory returns the factory for the given action name, or nil.
func LookupActionFactory(name string) ActionFactory {
	actionFactoryMu.RLock()
	defer actionFactoryMu.RUnlock()
	return actionFactories[name]
}

// ResetActionFactories clears all registered action factories. Used in tests.
func ResetActionFactories() {
	actionFactoryMu.Lock()
	defer actionFactoryMu.Unlock()
	actionFactories = make(map[string]ActionFactory)
}

// registerTaskAsAction auto-bridges a TaskFunc registration to the action factory.
// This ensures every Task() call also has a corresponding action factory,
// which is critical for serializable workflow definitions (F10) and child process
// workflow reconstruction (F13).
func registerTaskAsAction(name string, fn TaskFunc, td *taskDef) {
	actionFactoryMu.Lock()
	defer actionFactoryMu.Unlock()

	// Don't overwrite explicit action registrations
	if _, exists := actionFactories[name]; exists {
		return
	}

	actionFactories[name] = func() Action {
		return &taskFuncAction{
			name:        name,
			description: td.description,
			tags:        td.tags,
			fn:          fn,
		}
	}
}

// taskFuncAction wraps a TaskFunc as an Action.
type taskFuncAction struct {
	name        string
	description string
	tags        []string
	fn          TaskFunc
}

func (a *taskFuncAction) Name() string        { return a.name }
func (a *taskFuncAction) Description() string  { return a.description }
func (a *taskFuncAction) Tags() []string       { return a.tags }
func (a *taskFuncAction) Execute(ctx *Ctx) error { return a.fn(ctx) }
