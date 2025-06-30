package gostage

import "fmt"

var (
	actionRegistry = make(map[string]ActionFactory)
)

// RegisterAction registers an action factory with a unique ID.
// This function should be called at application startup for all actions
// that might be executed in a child process.
// It will panic if an action with the same ID is already registered.
func RegisterAction(id string, factory ActionFactory) {
	if _, exists := actionRegistry[id]; exists {
		panic(fmt.Sprintf("action with id '%s' is already registered", id))
	}
	actionRegistry[id] = factory
}

// NewActionFromRegistry creates a new Action instance from the registry using its ID.
// It returns an error if the action ID is not found.
func NewActionFromRegistry(id string) (Action, error) {
	factory, ok := actionRegistry[id]
	if !ok {
		return nil, fmt.Errorf("action with id '%s' not found in registry", id)
	}
	return factory(), nil
}
