package workflow

import (
	"fmt"
	"slices"

	"github.com/sasha-s/go-deadlock"
)

// ActionFactory constructs action functions for execution.
type ActionFactory func() ActionFunc

// ActionMetadata describes a registered action.
type ActionMetadata struct {
	Description string
	Tags        []string
	Version     string
	Author      string
}

// Registry stores action factories keyed by ID.
type Registry interface {
	Register(id string, factory ActionFactory, meta ActionMetadata) error
	MustRegister(id string, factory ActionFactory, meta ActionMetadata)
	Resolve(id string) (ActionFactory, ActionMetadata, error)
	List() []RegisteredAction
}

// RegisteredAction exposes registry metadata for inspection.
type RegisteredAction struct {
	ID          string
	Description string
	Tags        []string
	Version     string
	Author      string
}

type defaultRegistry struct {
	mu        deadlock.RWMutex
	factories map[string]ActionFactory
	metadata  map[string]ActionMetadata
}

var (
	global     Registry = NewRegistry()
	globalOnce deadlock.Once
)

// Default returns the process-wide registry.
func Default() Registry {
	globalOnce.Do(func() {
		if global == nil {
			global = NewRegistry()
		}
	})
	return global
}

// SetDefault swaps the global registry (useful for tests).
func SetDefault(reg Registry) {
	global = reg
}

// NewRegistry creates a thread-safe registry.
func NewRegistry() Registry {
	return &defaultRegistry{
		factories: make(map[string]ActionFactory),
		metadata:  make(map[string]ActionMetadata),
	}
}

func (r *defaultRegistry) Register(id string, factory ActionFactory, meta ActionMetadata) error {
	if id == "" {
		return fmt.Errorf("workflow: action id required")
	}
	if factory == nil {
		return fmt.Errorf("workflow: action %s has nil factory", id)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.factories[id]; exists {
		r.factories[id] = factory
		r.metadata[id] = meta
		return nil
	}
	r.factories[id] = factory
	meta.Tags = append([]string(nil), meta.Tags...)
	slices.Sort(meta.Tags)
	r.metadata[id] = meta
	return nil
}

func (r *defaultRegistry) MustRegister(id string, factory ActionFactory, meta ActionMetadata) {
	if err := r.Register(id, factory, meta); err != nil {
		panic(err)
	}
}

func (r *defaultRegistry) Resolve(id string) (ActionFactory, ActionMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.factories[id]
	if !ok {
		return nil, ActionMetadata{}, fmt.Errorf("workflow: action %s not registered", id)
	}
	meta := r.metadata[id]
	meta.Tags = append([]string(nil), meta.Tags...)
	return factory, meta, nil
}

func (r *defaultRegistry) List() []RegisteredAction {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RegisteredAction, 0, len(r.factories))
	for id, meta := range r.metadata {
		out = append(out, RegisteredAction{
			ID:          id,
			Description: meta.Description,
			Tags:        append([]string(nil), meta.Tags...),
			Version:     meta.Version,
			Author:      meta.Author,
		})
	}
	slices.SortFunc(out, func(a, b RegisteredAction) int {
		return cmpString(a.ID, b.ID)
	})
	return out
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// Exported helpers ---------------------------------------------------------

// RegisterAction registers an action on the default registry.
func RegisterAction(id string, factory ActionFactory, meta ActionMetadata) error {
	return Default().Register(id, factory, meta)
}

// MustRegisterAction panics on failure and is convenient for init-time registrations.
func MustRegisterAction(id string, factory ActionFactory, meta ActionMetadata) {
	Default().MustRegister(id, factory, meta)
}

// ResolveAction looks up an action factory by ID.
func ResolveAction(id string) (ActionFactory, ActionMetadata, error) {
	return Default().Resolve(id)
}

// ListRegisteredActions returns registry metadata for inspection.
func ListRegisteredActions() []RegisteredAction {
	return Default().List()
}
