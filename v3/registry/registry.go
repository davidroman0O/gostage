package registry

import (
	"fmt"

	"github.com/davidroman0O/gostage/v3/types"
	"github.com/sasha-s/go-deadlock"
)

// ActionFactory defines the function signature used to execute a registered action.
type ActionFactory func(types.Context) error

// ActionMetadata describes a registered action.
type ActionMetadata struct {
	Description string
	Tags        []string
	Version     string
	Author      string
}

// ActionInfo exposes public information about registered actions.
type ActionInfo struct {
	ID          string
	Description string
	Tags        []string
	Version     string
	Author      string
}

// Registry is the contract implemented by action registries.
type Registry interface {
	RegisterAction(id string, factory ActionFactory, meta ActionMetadata) error
	ResolveAction(id string) (ActionFactory, ActionMetadata, error)
	HasAction(id string) bool
	ListActions() []ActionInfo
	UpdateActionMetadata(id string, fn func(*ActionMetadata)) error
}

var defaultRegistry Registry = NewSafeRegistry()
var defaultOnce deadlock.Once

// Default returns the process-wide registry used by defaults helpers.
func Default() Registry {
	defaultOnce.Do(func() {
		if defaultRegistry == nil {
			defaultRegistry = NewSafeRegistry()
		}
	})
	return defaultRegistry
}

// SetDefault overrides the process-wide registry (primarily for tests).
func SetDefault(reg Registry) {
	defaultRegistry = reg
}

type safeRegistry struct {
	mu        deadlock.RWMutex
	factories map[string]ActionFactory
	metadata  map[string]ActionMetadata
}

// NewSafeRegistry creates a thread-safe registry instance.
func NewSafeRegistry() *safeRegistry {
	return &safeRegistry{
		factories: make(map[string]ActionFactory),
		metadata:  make(map[string]ActionMetadata),
	}
}

func (r *safeRegistry) RegisterAction(id string, factory ActionFactory, meta ActionMetadata) error {
	if id == "" {
		return fmt.Errorf("registry: empty action id")
	}
	if factory == nil {
		return fmt.Errorf("registry: nil factory for %s", id)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.factories[id]; exists {
		// Ignore duplicate registrations to keep defaults ergonomic.
		return nil
	}

	// ensure tags slice is copied to prevent external mutation
	copied := append([]string(nil), meta.Tags...)
	meta.Tags = copied

	r.factories[id] = factory
	r.metadata[id] = meta
	return nil
}

func (r *safeRegistry) ResolveAction(id string) (ActionFactory, ActionMetadata, error) {
	r.mu.RLock()
	factory, exists := r.factories[id]
	meta := r.metadata[id]
	r.mu.RUnlock()

	if !exists {
		return nil, ActionMetadata{}, fmt.Errorf("registry: action %s not registered", id)
	}
	return factory, meta, nil
}

func (r *safeRegistry) HasAction(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, exists := r.factories[id]
	return exists
}

func (r *safeRegistry) ListActions() []ActionInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	infos := make([]ActionInfo, 0, len(r.factories))
	for id, meta := range r.metadata {
		infos = append(infos, ActionInfo{
			ID:          id,
			Description: meta.Description,
			Tags:        append([]string(nil), meta.Tags...),
			Version:     meta.Version,
			Author:      meta.Author,
		})
	}
	return infos
}

func (r *safeRegistry) UpdateActionMetadata(id string, fn func(*ActionMetadata)) error {
	if fn == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	meta, exists := r.metadata[id]
	if !exists {
		return fmt.Errorf("registry: action %s metadata not found", id)
	}
	fn(&meta)
	// Ensure tags slice copied to prevent sharing.
	meta.Tags = append([]string(nil), meta.Tags...)
	r.metadata[id] = meta
	return nil
}
