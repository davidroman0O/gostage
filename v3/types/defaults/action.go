package defaults

import (
	"fmt"

	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/types"
)

// Action is a convenience implementation of types.Action built around the global registry.
type Action struct {
	name        string
	description string
	tags        []string
	version     string
	author      string
	reg         registry.Registry
}

// RegisterAction registers the execution factory for the given action name in the default registry.
func RegisterAction(name, description string, run func(types.Context) error, opts ...ActionOption) error {
	if run == nil {
		return fmt.Errorf("defaults: action %s requires a non-nil run function", name)
	}
	reg := registry.Default()
	if reg == nil {
		return fmt.Errorf("defaults: default registry is nil")
	}

	meta := registry.ActionMetadata{Description: description}
	for _, opt := range opts {
		opt(&meta)
	}

	if err := reg.RegisterAction(name, run, meta); err != nil {
		return err
	}
	return nil
}

// MustRegisterAction panics on registration error (useful in init functions and tests).
func MustRegisterAction(name, description string, run func(types.Context) error, opts ...ActionOption) {
	if err := RegisterAction(name, description, run, opts...); err != nil {
		panic(err)
	}
}

// ActionOption customises action metadata during registration.
type ActionOption func(meta *registry.ActionMetadata)

// WithActionTags sets the initial tag list during registration.
func WithActionTags(tags ...string) ActionOption {
	return func(meta *registry.ActionMetadata) {
		meta.Tags = append([]string(nil), tags...)
	}
}

// WithActionVersion records the version string for the action in the registry metadata.
func WithActionVersion(version string) ActionOption {
	return func(meta *registry.ActionMetadata) {
		meta.Version = version
	}
}

// WithActionAuthor records the author information for the action in the registry metadata.
func WithActionAuthor(author string) ActionOption {
	return func(meta *registry.ActionMetadata) {
		meta.Author = author
	}
}

// NewAction creates a workflow action definition referencing the registry entry.
// The action must have been registered beforehand via RegisterAction/MustRegisterAction.
func NewAction(name string) *Action {
	action, err := buildActionInstance(name)
	if err != nil {
		panic(err)
	}
	return action
}

// BuildAction constructs a concrete types.Action instance by looking up the registered
// action definition in the default registry. Unlike NewAction, this helper returns an error
// instead of panicking when the action cannot be resolved.
func BuildAction(name string) (types.Action, error) {
	return buildActionInstance(name)
}

// SetTags replaces the action tags and updates registry metadata.
func (a *Action) SetTags(tags ...string) {
	a.tags = append([]string(nil), tags...)
	if a.reg != nil {
		_ = a.reg.UpdateActionMetadata(a.name, func(meta *registry.ActionMetadata) {
			meta.Tags = append([]string(nil), tags...)
		})
	}
}

// AddTags appends unique tags and updates registry metadata.
func (a *Action) AddTags(tags ...string) {
	for _, tag := range tags {
		if !contains(a.tags, tag) {
			a.tags = append(a.tags, tag)
		}
	}
	if a.reg != nil {
		copied := append([]string(nil), a.tags...)
		_ = a.reg.UpdateActionMetadata(a.name, func(meta *registry.ActionMetadata) {
			meta.Tags = copied
		})
	}
}

func (a *Action) Name() string        { return a.name }
func (a *Action) Description() string { return a.description }
func (a *Action) Tags() []string      { return append([]string(nil), a.tags...) }

func (a *Action) Execute(ctx types.Context) error {
	if a.reg == nil {
		return fmt.Errorf("defaults: action %s missing registry reference", a.name)
	}
	factory, _, err := a.reg.ResolveAction(a.name)
	if err != nil {
		return err
	}
	return factory(ctx)
}

func buildActionInstance(name string) (*Action, error) {
	reg := registry.Default()
	if reg == nil {
		return nil, fmt.Errorf("defaults: default registry is nil")
	}
	_, meta, err := reg.ResolveAction(name)
	if err != nil {
		return nil, fmt.Errorf("defaults: action %s is not registered: %v", name, err)
	}

	return &Action{
		name:        name,
		description: meta.Description,
		tags:        append([]string(nil), meta.Tags...),
		version:     meta.Version,
		author:      meta.Author,
		reg:         reg,
	}, nil
}
