package gostage

import (
	"fmt"

	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/types"
	"github.com/davidroman0O/gostage/v3/workflow"
	deadlock "github.com/sasha-s/go-deadlock"
)

// ActionFunc is the callable executed for a registered action.
type ActionFunc func(types.Context) error

// ActionFactory constructs an ActionFunc instance per invocation.
type ActionFactory func() ActionFunc

// ActionOption customises metadata used when registering an action.
type ActionOption func(*registry.ActionMetadata)

// WithActionDescription sets the registry description metadata for the action.
func WithActionDescription(desc string) ActionOption {
	return func(meta *registry.ActionMetadata) {
		meta.Description = desc
	}
}

// WithActionTags sets the registry tag list for the action.
func WithActionTags(tags ...string) ActionOption {
	return func(meta *registry.ActionMetadata) {
		meta.Tags = append([]string(nil), tags...)
	}
}

// WithActionVersion sets the version metadata for the action.
func WithActionVersion(version string) ActionOption {
	return func(meta *registry.ActionMetadata) {
		meta.Version = version
	}
}

// WithActionAuthor sets the author metadata for the action.
func WithActionAuthor(author string) ActionOption {
	return func(meta *registry.ActionMetadata) {
		meta.Author = author
	}
}

// MustRegisterAction registers an action factory on the global registry and panics on error.
func MustRegisterAction(name string, factory ActionFactory, opts ...ActionOption) {
	if err := RegisterAction(name, factory, opts...); err != nil {
		panic(err)
	}
}

// RegisterAction registers an action factory on the global registry.
func RegisterAction(name string, factory ActionFactory, opts ...ActionOption) error {
	if name == "" {
		return fmt.Errorf("gostage: action name required")
	}
	if factory == nil {
		return fmt.Errorf("gostage: action %s requires factory", name)
	}
	meta := registry.ActionMetadata{}
	for _, opt := range opts {
		if opt != nil {
			opt(&meta)
		}
	}
	run := func(ctx types.Context) error {
		fn := factory()
		if fn == nil {
			return fmt.Errorf("gostage: action %s factory returned nil", name)
		}
		return fn(ctx)
	}
	return registry.Default().RegisterAction(name, run, meta)
}

var (
	workflowsMu deadlock.RWMutex
	workflows   = make(map[string]workflow.Definition)
)

// MustRegisterWorkflow stores a workflow definition for later submission.
func MustRegisterWorkflow(def workflow.Definition) {
	if err := RegisterWorkflow(def); err != nil {
		panic(err)
	}
}

// RegisterWorkflow stores the workflow definition; successive registrations overwrite the previous copy.
func RegisterWorkflow(def workflow.Definition) error {
	if def.ID == "" {
		return fmt.Errorf("gostage: workflow id required")
	}
	workflowsMu.Lock()
	workflows[def.ID] = def.Clone()
	workflowsMu.Unlock()
	return nil
}

// WorkflowRef resolves a workflow definition by ID at submission time.
func WorkflowRef(id string) WorkflowReference {
	return workflowIDRef{id: id}
}

// WorkflowDefinition wraps an inline definition as a submission reference.
func WorkflowDefinition(def workflow.Definition) WorkflowReference {
	return workflowInlineRef{def: def.Clone()}
}

// WorkflowReference resolves a workflow definition lazily during submission.
type WorkflowReference interface {
	resolve() (workflow.Definition, error)
}

type workflowIDRef struct {
	id string
}

func (r workflowIDRef) resolve() (workflow.Definition, error) {
	if r.id == "" {
		return workflow.Definition{}, fmt.Errorf("gostage: workflow id required")
	}
	workflowsMu.RLock()
	def, ok := workflows[r.id]
	workflowsMu.RUnlock()
	if !ok {
		return workflow.Definition{}, fmt.Errorf("gostage: workflow %s not registered", r.id)
	}
	return def.Clone(), nil
}

type workflowInlineRef struct {
	def workflow.Definition
}

func (r workflowInlineRef) resolve() (workflow.Definition, error) {
	if r.def.ID == "" {
		return workflow.Definition{}, fmt.Errorf("gostage: workflow id required")
	}
	return r.def.Clone(), nil
}

// ResolveWorkflow exposes registered definitions (primarily for tests).
func ResolveWorkflow(id string) (workflow.Definition, bool) {
	workflowsMu.RLock()
	def, ok := workflows[id]
	workflowsMu.RUnlock()
	if !ok {
		return workflow.Definition{}, false
	}
	return def.Clone(), true
}

// RegisteredWorkflows returns the list of workflow IDs currently registered.
func RegisteredWorkflows() []string {
	workflowsMu.RLock()
	ids := make([]string, 0, len(workflows))
	for id := range workflows {
		ids = append(ids, id)
	}
	workflowsMu.RUnlock()
	return ids
}
