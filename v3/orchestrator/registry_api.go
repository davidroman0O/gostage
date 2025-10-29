package orchestrator

import (
	"fmt"

	"github.com/davidroman0O/gostage/v3/internal/locks"
	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/workflow"
	"github.com/google/uuid"
)

// ActionFunc is the callable executed for a registered action.
type ActionFunc func(rt.Context) error

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
	run := func(ctx rt.Context) error {
		fn := factory()
		if fn == nil {
			return fmt.Errorf("gostage: action %s factory returned nil", name)
		}
		return fn(ctx)
	}
	return registry.Default().RegisterAction(name, run, meta)
}

var (
	workflowsMu         locks.RWMutex
	workflows           = make(map[string]workflow.Definition)
	workflowAssignments = make(map[string]workflow.IDAssignment)
)

// MustRegisterWorkflow stores a workflow definition for later submission.
// It returns the workflow ID alongside the generated identifier mapping.
func MustRegisterWorkflow(def workflow.Definition) (string, workflow.IDAssignment) {
	id, assignment, err := RegisterWorkflow(def)
	if err != nil {
		panic(err)
	}
	return id, assignment
}

// RegisterWorkflow stores the workflow definition; successive registrations overwrite the previous copy.
// The returned assignment details the identifiers associated with each stage and action.
func RegisterWorkflow(def workflow.Definition) (string, workflow.IDAssignment, error) {
	id := def.ID
	if id == "" {
		id = generateWorkflowID()
	}
	clone, assignment, err := workflow.EnsureIDs(def)
	if err != nil {
		return "", workflow.IDAssignment{}, err
	}
	clone.ID = id
	workflowsMu.Lock()
	workflows[id] = clone
	workflowAssignments[id] = assignment
	workflowsMu.Unlock()
	return id, cloneAssignment(assignment), nil
}

// WorkflowRef resolves a workflow definition by ID at submission time.
func WorkflowRef(id string) WorkflowReference {
	return workflowIDRef{id: id}
}

// WorkflowDefinition wraps an inline definition as a submission reference.
func WorkflowDefinition(def workflow.Definition) WorkflowReference {
	clone, assignment, err := workflow.EnsureIDs(def)
	if err != nil {
		// Defer error propagation to submission time by storing the failure.
		return workflowErrorRef{err: err}
	}
	if clone.ID == "" {
		clone.ID = generateWorkflowID()
	}
	return workflowInlineRef{def: clone, assignment: assignment}
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
	def        workflow.Definition
	assignment workflow.IDAssignment
}

func (r workflowInlineRef) resolve() (workflow.Definition, error) {
	return r.def.Clone(), nil
}

type workflowErrorRef struct {
	err error
}

func (r workflowErrorRef) resolve() (workflow.Definition, error) {
	return workflow.Definition{}, r.err
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

// WorkflowIDs returns the identifier assignment associated with a registered workflow.
func WorkflowIDs(id string) (workflow.IDAssignment, bool) {
	workflowsMu.RLock()
	assignment, ok := workflowAssignments[id]
	workflowsMu.RUnlock()
	if !ok {
		return workflow.IDAssignment{}, false
	}
	return cloneAssignment(assignment), true
}

// WorkflowReferenceIDs attempts to resolve the identifier assignment for the provided reference.
// For registered workflows, it delegates to WorkflowIDs; for inline definitions it returns the
// mapping captured during WorkflowDefinition.
func WorkflowReferenceIDs(ref WorkflowReference) (workflow.IDAssignment, bool) {
	switch v := ref.(type) {
	case workflowIDRef:
		return WorkflowIDs(v.id)
	case workflowInlineRef:
		return cloneAssignment(v.assignment), true
	case workflowErrorRef:
		return workflow.IDAssignment{}, false
	default:
		return workflow.IDAssignment{}, false
	}
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

func generateWorkflowID() string {
	return uuid.NewString()
}

func cloneAssignment(in workflow.IDAssignment) workflow.IDAssignment {
	out := workflow.IDAssignment{Stages: make([]workflow.StageIDAssignment, len(in.Stages))}
	for i, stage := range in.Stages {
		stageCopy := stage
		if len(stage.Actions) > 0 {
			stageCopy.Actions = append([]workflow.ActionIDAssignment(nil), stage.Actions...)
		}
		out.Stages[i] = stageCopy
	}
	return out
}
