package defaults

import (
	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v2/types"
)

// Workflow is a convenience implementation of types.Workflow.
type Workflow struct {
	id          string
	name        string
	description string
	tags        []string
	stages      []types.Stage
	store       *store.KVStore
	metadata    map[string]interface{}
	middlewares []types.WorkflowMiddleware
}

// NewWorkflow builds an empty workflow definition.
func NewWorkflow(id, name, description string) *Workflow {
	return &Workflow{
		id:          id,
		name:        name,
		description: description,
		tags:        make([]string, 0),
		stages:      make([]types.Stage, 0),
		store:       store.NewKVStore(),
		metadata:    make(map[string]interface{}),
		middlewares: make([]types.WorkflowMiddleware, 0),
	}
}

// AddStage appends stages to the workflow definition.
func (w *Workflow) AddStage(stages ...types.Stage) {
	w.stages = append(w.stages, stages...)
}

// AddTags appends tags to the workflow.
func (w *Workflow) AddTags(tags ...string) {
	for _, tag := range tags {
		if !contains(w.tags, tag) {
			w.tags = append(w.tags, tag)
		}
	}
}

// Use attaches workflow-level middleware.
func (w *Workflow) Use(mw ...types.WorkflowMiddleware) {
	w.middlewares = append(w.middlewares, mw...)
}

// Metadata returns the workflow metadata map (mutable).
func (w *Workflow) Metadata() map[string]interface{} { return w.metadata }

// Store returns the workflow store.
func (w *Workflow) Store() *store.KVStore { return w.store }

// InitialStore mirrors Store for compatibility.
func (w *Workflow) InitialStore() *store.KVStore { return w.store }

func (w *Workflow) ID() string            { return w.id }
func (w *Workflow) Name() string          { return w.name }
func (w *Workflow) Description() string   { return w.description }
func (w *Workflow) Tags() []string        { return append([]string(nil), w.tags...) }
func (w *Workflow) Stages() []types.Stage { return append([]types.Stage(nil), w.stages...) }
func (w *Workflow) Middlewares() []types.WorkflowMiddleware {
	return append([]types.WorkflowMiddleware(nil), w.middlewares...)
}
