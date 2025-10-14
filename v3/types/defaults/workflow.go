package defaults

import (
	"time"

	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v3/types"
	deadlock "github.com/sasha-s/go-deadlock"
)

// Workflow is a convenience implementation of types.Workflow.
type Workflow struct {
	id           string
	name         string
	description  string
	tags         []string
	stages       []types.Stage
	store        *store.KVStore
	metadata     map[string]interface{}
	workflowType string
	payload      map[string]interface{}
	middlewares  []types.WorkflowMiddleware

	runtime workflowRuntime
}

type workflowRuntime struct {
	mu deadlock.RWMutex

	dynamicStages []RuntimeStageAddition
	disabled      map[string]RuntimeToggle
	enabled       map[string]RuntimeToggle
	removed       map[string]RuntimeToggle
}

// RuntimeStageAddition records a transient stage inserted during execution.
type RuntimeStageAddition struct {
	Stage     types.Stage
	CreatedBy string
	Timestamp time.Time
}

// NewWorkflow builds an empty workflow definition.
func NewWorkflow(id, name, description string) *Workflow {
	return &Workflow{
		id:           id,
		name:         name,
		description:  description,
		tags:         make([]string, 0),
		stages:       make([]types.Stage, 0),
		store:        store.NewKVStore(),
		metadata:     make(map[string]interface{}),
		middlewares:  make([]types.WorkflowMiddleware, 0),
		workflowType: "",
		payload:      nil,
		runtime: workflowRuntime{
			dynamicStages: make([]RuntimeStageAddition, 0),
			disabled:      make(map[string]RuntimeToggle),
			enabled:       make(map[string]RuntimeToggle),
			removed:       make(map[string]RuntimeToggle),
		},
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

// SetType sets the logical workflow type identifier.
func (w *Workflow) SetType(workflowType string) {
	w.workflowType = workflowType
}

// Type returns the workflow type identifier.
func (w *Workflow) Type() string {
	return w.workflowType
}

// WorkflowType implements types.TypedWorkflow.
func (w *Workflow) WorkflowType() string {
	return w.Type()
}

// SetPayload replaces the workflow payload used for orchestration metadata.
func (w *Workflow) SetPayload(payload map[string]interface{}) {
	if payload == nil {
		w.payload = nil
		return
	}
	w.payload = cloneMap(payload)
}

// Payload returns a copy of the workflow payload map.
func (w *Workflow) Payload() map[string]interface{} {
	return cloneMap(w.payload)
}

// WorkflowPayload implements types.TypedWorkflow.
func (w *Workflow) WorkflowPayload() map[string]interface{} {
	return w.Payload()
}

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

// RuntimeState returns a snapshot of runtime-only mutations applied to the workflow.
func (w *Workflow) RuntimeState() WorkflowRuntimeSnapshot {
	w.runtime.mu.RLock()
	defer w.runtime.mu.RUnlock()

	dynamicCopy := make([]RuntimeStageAddition, len(w.runtime.dynamicStages))
	copy(dynamicCopy, w.runtime.dynamicStages)

	return WorkflowRuntimeSnapshot{
		DynamicStages: dynamicCopy,
		Disabled:      copyRuntimeToggleMap(w.runtime.disabled),
		Enabled:       copyRuntimeToggleMap(w.runtime.enabled),
		Removed:       copyRuntimeToggleMap(w.runtime.removed),
	}
}

// WorkflowRuntimeSnapshot captures runtime-only mutations applied to the workflow.
type WorkflowRuntimeSnapshot struct {
	DynamicStages []RuntimeStageAddition
	Disabled      map[string]RuntimeToggle
	Enabled       map[string]RuntimeToggle
	Removed       map[string]RuntimeToggle
}

// RecordDynamicStage registers a newly inserted dynamic stage.
func (w *Workflow) RecordDynamicStage(stage types.Stage, createdBy string) {
	w.runtime.mu.Lock()
	w.runtime.dynamicStages = append(w.runtime.dynamicStages, RuntimeStageAddition{
		Stage:     stage,
		CreatedBy: createdBy,
		Timestamp: time.Now(),
	})
	w.runtime.mu.Unlock()
}

// RecordStageDisabled notes that a stage was disabled during execution.
func (w *Workflow) RecordStageDisabled(stageID, createdBy string) {
	w.recordStageToggle(&w.runtime.disabled, stageID, createdBy)
}

// RecordStageEnabled notes that a stage was re-enabled during execution.
func (w *Workflow) RecordStageEnabled(stageID, createdBy string) {
	w.recordStageToggle(&w.runtime.enabled, stageID, createdBy)
}

// RecordStageRemoved notes that a stage was removed dynamically.
func (w *Workflow) RecordStageRemoved(stageID, createdBy string) {
	w.recordStageToggle(&w.runtime.removed, stageID, createdBy)
}

func (w *Workflow) recordStageToggle(target *map[string]RuntimeToggle, id, createdBy string) {
	w.runtime.mu.Lock()
	if *target == nil {
		*target = make(map[string]RuntimeToggle)
	}
	(*target)[id] = RuntimeToggle{CreatedBy: createdBy, Timestamp: time.Now()}
	w.runtime.mu.Unlock()
}
