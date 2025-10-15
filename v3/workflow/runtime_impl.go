package workflow

import (
	"time"

	"github.com/davidroman0O/gostage/v3/internal/store"
	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/google/uuid"
	deadlock "github.com/sasha-s/go-deadlock"
)

type RuntimeWorkflow struct {
	id           string
	name         string
	description  string
	tags         []string
	stages       []rt.Stage
	store        *store.KVStore
	metadata     map[string]any
	workflowType string
	payload      map[string]any
	middlewares  []rt.WorkflowMiddleware

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
	Stage     rt.Stage
	CreatedBy string
	Timestamp time.Time
}

// RuntimeToggle captures metadata for runtime enable/disable/remove events.
type RuntimeToggle struct {
	CreatedBy string
	Timestamp time.Time
}

// WorkflowRuntimeSnapshot captures runtime-only mutations applied to the workflow.
type WorkflowRuntimeSnapshot struct {
	DynamicStages []RuntimeStageAddition
	Disabled      map[string]RuntimeToggle
	Enabled       map[string]RuntimeToggle
	Removed       map[string]RuntimeToggle
}

func newRuntimeWorkflow(id, name, description string) *RuntimeWorkflow {
	return &RuntimeWorkflow{
		id:          id,
		name:        name,
		description: description,
		tags:        make([]string, 0),
		stages:      make([]rt.Stage, 0),
		store:       store.NewKVStore(),
		metadata:    make(map[string]any),
		middlewares: make([]rt.WorkflowMiddleware, 0),
		runtime: workflowRuntime{
			dynamicStages: make([]RuntimeStageAddition, 0),
			disabled:      make(map[string]RuntimeToggle),
			enabled:       make(map[string]RuntimeToggle),
			removed:       make(map[string]RuntimeToggle),
		},
	}
}

// NewRuntimeWorkflow constructs an empty runtime workflow for advanced scenarios.
func NewRuntimeWorkflow(id, name, description string) *RuntimeWorkflow {
	return newRuntimeWorkflow(id, name, description)
}

func (w *RuntimeWorkflow) AddStage(stages ...rt.Stage) {
	w.stages = append(w.stages, stages...)
}

func (w *RuntimeWorkflow) AddTags(tags ...string) {
	for _, tag := range tags {
		if !contains(w.tags, tag) {
			w.tags = append(w.tags, tag)
		}
	}
}

func (w *RuntimeWorkflow) Use(mw ...rt.WorkflowMiddleware) {
	w.middlewares = append(w.middlewares, mw...)
}

func (w *RuntimeWorkflow) Metadata() map[string]any { return w.metadata }

func (w *RuntimeWorkflow) Store() *store.KVStore { return w.store }

func (w *RuntimeWorkflow) InitialStore() *store.KVStore { return w.store }

func (w *RuntimeWorkflow) ID() string          { return w.id }
func (w *RuntimeWorkflow) Name() string        { return w.name }
func (w *RuntimeWorkflow) Description() string { return w.description }
func (w *RuntimeWorkflow) Tags() []string      { return append([]string(nil), w.tags...) }
func (w *RuntimeWorkflow) Stages() []rt.Stage  { return append([]rt.Stage(nil), w.stages...) }
func (w *RuntimeWorkflow) Middlewares() []rt.WorkflowMiddleware {
	return append([]rt.WorkflowMiddleware(nil), w.middlewares...)
}

func (w *RuntimeWorkflow) RuntimeState() WorkflowRuntimeSnapshot {
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

func (w *RuntimeWorkflow) RecordDynamicStage(stage rt.Stage, createdBy string) {
	w.runtime.mu.Lock()
	w.runtime.dynamicStages = append(w.runtime.dynamicStages, RuntimeStageAddition{
		Stage:     stage,
		CreatedBy: createdBy,
		Timestamp: time.Now(),
	})
	w.runtime.mu.Unlock()
}

func (w *RuntimeWorkflow) RecordStageDisabled(stageID, createdBy string) {
	w.recordStageToggle(&w.runtime.disabled, stageID, createdBy)
}

func (w *RuntimeWorkflow) RecordStageEnabled(stageID, createdBy string) {
	w.recordStageToggle(&w.runtime.enabled, stageID, createdBy)
}

func (w *RuntimeWorkflow) RecordStageRemoved(stageID, createdBy string) {
	w.recordStageToggle(&w.runtime.removed, stageID, createdBy)
}

func (w *RuntimeWorkflow) recordStageToggle(target *map[string]RuntimeToggle, id, createdBy string) {
	w.runtime.mu.Lock()
	if *target == nil {
		*target = make(map[string]RuntimeToggle)
	}
	(*target)[id] = RuntimeToggle{CreatedBy: createdBy, Timestamp: time.Now()}
	w.runtime.mu.Unlock()
}

var _ rt.Workflow = (*RuntimeWorkflow)(nil)
var _ rt.RuntimeWorkflowRecorder = (*RuntimeWorkflow)(nil)

type RuntimeStage struct {
	id          string
	name        string
	description string
	tags        []string
	actions     []rt.Action
	initial     *store.KVStore
	middlewares []rt.StageMiddleware
	actionMW    []rt.ActionMiddleware

	runtime stageRuntime
}

type stageRuntime struct {
	mu deadlock.RWMutex

	dynamicActions []RuntimeActionAddition
	disabled       map[string]RuntimeToggle
	enabled        map[string]RuntimeToggle
	removed        map[string]RuntimeToggle
}

// RuntimeActionAddition records a transient action inserted during execution.
type RuntimeActionAddition struct {
	Action    rt.Action
	CreatedBy string
	Timestamp time.Time
}

// StageRuntimeSnapshot captures runtime-only mutations applied to the stage.
type StageRuntimeSnapshot struct {
	DynamicActions []RuntimeActionAddition
	Disabled       map[string]RuntimeToggle
	Enabled        map[string]RuntimeToggle
	Removed        map[string]RuntimeToggle
}

func newRuntimeStage(id, name, description string) *RuntimeStage {
	return &RuntimeStage{
		id:          id,
		name:        name,
		description: description,
		tags:        make([]string, 0),
		actions:     make([]rt.Action, 0),
		initial:     store.NewKVStore(),
		middlewares: make([]rt.StageMiddleware, 0),
		actionMW:    make([]rt.ActionMiddleware, 0),
		runtime: stageRuntime{
			dynamicActions: make([]RuntimeActionAddition, 0),
			disabled:       make(map[string]RuntimeToggle),
			enabled:        make(map[string]RuntimeToggle),
			removed:        make(map[string]RuntimeToggle),
		},
	}
}

// NewRuntimeStage constructs an empty runtime stage for advanced scenarios.
func NewRuntimeStage(id, name, description string) *RuntimeStage {
	return newRuntimeStage(id, name, description)
}

func (s *RuntimeStage) AddActions(actions ...rt.Action) {
	s.actions = append(s.actions, actions...)
}

func (s *RuntimeStage) AddTags(tags ...string) {
	for _, tag := range tags {
		if !contains(s.tags, tag) {
			s.tags = append(s.tags, tag)
		}
	}
}

func (s *RuntimeStage) WithMiddleware(mw ...rt.StageMiddleware) {
	s.middlewares = append(s.middlewares, mw...)
}

func (s *RuntimeStage) WithActionMiddleware(mw ...rt.ActionMiddleware) {
	s.actionMW = append(s.actionMW, mw...)
}

func (s *RuntimeStage) SetInitialStore(st *store.KVStore) {
	if st == nil {
		st = store.NewKVStore()
	}
	s.initial = st
}

func (s *RuntimeStage) ID() string                   { return s.id }
func (s *RuntimeStage) Name() string                 { return s.name }
func (s *RuntimeStage) Description() string          { return s.description }
func (s *RuntimeStage) Tags() []string               { return append([]string(nil), s.tags...) }
func (s *RuntimeStage) InitialStore() *store.KVStore { return s.initial }
func (s *RuntimeStage) Middlewares() []rt.StageMiddleware {
	return append([]rt.StageMiddleware(nil), s.middlewares...)
}

func (s *RuntimeStage) ActionMiddlewares() []rt.ActionMiddleware {
	return append([]rt.ActionMiddleware(nil), s.actionMW...)
}

func (s *RuntimeStage) ActionList() []rt.Action {
	return append([]rt.Action(nil), s.actions...)
}

func (s *RuntimeStage) Actions() rt.ActionMutation { return &actionMutation{stage: s} }

func (s *RuntimeStage) RuntimeState() StageRuntimeSnapshot {
	s.runtime.mu.RLock()
	defer s.runtime.mu.RUnlock()

	dynamicCopy := make([]RuntimeActionAddition, len(s.runtime.dynamicActions))
	copy(dynamicCopy, s.runtime.dynamicActions)

	return StageRuntimeSnapshot{
		DynamicActions: dynamicCopy,
		Disabled:       copyRuntimeToggleMap(s.runtime.disabled),
		Enabled:        copyRuntimeToggleMap(s.runtime.enabled),
		Removed:        copyRuntimeToggleMap(s.runtime.removed),
	}
}

func (s *RuntimeStage) RecordDynamicAction(action rt.Action, createdBy string) {
	s.runtime.mu.Lock()
	s.runtime.dynamicActions = append(s.runtime.dynamicActions, RuntimeActionAddition{
		Action:    action,
		CreatedBy: createdBy,
		Timestamp: time.Now(),
	})
	s.runtime.mu.Unlock()
}

func (s *RuntimeStage) RecordActionDisabled(id, createdBy string) {
	s.recordToggle(&s.runtime.disabled, id, createdBy)
}

func (s *RuntimeStage) RecordActionEnabled(id, createdBy string) {
	s.recordToggle(&s.runtime.enabled, id, createdBy)
}

func (s *RuntimeStage) RecordActionRemoved(id, createdBy string) {
	s.recordToggle(&s.runtime.removed, id, createdBy)
}

func (s *RuntimeStage) recordToggle(target *map[string]RuntimeToggle, id, createdBy string) {
	s.runtime.mu.Lock()
	if *target == nil {
		*target = make(map[string]RuntimeToggle)
	}
	(*target)[id] = RuntimeToggle{CreatedBy: createdBy, Timestamp: time.Now()}
	s.runtime.mu.Unlock()
}

var _ rt.Stage = (*RuntimeStage)(nil)
var _ rt.RuntimeStageRecorder = (*RuntimeStage)(nil)

type actionMutation struct {
	stage *RuntimeStage
}

func (m *actionMutation) Add(action rt.Action) string {
	if action == nil {
		return ""
	}
	id := action.Name()
	if id == "" {
		id = uuid.NewString()
		action = runtimeActionWithOverrideName{Action: action, name: id}
	}
	m.stage.actions = append(m.stage.actions, action)
	return id
}

func (m *actionMutation) Remove(id string) bool {
	for i, action := range m.stage.actions {
		if action.Name() == id {
			m.stage.actions = append(m.stage.actions[:i], m.stage.actions[i+1:]...)
			return true
		}
	}
	return false
}

func (m *actionMutation) RemoveByTags(tags []string) int {
	removed := 0
	filtered := m.stage.actions[:0]
	for _, action := range m.stage.actions {
		if hasAny(action.Tags(), tags) {
			removed++
			continue
		}
		filtered = append(filtered, action)
	}
	m.stage.actions = filtered
	return removed
}

func (m *actionMutation) Enable(string) {}

func (m *actionMutation) EnableByTags([]string) int { return 0 }

func (m *actionMutation) Disable(string) {}

func (m *actionMutation) DisableByTags([]string) int { return 0 }

func (m *actionMutation) IsEnabled(string) bool { return true }

type runtimeActionWithOverrideName struct {
	rt.Action
	name string
}

func (a runtimeActionWithOverrideName) Name() string { return a.name }

func (a runtimeActionWithOverrideName) MiddlewareChain() []rt.ActionMiddleware {
	type chainProvider interface {
		MiddlewareChain() []rt.ActionMiddleware
	}
	if provider, ok := a.Action.(chainProvider); ok {
		return provider.MiddlewareChain()
	}
	return nil
}

func contains(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

func hasAny(values []string, targets []string) bool {
	for _, v := range values {
		for _, t := range targets {
			if v == t {
				return true
			}
		}
	}
	return false
}

func cloneMap(src map[string]any) map[string]any {
	if src == nil {
		return nil
	}
	dup := make(map[string]any, len(src))
	for k, v := range src {
		dup[k] = v
	}
	return dup
}

func copyRuntimeToggleMap(src map[string]RuntimeToggle) map[string]RuntimeToggle {
	if src == nil {
		return nil
	}
	out := make(map[string]RuntimeToggle, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// MustRuntimeAction resolves a registered action into a runtime action, panicking on failure.
func MustRuntimeAction(ref string) rt.Action {
	action, err := resolveAction(Action{ID: ref, Ref: ref}, registry.Default())
	if err != nil {
		panic(err)
	}
	return action
}
