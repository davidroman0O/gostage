package workflow

import (
	"time"

	"github.com/davidroman0O/gostage/v3/internal/locks"
	internalstore "github.com/davidroman0O/gostage/v3/internal/store"
	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	store "github.com/davidroman0O/gostage/v3/store"
	"github.com/google/uuid"
)

const actionMutationSource = "runtime.ActionMutation"

type RuntimeWorkflow struct {
	id           string
	name         string
	description  string
	tags         []string
	stages       []rt.Stage
	store        *internalstore.KVStore
	metadata     map[string]any
	workflowType string
	payload      map[string]any
	middlewares  []rt.WorkflowMiddleware

	runtime workflowRuntime
}

type workflowRuntime struct {
	mu locks.RWMutex

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
		store:       internalstore.NewKVStore(),
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

func (w *RuntimeWorkflow) Store() store.Handle { return store.FromInternal(w.store) }

func (w *RuntimeWorkflow) InitialStore() store.Handle { return store.FromInternal(w.store) }

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

func (w *RuntimeWorkflow) DisabledSnapshot() (map[string]bool, map[string]bool) {
	if len(w.stages) == 0 {
		return nil, nil
	}
	actionDisabled := make(map[string]bool)
	stageDisabled := make(map[string]bool)
	for _, st := range w.stages {
		if rs, ok := st.(*RuntimeStage); ok {
			if snapshot := rs.disabledSnapshot(); snapshot != nil {
				for id := range snapshot {
					actionDisabled[id] = true
				}
			}
			if rs.isStageDisabled() {
				stageDisabled[rs.ID()] = true
			}
		}
	}
	if len(actionDisabled) == 0 {
		actionDisabled = nil
	}
	if len(stageDisabled) == 0 {
		stageDisabled = nil
	}
	return actionDisabled, stageDisabled
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
var _ rt.DisableSnapshotProvider = (*RuntimeWorkflow)(nil)

type RuntimeStage struct {
	id          string
	name        string
	description string
	tags        []string
	actions     []rt.Action
	initial     store.Handle
	middlewares []rt.StageMiddleware
	actionMW    []rt.ActionMiddleware
	disabled    map[string]bool

	runtime stageRuntime
}

type stageRuntime struct {
	mu locks.RWMutex

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
		initial:     store.New(),
		middlewares: make([]rt.StageMiddleware, 0),
		actionMW:    make([]rt.ActionMiddleware, 0),
		disabled:    make(map[string]bool),
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

func (s *RuntimeStage) SetInitialStore(st store.Handle) {
	if st.IsZero() {
		s.initial = store.New()
		return
	}
	s.initial = store.Clone(st)
}

func (s *RuntimeStage) ID() string                 { return s.id }
func (s *RuntimeStage) Name() string               { return s.name }
func (s *RuntimeStage) Description() string        { return s.description }
func (s *RuntimeStage) Tags() []string             { return append([]string(nil), s.tags...) }
func (s *RuntimeStage) InitialStore() store.Handle { return s.initial }
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
	id := uuid.NewString()
	action = runtimeActionWithOverrideName{Action: action, name: id}
	m.stage.actions = append(m.stage.actions, action)
	m.stage.RecordDynamicAction(action, actionMutationSource)
	return id
}

func (m *actionMutation) Remove(id string) bool {
	if id == "" {
		return false
	}
	if _, idx := m.stage.actionByID(id); idx == -1 {
		return false
	} else {
		m.stage.actions = append(m.stage.actions[:idx], m.stage.actions[idx+1:]...)
	}
	delete(m.stage.disabled, id)
	m.stage.RecordActionRemoved(id, actionMutationSource)
	return true
}

func (m *actionMutation) RemoveByTags(tags []string) int {
	removed := 0
	filtered := m.stage.actions[:0]
	for _, action := range m.stage.actions {
		if hasAny(action.Tags(), tags) {
			removed++
			delete(m.stage.disabled, action.Name())
			m.stage.RecordActionRemoved(action.Name(), actionMutationSource)
			continue
		}
		filtered = append(filtered, action)
	}
	m.stage.actions = filtered
	return removed
}

func (m *actionMutation) Enable(id string) {
	_ = m.stage.enableAction(id, actionMutationSource)
}

func (m *actionMutation) EnableByTags(tags []string) int {
	enabled := 0
	for _, action := range m.stage.actions {
		if hasAny(action.Tags(), tags) {
			if m.stage.enableAction(action.Name(), actionMutationSource) {
				enabled++
			}
		}
	}
	return enabled
}

func (m *actionMutation) Disable(id string) {
	_ = m.stage.disableAction(id, actionMutationSource)
}

func (m *actionMutation) DisableByTags(tags []string) int {
	disabled := 0
	for _, action := range m.stage.actions {
		if hasAny(action.Tags(), tags) {
			if m.stage.disableAction(action.Name(), actionMutationSource) {
				disabled++
			}
		}
	}
	return disabled
}

func (m *actionMutation) IsEnabled(id string) bool {
	return m.stage.isActionEnabled(id)
}

type runtimeActionWithOverrideName struct {
	rt.Action
	name string
}

func (a runtimeActionWithOverrideName) Name() string { return a.name }

func (a runtimeActionWithOverrideName) Ref() string {
	if withRef, ok := a.Action.(interface{ Ref() string }); ok {
		return withRef.Ref()
	}
	return ""
}

func (a runtimeActionWithOverrideName) MiddlewareChain() []rt.ActionMiddleware {
	type chainProvider interface {
		MiddlewareChain() []rt.ActionMiddleware
	}
	if provider, ok := a.Action.(chainProvider); ok {
		return provider.MiddlewareChain()
	}
	return nil
}

func (s *RuntimeStage) actionByID(id string) (rt.Action, int) {
	for idx, action := range s.actions {
		if action.Name() == id {
			return action, idx
		}
	}
	return nil, -1
}

func (s *RuntimeStage) disableAction(id, createdBy string) bool {
	if id == "" {
		return false
	}
	if _, exists := s.disabled[id]; exists {
		return false
	}
	if _, idx := s.actionByID(id); idx == -1 {
		return false
	}
	s.disabled[id] = true
	s.RecordActionDisabled(id, createdBy)
	return true
}

func (s *RuntimeStage) enableAction(id, createdBy string) bool {
	if id == "" {
		return false
	}
	if _, exists := s.disabled[id]; !exists {
		return false
	}
	delete(s.disabled, id)
	s.RecordActionEnabled(id, createdBy)
	return true
}

func (s *RuntimeStage) isActionEnabled(id string) bool {
	if id == "" {
		return false
	}
	return !s.disabled[id]
}

func (s *RuntimeStage) disabledSnapshot() map[string]bool {
	if len(s.disabled) == 0 {
		return nil
	}
	out := make(map[string]bool, len(s.disabled))
	for id, disabled := range s.disabled {
		if disabled {
			out[id] = true
		}
	}
	return out
}

func (s *RuntimeStage) isStageDisabled() bool {
	// stage-level disable tracking not yet defined; placeholder for future extension.
	return false
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
