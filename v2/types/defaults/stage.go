package defaults

import (
	"time"

	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v2/types"
	deadlock "github.com/sasha-s/go-deadlock"
)

// Stage is a convenience implementation of types.Stage.
type Stage struct {
	id          string
	name        string
	description string
	tags        []string
	actions     []types.Action
	initial     *store.KVStore
	middlewares []types.StageMiddleware
	actionMW    []types.ActionMiddleware

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
	Action    types.Action
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

// NewStage builds a stage with the provided identity.
func NewStage(id, name, description string) *Stage {
	return &Stage{
		id:          id,
		name:        name,
		description: description,
		tags:        make([]string, 0),
		actions:     make([]types.Action, 0),
		initial:     store.NewKVStore(),
		middlewares: make([]types.StageMiddleware, 0),
		actionMW:    make([]types.ActionMiddleware, 0),
		runtime: stageRuntime{
			dynamicActions: make([]RuntimeActionAddition, 0),
			disabled:       make(map[string]RuntimeToggle),
			enabled:        make(map[string]RuntimeToggle),
			removed:        make(map[string]RuntimeToggle),
		},
	}
}

// AddActions appends actions to the stage definition.
func (s *Stage) AddActions(actions ...types.Action) {
	s.actions = append(s.actions, actions...)
}

// AddTags appends tags to the stage.
func (s *Stage) AddTags(tags ...string) {
	for _, tag := range tags {
		if !contains(s.tags, tag) {
			s.tags = append(s.tags, tag)
		}
	}
}

// WithMiddleware attaches stage-level middleware.
func (s *Stage) WithMiddleware(mw ...types.StageMiddleware) {
	s.middlewares = append(s.middlewares, mw...)
}

// WithActionMiddleware attaches action-level middleware executed around each action.
func (s *Stage) WithActionMiddleware(mw ...types.ActionMiddleware) {
	s.actionMW = append(s.actionMW, mw...)
}

// Use is kept for backward compatibility; prefer WithMiddleware.
// Deprecated: use WithMiddleware instead.
func (s *Stage) Use(mw ...types.StageMiddleware) {
	s.WithMiddleware(mw...)
}

// UseActionMiddleware is kept for backward compatibility; prefer WithActionMiddleware.
// Deprecated: use WithActionMiddleware instead.
func (s *Stage) UseActionMiddleware(mw ...types.ActionMiddleware) {
	s.WithActionMiddleware(mw...)
}

// SetInitialStore replaces the initial store reference.
func (s *Stage) SetInitialStore(st *store.KVStore) {
	if st == nil {
		st = store.NewKVStore()
	}
	s.initial = st
}

func (s *Stage) ID() string                   { return s.id }
func (s *Stage) Name() string                 { return s.name }
func (s *Stage) Description() string          { return s.description }
func (s *Stage) Tags() []string               { return append([]string(nil), s.tags...) }
func (s *Stage) InitialStore() *store.KVStore { return s.initial }
func (s *Stage) Middlewares() []types.StageMiddleware {
	return append([]types.StageMiddleware(nil), s.middlewares...)
}

// ActionMiddlewares returns registered action-level middleware.
func (s *Stage) ActionMiddlewares() []types.ActionMiddleware {
	return append([]types.ActionMiddleware(nil), s.actionMW...)
}

func (s *Stage) ActionList() []types.Action {
	return append([]types.Action(nil), s.actions...)
}

func (s *Stage) Actions() types.ActionMutation {
	return &actionMutation{stage: s}
}

// RuntimeState returns a snapshot of runtime-only mutations applied to the stage.
func (s *Stage) RuntimeState() StageRuntimeSnapshot {
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

// RecordDynamicAction stores metadata about a transient action inserted at runtime.
func (s *Stage) RecordDynamicAction(action types.Action, createdBy string) {
	s.runtime.mu.Lock()
	s.runtime.dynamicActions = append(s.runtime.dynamicActions, RuntimeActionAddition{
		Action:    action,
		CreatedBy: createdBy,
		Timestamp: time.Now(),
	})
	s.runtime.mu.Unlock()
}

// RecordActionDisabled notes that an action was disabled during execution.
func (s *Stage) RecordActionDisabled(id, createdBy string) {
	s.recordToggle(&s.runtime.disabled, id, createdBy)
}

// RecordActionEnabled notes that an action was re-enabled during execution.
func (s *Stage) RecordActionEnabled(id, createdBy string) {
	s.recordToggle(&s.runtime.enabled, id, createdBy)
}

// RecordActionRemoved notes that an action was removed dynamically.
func (s *Stage) RecordActionRemoved(id, createdBy string) {
	s.recordToggle(&s.runtime.removed, id, createdBy)
}

func (s *Stage) recordToggle(target *map[string]RuntimeToggle, id, createdBy string) {
	s.runtime.mu.Lock()
	if *target == nil {
		*target = make(map[string]RuntimeToggle)
	}
	(*target)[id] = RuntimeToggle{CreatedBy: createdBy, Timestamp: time.Now()}
	s.runtime.mu.Unlock()
}

// actionMutation satisfies types.ActionMutation for stage definitions.
type actionMutation struct {
	stage *Stage
}

func (m *actionMutation) Add(action types.Action) { m.stage.actions = append(m.stage.actions, action) }

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

func (m *actionMutation) Enable(string)              {}
func (m *actionMutation) EnableByTags([]string) int  { return 0 }
func (m *actionMutation) Disable(string)             {}
func (m *actionMutation) DisableByTags([]string) int { return 0 }
func (m *actionMutation) IsEnabled(string) bool      { return true }
