package defaults

import (
	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v2/types"
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

// Use attaches stage-level middleware.
func (s *Stage) Use(mw ...types.StageMiddleware) {
	s.middlewares = append(s.middlewares, mw...)
}

// UseActionMiddleware attaches action-level middleware executed around each action.
func (s *Stage) UseActionMiddleware(mw ...types.ActionMiddleware) {
	s.actionMW = append(s.actionMW, mw...)
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
