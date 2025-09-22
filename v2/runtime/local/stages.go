package local

import "github.com/davidroman0O/gostage/v2/types"

type stageMutationContext struct {
	*actionContext
}

func newStageMutation(ctx *actionContext) types.StageActionMutation {
	return &stageMutationContext{actionContext: ctx}
}

func (s *stageMutationContext) Add(entity types.Stage) {
	s.addDynamicStage(entity)
}

func (s *stageMutationContext) Remove(entityID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, stage := range s.workflow.Stages() {
		if stage.ID() == entityID {
			s.disableStage(entityID)
			return true
		}
	}

	for i, stage := range s.dynamicStages {
		if stage.ID() == entityID {
			s.dynamicStages = append(s.dynamicStages[:i], s.dynamicStages[i+1:]...)
			return true
		}
	}
	return false
}

func (s *stageMutationContext) RemoveByTags(tags []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0

	if s.disabledStages == nil {
		s.disabledStages = make(map[string]bool)
	}

	for _, stage := range s.workflow.Stages() {
		if hasAny(stage.Tags(), tags) {
			s.disabledStages[stage.ID()] = true
			removed++
		}
	}

	filtered := s.dynamicStages[:0]
	for _, stage := range s.dynamicStages {
		if hasAny(stage.Tags(), tags) {
			removed++
			continue
		}
		filtered = append(filtered, stage)
	}
	s.dynamicStages = filtered

	return removed
}

func (s *stageMutationContext) Enable(entityID string) { s.enableStage(entityID) }

func (s *stageMutationContext) EnableByTags(tags []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.disabledStages == nil {
		return 0
	}
	enabled := 0
	for _, stage := range s.workflow.Stages() {
		if hasAny(stage.Tags(), tags) {
			if _, ok := s.disabledStages[stage.ID()]; ok {
				delete(s.disabledStages, stage.ID())
				enabled++
			}
		}
	}
	return enabled
}

func (s *stageMutationContext) Disable(entityID string) { s.disableStage(entityID) }

func (s *stageMutationContext) DisableByTags(tags []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.disabledStages == nil {
		s.disabledStages = make(map[string]bool)
	}
	disabled := 0
	for _, stage := range s.workflow.Stages() {
		if hasAny(stage.Tags(), tags) {
			if !s.disabledStages[stage.ID()] {
				s.disabledStages[stage.ID()] = true
				disabled++
			}
		}
	}
	return disabled
}

func (s *stageMutationContext) IsEnabled(entityID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.disabledStages == nil {
		return true
	}
	return !s.disabledStages[entityID]
}

func hasAny(values, targets []string) bool {
	for _, v := range values {
		for _, t := range targets {
			if v == t {
				return true
			}
		}
	}
	return false
}
