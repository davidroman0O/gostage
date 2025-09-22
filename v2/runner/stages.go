package runner

import "github.com/davidroman0O/gostage/v2/types"

// stageMutationContext implements the StageActionMutation interface
type stageMutationContext struct {
	*actionContext
}

func newStageMutation(ctx *actionContext) types.StageActionMutation {
	return &stageMutationContext{
		actionContext: ctx,
	}
}

func (s *stageMutationContext) Add(entity types.Stage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.actionContext.dynamicStages = append(s.actionContext.dynamicStages, entity)
}

func (s *stageMutationContext) Remove(entityID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, stage := range s.actionContext.workflow.Stages() {
		if stage.ID() == entityID {
			// Note: We can't actually remove from the workflow.Stages() slice
			// since it's read-only. Instead, we mark it as disabled.
			// Direct access to map to avoid recursive locking
			if s.disabledStages == nil {
				s.disabledStages = make(map[string]bool)
			}
			s.disabledStages[entityID] = true
			return true
		}
	}
	// Also remove from dynamic stages if present
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

	removedCount := 0

	// Remove from workflow stages (mark as disabled)
	// Direct access to map to avoid recursive locking
	if s.disabledStages == nil {
		s.disabledStages = make(map[string]bool)
	}

	for _, stage := range s.actionContext.workflow.Stages() {
		for _, stageTag := range stage.Tags() {
			for _, targetTag := range tags {
				if stageTag == targetTag {
					s.disabledStages[stage.ID()] = true
					removedCount++
					goto nextStage
				}
			}
		}
	nextStage:
	}

	// Remove from dynamicStages
	newDynamicStages := make([]types.Stage, 0, len(s.dynamicStages))
	for _, stage := range s.dynamicStages {
		hasTag := false
		for _, stageTag := range stage.Tags() {
			for _, targetTag := range tags {
				if stageTag == targetTag {
					hasTag = true
					break
				}
			}
			if hasTag {
				break
			}
		}
		if !hasTag {
			newDynamicStages = append(newDynamicStages, stage)
		}
	}
	s.dynamicStages = newDynamicStages

	return removedCount
}

func (s *stageMutationContext) Enable(entityID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.disabledStages == nil {
		s.disabledStages = make(map[string]bool)
	}
	delete(s.disabledStages, entityID)
}

func (s *stageMutationContext) EnableByTags(tags []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.disabledStages == nil {
		return 0
	}

	enabledCount := 0
	for _, stage := range s.actionContext.workflow.Stages() {
		for _, stageTag := range stage.Tags() {
			for _, targetTag := range tags {
				if stageTag == targetTag {
					stageID := stage.ID()
					if _, exists := s.disabledStages[stageID]; exists {
						delete(s.disabledStages, stageID)
						enabledCount++
					}
					goto nextStage
				}
			}
		}
	nextStage:
	}
	return enabledCount
}

func (s *stageMutationContext) Disable(entityID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.disabledStages == nil {
		s.disabledStages = make(map[string]bool)
	}
	s.disabledStages[entityID] = true
}

func (s *stageMutationContext) DisableByTags(tags []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.disabledStages == nil {
		s.disabledStages = make(map[string]bool)
	}

	disabledCount := 0
	for _, stage := range s.actionContext.workflow.Stages() {
		for _, stageTag := range stage.Tags() {
			for _, targetTag := range tags {
				if stageTag == targetTag {
					stageID := stage.ID()
					if _, exists := s.disabledStages[stageID]; !exists {
						s.disabledStages[stageID] = true
						disabledCount++
					}
					goto nextStage
				}
			}
		}
	nextStage:
	}
	return disabledCount
}

func (s *stageMutationContext) IsEnabled(entityID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.disabledStages == nil {
		return true
	}
	return !s.disabledStages[entityID]
}
