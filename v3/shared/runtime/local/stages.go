package local

import (
	"github.com/davidroman0O/gostage/v3/internal/foundation/id"
	rt "github.com/davidroman0O/gostage/v3/shared/runtime"
)

type stageMutationContext struct {
	*actionContext
}

func newStageMutation(ctx *actionContext) rt.StageMutation {
	return &stageMutationContext{actionContext: ctx}
}

func (s *stageMutationContext) Add(entity rt.Stage) string {
	if entity == nil {
		return ""
	}
	id := id.DefaultGenerator().GenerateWithPrefix("stage")
	entity = stageWithOverrideID{Stage: entity, id: id}
	s.addDynamicStage(entity)
	return id
}

func (s *stageMutationContext) Remove(entityID string) bool {
	s.mu.Lock()
	for _, stage := range s.workflow.Stages() {
		if stage.ID() == entityID {
			s.mu.Unlock()
			s.disableStage(entityID)
			return true
		}
	}

	for i, stage := range s.dynamicStages {
		if stage.ID() == entityID {
			s.dynamicStages = append(s.dynamicStages[:i], s.dynamicStages[i+1:]...)
			workflow := s.workflow
			currentStage := s.currentStage
			currentAction := s.currentAction
			s.mu.Unlock()
			createdBy := mutationSource(currentStage, currentAction)
			if recorder, ok := workflow.(rt.RuntimeWorkflowRecorder); ok {
				recorder.RecordStageRemoved(entityID, createdBy)
			}
			s.markStageRemoved(entityID, createdBy)
			return true
		}
	}
	s.mu.Unlock()
	return false
}

func (s *stageMutationContext) RemoveByTags(tags []string) int {
	removed := 0
	for _, stage := range s.workflow.Stages() {
		if hasAny(stage.Tags(), tags) {
			if s.disableStage(stage.ID()) {
				removed++
			}
		}
	}

	s.mu.Lock()
	filtered := s.dynamicStages[:0]
	removedIDs := make([]string, 0)
	for _, stage := range s.dynamicStages {
		if hasAny(stage.Tags(), tags) {
			removedIDs = append(removedIDs, stage.ID())
			continue
		}
		filtered = append(filtered, stage)
	}
	s.dynamicStages = filtered
	workflow := s.workflow
	currentStage := s.currentStage
	currentAction := s.currentAction
	s.mu.Unlock()

	for _, id := range removedIDs {
		createdBy := mutationSource(currentStage, currentAction)
		if recorder, ok := workflow.(rt.RuntimeWorkflowRecorder); ok {
			recorder.RecordStageRemoved(id, createdBy)
		}
		s.markStageRemoved(id, createdBy)
	}

	return removed + len(removedIDs)
}

func (s *stageMutationContext) Enable(entityID string) { s.enableStage(entityID) }

func (s *stageMutationContext) EnableByTags(tags []string) int {
	enabled := 0
	for _, stage := range s.workflow.Stages() {
		if hasAny(stage.Tags(), tags) {
			if s.enableStage(stage.ID()) {
				enabled++
			}
		}
	}
	return enabled
}

func (s *stageMutationContext) Disable(entityID string) { s.disableStage(entityID) }

func (s *stageMutationContext) DisableByTags(tags []string) int {
	disabled := 0
	for _, stage := range s.workflow.Stages() {
		if hasAny(stage.Tags(), tags) {
			if s.disableStage(stage.ID()) {
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

type stageWithOverrideID struct {
	rt.Stage
	id string
}

func (s stageWithOverrideID) ID() string { return s.id }

func (s stageWithOverrideID) RecordDynamicAction(action rt.Action, createdBy string) {
	if recorder, ok := s.Stage.(rt.RuntimeStageRecorder); ok {
		recorder.RecordDynamicAction(action, createdBy)
	}
}

func (s stageWithOverrideID) RecordActionDisabled(id, createdBy string) {
	if recorder, ok := s.Stage.(rt.RuntimeStageRecorder); ok {
		recorder.RecordActionDisabled(id, createdBy)
	}
}

func (s stageWithOverrideID) RecordActionEnabled(id, createdBy string) {
	if recorder, ok := s.Stage.(rt.RuntimeStageRecorder); ok {
		recorder.RecordActionEnabled(id, createdBy)
	}
}

func (s stageWithOverrideID) RecordActionRemoved(id, createdBy string) {
	if recorder, ok := s.Stage.(rt.RuntimeStageRecorder); ok {
		recorder.RecordActionRemoved(id, createdBy)
	}
}
