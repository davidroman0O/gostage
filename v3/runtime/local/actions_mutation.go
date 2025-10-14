package local

import "github.com/davidroman0O/gostage/v3/types"

type actionMutationContext struct {
	*actionContext
}

func newActionMutation(ctx *actionContext) types.ActionMutation {
	return &actionMutationContext{actionContext: ctx}
}

func (a *actionMutationContext) Add(entity types.Action) { a.addDynamicAction(entity) }

func (a *actionMutationContext) Disable(entityID string) { a.disableAction(entityID) }

func (a *actionMutationContext) DisableByTags(tags []string) int {
	disabled := 0
	for _, action := range a.snapshotAllActions() {
		if hasAny(action.Tags(), tags) {
			if a.disableAction(a.getActionID(action)) {
				disabled++
			}
		}
	}
	return disabled
}

func (a *actionMutationContext) Enable(entityID string) { a.enableAction(entityID) }

func (a *actionMutationContext) EnableByTags(tags []string) int {
	enabled := 0
	for _, action := range a.snapshotAllActions() {
		if hasAny(action.Tags(), tags) {
			if a.enableAction(a.getActionID(action)) {
				enabled++
			}
		}
	}
	return enabled
}

func (a *actionMutationContext) IsEnabled(entityID string) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.disabledActions == nil {
		return true
	}
	return !a.disabledActions[entityID]
}

func (a *actionMutationContext) Remove(entityID string) bool {
	removedFromStatic, removedFromDynamic := false, false
	a.mu.Lock()
	for i, action := range a.allActions {
		if a.getActionID(action) == entityID {
			a.allActions = append(a.allActions[:i], a.allActions[i+1:]...)
			removedFromStatic = true
			break
		}
	}
	if !removedFromStatic {
		for i, action := range a.dynamicActions {
			if a.getActionID(action) == entityID {
				a.dynamicActions = append(a.dynamicActions[:i], a.dynamicActions[i+1:]...)
				removedFromDynamic = true
				break
			}
		}
	}
	stage := a.currentStage
	currentAction := a.currentAction
	a.mu.Unlock()

	if removedFromStatic || removedFromDynamic {
		createdBy := mutationSource(stage, currentAction)
		if recorder, ok := stage.(types.RuntimeStageRecorder); ok {
			recorder.RecordActionRemoved(entityID, createdBy)
		}
		stageID := ""
		if stage != nil {
			stageID = stage.ID()
		}
		a.actionContext.markActionRemoved(stageID, entityID, createdBy)
		_ = a.disableAction(entityID)
		return true
	}
	return false
}

func (a *actionMutationContext) RemoveByTags(tags []string) int {
	removedIDs := make([]string, 0)
	a.mu.Lock()
	filteredActions := a.allActions[:0]
	for _, action := range a.allActions {
		if hasAny(action.Tags(), tags) {
			removedIDs = append(removedIDs, a.getActionID(action))
			continue
		}
		filteredActions = append(filteredActions, action)
	}
	a.allActions = filteredActions

	filteredDynamic := a.dynamicActions[:0]
	for _, action := range a.dynamicActions {
		if hasAny(action.Tags(), tags) {
			removedIDs = append(removedIDs, a.getActionID(action))
			continue
		}
		filteredDynamic = append(filteredDynamic, action)
	}
	a.dynamicActions = filteredDynamic
	stage := a.currentStage
	currentAction := a.currentAction
	a.mu.Unlock()

	for _, id := range removedIDs {
		createdBy := mutationSource(stage, currentAction)
		if recorder, ok := stage.(types.RuntimeStageRecorder); ok {
			recorder.RecordActionRemoved(id, createdBy)
		}
		stageID := ""
		if stage != nil {
			stageID = stage.ID()
		}
		a.actionContext.markActionRemoved(stageID, id, createdBy)
		_ = a.disableAction(id)
	}

	return len(removedIDs)
}

func (a *actionMutationContext) getActionID(action types.Action) string {
	return action.Name()
}

func (a *actionMutationContext) snapshotAllActions() []types.Action {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.allActions) == 0 {
		return nil
	}
	copyList := make([]types.Action, len(a.allActions))
	copy(copyList, a.allActions)
	return copyList
}
