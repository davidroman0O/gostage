package local

import "github.com/davidroman0O/gostage/v2/types"

type actionMutationContext struct {
	*actionContext
}

func newActionMutation(ctx *actionContext) types.ActionMutation {
	return &actionMutationContext{actionContext: ctx}
}

func (a *actionMutationContext) Add(entity types.Action) { a.addDynamicAction(entity) }

func (a *actionMutationContext) Disable(entityID string) { a.disableAction(entityID) }

func (a *actionMutationContext) DisableByTags(tags []string) int {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.disabledActions == nil {
		a.disabledActions = make(map[string]bool)
	}

	disabled := 0
	for _, action := range a.allActions {
		if hasAny(action.Tags(), tags) {
			id := a.getActionID(action)
			if !a.disabledActions[id] {
				a.disabledActions[id] = true
				disabled++
			}
		}
	}
	return disabled
}

func (a *actionMutationContext) Enable(entityID string) { a.enableAction(entityID) }

func (a *actionMutationContext) EnableByTags(tags []string) int {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.disabledActions == nil {
		return 0
	}
	enabled := 0
	for _, action := range a.allActions {
		if hasAny(action.Tags(), tags) {
			id := a.getActionID(action)
			if a.disabledActions[id] {
				delete(a.disabledActions, id)
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
	a.mu.Lock()
	defer a.mu.Unlock()

	for i, action := range a.allActions {
		if a.getActionID(action) == entityID {
			a.allActions = append(a.allActions[:i], a.allActions[i+1:]...)
			return true
		}
	}

	for i, action := range a.dynamicActions {
		if a.getActionID(action) == entityID {
			a.dynamicActions = append(a.dynamicActions[:i], a.dynamicActions[i+1:]...)
			return true
		}
	}
	return false
}

func (a *actionMutationContext) RemoveByTags(tags []string) int {
	a.mu.Lock()
	defer a.mu.Unlock()

	removed := 0

	filteredActions := a.allActions[:0]
	for _, action := range a.allActions {
		if hasAny(action.Tags(), tags) {
			removed++
			continue
		}
		filteredActions = append(filteredActions, action)
	}
	a.allActions = filteredActions

	filteredDynamic := a.dynamicActions[:0]
	for _, action := range a.dynamicActions {
		if hasAny(action.Tags(), tags) {
			removed++
			continue
		}
		filteredDynamic = append(filteredDynamic, action)
	}
	a.dynamicActions = filteredDynamic

	return removed
}

func (a *actionMutationContext) getActionID(action types.Action) string {
	return action.Name()
}
