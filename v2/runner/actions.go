package runner

import (
	"github.com/davidroman0O/gostage/v2/types"
	deadlock "github.com/sasha-s/go-deadlock"
)

type actionContext struct {
	mu deadlock.RWMutex // Thread-safe access with deadlock detection

	workflow types.Workflow

	// Dynamically generated actions (will be inserted after the current action)
	dynamicActions []types.Action

	// Dynamically generated stages (will be inserted after the current stage)
	dynamicStages []types.Stage

	// Track actions to disable
	disabledActions map[string]bool

	// Track stages to disable
	disabledStages map[string]bool

	// Store all workflow actions for mutation operations
	allActions []types.Action
}

type actionMutationContext struct {
	*actionContext
}

func newActionMutation(ctx *actionContext) types.ActionMutation {
	return &actionMutationContext{
		actionContext: ctx,
	}
}

// newActionContext creates a new action context and populates allActions from the workflow
func newActionContext(workflow types.Workflow) *actionContext {
	ctx := &actionContext{
		workflow:        workflow,
		dynamicActions:  make([]types.Action, 0),
		dynamicStages:   make([]types.Stage, 0),
		disabledActions: make(map[string]bool),
		disabledStages:  make(map[string]bool),
		allActions:      make([]types.Action, 0),
	}

	// Collect all actions from all stages
	// Note: Since Stage.Actions() returns ActionMutation interface,
	// we need a way to extract actual actions. This might require
	// additional interface methods or a different approach.
	// For now, we'll leave allActions empty and rely on external initialization.

	return ctx
}

// populateActions populates the allActions slice with actions from the provided slice
// This method should be called externally when the actual actions are available
func (ctx *actionContext) populateActions(actions []types.Action) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	ctx.allActions = make([]types.Action, len(actions))
	copy(ctx.allActions, actions)
}

func (a *actionMutationContext) Add(entity types.Action) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.actionContext.dynamicActions = append(a.actionContext.dynamicActions, entity)
}

func (a *actionMutationContext) Disable(entityID string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.disabledActions == nil {
		a.disabledActions = make(map[string]bool)
	}
	a.disabledActions[entityID] = true
}

func (a *actionMutationContext) DisableByTags(tags []string) int {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.disabledActions == nil {
		a.disabledActions = make(map[string]bool)
	}

	disabledCount := 0
	// Iterate through all actions in the workflow
	for _, action := range a.allActions {
		for _, actionTag := range action.Tags() {
			for _, targetTag := range tags {
				if actionTag == targetTag {
					actionID := a.getActionID(action)
					if _, exists := a.disabledActions[actionID]; !exists {
						a.disabledActions[actionID] = true
						disabledCount++
					}
					goto nextAction
				}
			}
		}
	nextAction:
	}
	return disabledCount
}

func (a *actionMutationContext) Enable(entityID string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.disabledActions == nil {
		a.disabledActions = make(map[string]bool)
	}
	delete(a.disabledActions, entityID)
}

func (a *actionMutationContext) EnableByTags(tags []string) int {
	if a.disabledActions == nil {
		return 0
	}

	enabledCount := 0
	// Iterate through all actions in the workflow
	for _, action := range a.allActions {
		for _, actionTag := range action.Tags() {
			for _, targetTag := range tags {
				if actionTag == targetTag {
					actionID := a.getActionID(action)
					if _, exists := a.disabledActions[actionID]; exists {
						delete(a.disabledActions, actionID)
						enabledCount++
					}
					goto nextAction
				}
			}
		}
	nextAction:
	}
	return enabledCount
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
	for i, action := range a.allActions {
		if a.getActionID(action) == entityID {
			// Remove action from slice
			a.allActions = append(a.allActions[:i], a.allActions[i+1:]...)
			return true
		}
	}
	// Also remove from dynamic actions if present
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

	removedCount := 0

	// Remove from allActions
	newAllActions := make([]types.Action, 0, len(a.allActions))
	for _, action := range a.allActions {
		hasTag := false
		for _, actionTag := range action.Tags() {
			for _, targetTag := range tags {
				if actionTag == targetTag {
					hasTag = true
					removedCount++
					break
				}
			}
			if hasTag {
				break
			}
		}
		if !hasTag {
			newAllActions = append(newAllActions, action)
		}
	}
	a.allActions = newAllActions

	// Remove from dynamicActions
	newDynamicActions := make([]types.Action, 0, len(a.dynamicActions))
	for _, action := range a.dynamicActions {
		hasTag := false
		for _, actionTag := range action.Tags() {
			for _, targetTag := range tags {
				if actionTag == targetTag {
					hasTag = true
					break
				}
			}
			if hasTag {
				break
			}
		}
		if !hasTag {
			newDynamicActions = append(newDynamicActions, action)
		}
	}
	a.dynamicActions = newDynamicActions

	return removedCount
}

// getActionID generates a unique identifier for an action
// Since Action interface doesn't have an ID method, we use Name as identifier
func (a *actionMutationContext) getActionID(action types.Action) string {
	return action.Name()
}
