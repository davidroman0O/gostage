package runner

import (
	"time"

	"github.com/davidroman0O/gostage/types"
)

type actionContext struct {
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
	ctx.allActions = make([]types.Action, len(actions))
	copy(ctx.allActions, actions)
}

func (a *actionMutationContext) Add(entity types.Action) {
	a.actionContext.dynamicActions = append(a.actionContext.dynamicActions, entity)
}

func (a *actionMutationContext) Disable(entityID string) {
	if a.disabledActions == nil {
		a.disabledActions = make(map[string]bool)
	}
	a.disabledActions[entityID] = true
}

func (a *actionMutationContext) DisableByTags(tags []string) int {
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
	s.actionContext.dynamicStages = append(s.actionContext.dynamicStages, entity)
}

func (s *stageMutationContext) Remove(entityID string) bool {
	for _, stage := range s.actionContext.workflow.Stages() {
		if stage.ID() == entityID {
			// Note: We can't actually remove from the workflow.Stages() slice
			// since it's read-only. Instead, we mark it as disabled.
			s.Disable(entityID)
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
	removedCount := 0

	// Remove from workflow stages (mark as disabled)
	for _, stage := range s.actionContext.workflow.Stages() {
		for _, stageTag := range stage.Tags() {
			for _, targetTag := range tags {
				if stageTag == targetTag {
					s.Disable(stage.ID())
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
	if s.disabledStages == nil {
		s.disabledStages = make(map[string]bool)
	}
	delete(s.disabledStages, entityID)
}

func (s *stageMutationContext) EnableByTags(tags []string) int {
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
	if s.disabledStages == nil {
		s.disabledStages = make(map[string]bool)
	}
	s.disabledStages[entityID] = true
}

func (s *stageMutationContext) DisableByTags(tags []string) int {
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
	if s.disabledStages == nil {
		return true
	}
	return !s.disabledStages[entityID]
}

// contextImpl implements the types.Context interface
type contextImpl struct {
	*actionContext
	broker types.BrokerCall
	// Context fields for deadline management
	deadline    time.Time
	hasDeadline bool
	done        chan struct{}
	err         error
	values      map[interface{}]interface{}
}

// NewContext creates a new context implementation
func NewContext(workflow types.Workflow, broker types.BrokerCall) types.Context {
	actionCtx := newActionContext(workflow)
	return &contextImpl{
		actionContext: actionCtx,
		broker:        broker,
		done:          make(chan struct{}),
		values:        make(map[interface{}]interface{}),
	}
}

// Context interface methods
func (c *contextImpl) Deadline() (deadline time.Time, ok bool) {
	return c.deadline, c.hasDeadline
}

func (c *contextImpl) Done() <-chan struct{} {
	return c.done
}

func (c *contextImpl) Err() error {
	return c.err
}

func (c *contextImpl) Value(key any) any {
	return c.values[key]
}

// Workflow mutation methods
func (c *contextImpl) Stages() types.StageActionMutation {
	return newStageMutation(c.actionContext)
}

func (c *contextImpl) Actions() types.ActionMutation {
	return newActionMutation(c.actionContext)
}

func (c *contextImpl) Broker() types.BrokerCall {
	return c.broker
}

// Helper methods for context management
func (c *contextImpl) SetDeadline(deadline time.Time) {
	c.deadline = deadline
	c.hasDeadline = true
}

func (c *contextImpl) SetValue(key, value interface{}) {
	c.values[key] = value
}

func (c *contextImpl) Cancel(err error) {
	c.err = err
	close(c.done)
}
