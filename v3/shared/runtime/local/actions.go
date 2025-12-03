// Package local provides local execution context implementation for workflow actions.
package local

import (
	"github.com/davidroman0O/gostage/v3/internal/foundation/locks"
	rt "github.com/davidroman0O/gostage/v3/shared/runtime"
)

const globalStageID = "*"

type actionContext struct {
	mu locks.RWMutex

	workflow rt.Workflow
	logger   rt.Logger
	broker   rt.Broker

	currentStage  rt.Stage
	currentAction rt.Action
	actionIndex   int
	isLastAction  bool

	dynamicActions []rt.Action
	dynamicStages  []rt.Stage

	disabledActions map[string]bool
	disabledStages  map[string]bool
	removedActions  map[string]map[string]string // stageID -> actionName -> createdBy
	removedStages   map[string]string            // stageID -> createdBy

	allActions []rt.Action
}

func newActionContext(workflow rt.Workflow) *actionContext {
	return &actionContext{
		workflow:        workflow,
		dynamicActions:  make([]rt.Action, 0),
		dynamicStages:   make([]rt.Stage, 0),
		disabledActions: make(map[string]bool),
		disabledStages:  make(map[string]bool),
		removedActions:  make(map[string]map[string]string),
		removedStages:   make(map[string]string),
		allActions:      make([]rt.Action, 0),
	}
}

func (ctx *actionContext) setBroker(b rt.Broker) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.broker = b
}

func (ctx *actionContext) setStage(stage rt.Stage) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.currentStage = stage
	ctx.refreshActionsLocked(stage)
}

func (ctx *actionContext) clearStage() {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.currentStage = nil
	ctx.currentAction = nil
	ctx.actionIndex = 0
	ctx.isLastAction = false
	ctx.dynamicActions = nil
	ctx.dynamicStages = nil
	ctx.allActions = nil
}

func (ctx *actionContext) refreshActionsLocked(stage rt.Stage) {
	if stage == nil {
		ctx.allActions = nil
		return
	}
	actions := stage.ActionList()
	ctx.allActions = make([]rt.Action, len(actions))
	copy(ctx.allActions, actions)
}

func (ctx *actionContext) setAction(action rt.Action, index int, isLast bool) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.currentAction = action
	ctx.actionIndex = index
	ctx.isLastAction = isLast
}

func (ctx *actionContext) setLogger(logger rt.Logger) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.logger = logger
}

func (ctx *actionContext) getLogger() rt.Logger {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.logger
}

func (ctx *actionContext) getStage() rt.Stage {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.currentStage
}

func (ctx *actionContext) getAction() (rt.Action, int, bool) {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.currentAction, ctx.actionIndex, ctx.isLastAction
}

func (ctx *actionContext) consumeDynamicActions() []rt.Action {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	actions := ctx.dynamicActions
	ctx.dynamicActions = nil
	return actions
}

func (ctx *actionContext) consumeDynamicStages() []rt.Stage {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	stages := ctx.dynamicStages
	ctx.dynamicStages = nil
	return stages
}

func (ctx *actionContext) setDisabledMaps(actions, stages map[string]bool) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if actions != nil {
		ctx.disabledActions = actions
	}
	if stages != nil {
		ctx.disabledStages = stages
	}
}

func (ctx *actionContext) disabledMaps() (map[string]bool, map[string]bool) {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.disabledActions, ctx.disabledStages
}

func (ctx *actionContext) populateActions(actions []rt.Action) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	ctx.allActions = make([]rt.Action, len(actions))
	copy(ctx.allActions, actions)
}

func (ctx *actionContext) addDynamicAction(action rt.Action) {
	ctx.mu.Lock()
	stage := ctx.currentStage
	currentAction := ctx.currentAction
	ctx.dynamicActions = append(ctx.dynamicActions, action)
	ctx.mu.Unlock()

	if recorder, ok := stage.(rt.RuntimeStageRecorder); ok {
		recorder.RecordDynamicAction(action, mutationSource(stage, currentAction))
	}
}

func (ctx *actionContext) addDynamicStage(stage rt.Stage) {
	ctx.mu.Lock()
	currentStage := ctx.currentStage
	currentAction := ctx.currentAction
	workflow := ctx.workflow
	ctx.dynamicStages = append(ctx.dynamicStages, stage)
	ctx.mu.Unlock()

	if recorder, ok := workflow.(rt.RuntimeWorkflowRecorder); ok {
		recorder.RecordDynamicStage(stage, mutationSource(currentStage, currentAction))
	}
}

func (ctx *actionContext) disableAction(id string) bool {
	ctx.mu.Lock()
	if ctx.disabledActions == nil {
		ctx.disabledActions = make(map[string]bool)
	}
	if ctx.disabledActions[id] {
		ctx.mu.Unlock()
		return false
	}
	ctx.disabledActions[id] = true
	stage := ctx.currentStage
	currentAction := ctx.currentAction
	ctx.mu.Unlock()

	if recorder, ok := stage.(rt.RuntimeStageRecorder); ok {
		recorder.RecordActionDisabled(id, mutationSource(stage, currentAction))
	}
	return true
}

func (ctx *actionContext) enableAction(id string) bool {
	ctx.mu.Lock()
	if ctx.disabledActions == nil {
		ctx.mu.Unlock()
		return false
	}
	if !ctx.disabledActions[id] {
		ctx.mu.Unlock()
		return false
	}
	delete(ctx.disabledActions, id)
	stage := ctx.currentStage
	currentAction := ctx.currentAction
	ctx.mu.Unlock()

	if recorder, ok := stage.(rt.RuntimeStageRecorder); ok {
		recorder.RecordActionEnabled(id, mutationSource(stage, currentAction))
	}
	return true
}

func (ctx *actionContext) disableStage(id string) bool {
	ctx.mu.Lock()
	if ctx.disabledStages == nil {
		ctx.disabledStages = make(map[string]bool)
	}
	if ctx.disabledStages[id] {
		ctx.mu.Unlock()
		return false
	}
	ctx.disabledStages[id] = true
	workflow := ctx.workflow
	currentStage := ctx.currentStage
	currentAction := ctx.currentAction
	ctx.mu.Unlock()

	if recorder, ok := workflow.(rt.RuntimeWorkflowRecorder); ok {
		recorder.RecordStageDisabled(id, mutationSource(currentStage, currentAction))
	}
	return true
}

func (ctx *actionContext) enableStage(id string) bool {
	ctx.mu.Lock()
	if ctx.disabledStages == nil {
		ctx.mu.Unlock()
		return false
	}
	if !ctx.disabledStages[id] {
		ctx.mu.Unlock()
		return false
	}
	delete(ctx.disabledStages, id)
	workflow := ctx.workflow
	currentStage := ctx.currentStage
	currentAction := ctx.currentAction
	ctx.mu.Unlock()

	if recorder, ok := workflow.(rt.RuntimeWorkflowRecorder); ok {
		recorder.RecordStageEnabled(id, mutationSource(currentStage, currentAction))
	}
	return true
}

func (ctx *actionContext) markActionRemoved(stageID, actionName, createdBy string) {
	if stageID == "" {
		stageID = globalStageID
	}
	ctx.mu.Lock()
	if ctx.removedActions == nil {
		ctx.removedActions = make(map[string]map[string]string)
	}
	if ctx.removedActions[stageID] == nil {
		ctx.removedActions[stageID] = make(map[string]string)
	}
	ctx.removedActions[stageID][actionName] = createdBy
	ctx.mu.Unlock()
}

func (ctx *actionContext) consumeRemovedAction(stageID, actionName string) (bool, string) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	stageMap, ok := ctx.removedActions[stageID]
	if !ok {
		if stageID != globalStageID {
			if globalMap, exists := ctx.removedActions[globalStageID]; exists {
				if createdBy, found := globalMap[actionName]; found {
					delete(globalMap, actionName)
					if len(globalMap) == 0 {
						delete(ctx.removedActions, globalStageID)
					}
					return true, createdBy
				}
			}
		}
		return false, ""
	}
	createdBy, exists := stageMap[actionName]
	if !exists {
		return false, ""
	}
	delete(stageMap, actionName)
	if len(stageMap) == 0 {
		delete(ctx.removedActions, stageID)
	}
	return true, createdBy
}

func (ctx *actionContext) markStageRemoved(id, createdBy string) {
	ctx.mu.Lock()
	if ctx.removedStages == nil {
		ctx.removedStages = make(map[string]string)
	}
	ctx.removedStages[id] = createdBy
	ctx.mu.Unlock()
}

func (ctx *actionContext) consumeRemovedStages() map[string]string {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if len(ctx.removedStages) == 0 {
		return nil
	}
	result := make(map[string]string, len(ctx.removedStages))
	for id, createdBy := range ctx.removedStages {
		result[id] = createdBy
	}
	ctx.removedStages = make(map[string]string)
	return result
}

func mutationSource(stage rt.Stage, action rt.Action) string {
	var stageID, actionName string
	if stage != nil {
		stageID = stage.ID()
	}
	if action != nil {
		actionName = action.Name()
	}
	if stageID == "" && actionName == "" {
		return ""
	}
	if actionName == "" {
		return stageID
	}
	if stageID == "" {
		return actionName
	}
	return stageID + "::" + actionName
}
