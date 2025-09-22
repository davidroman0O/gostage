package local

import (
	"github.com/davidroman0O/gostage/v2/types"
	deadlock "github.com/sasha-s/go-deadlock"
)

type actionContext struct {
	mu deadlock.RWMutex

	workflow types.Workflow
	logger   types.Logger

	currentStage  types.Stage
	currentAction types.Action
	actionIndex   int
	isLastAction  bool

	dynamicActions []types.Action
	dynamicStages  []types.Stage

	disabledActions map[string]bool
	disabledStages  map[string]bool

	allActions []types.Action
}

func newActionContext(workflow types.Workflow) *actionContext {
	return &actionContext{
		workflow:        workflow,
		dynamicActions:  make([]types.Action, 0),
		dynamicStages:   make([]types.Stage, 0),
		disabledActions: make(map[string]bool),
		disabledStages:  make(map[string]bool),
		allActions:      make([]types.Action, 0),
	}
}

func (ctx *actionContext) setWorkflow(workflow types.Workflow) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.workflow = workflow
}

func (ctx *actionContext) setStage(stage types.Stage) {
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

func (ctx *actionContext) refreshActionsLocked(stage types.Stage) {
	if stage == nil {
		ctx.allActions = nil
		return
	}
	actions := stage.ActionList()
	ctx.allActions = make([]types.Action, len(actions))
	copy(ctx.allActions, actions)
}

func (ctx *actionContext) setAction(action types.Action, index int, isLast bool) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.currentAction = action
	ctx.actionIndex = index
	ctx.isLastAction = isLast
}

func (ctx *actionContext) setLogger(logger types.Logger) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.logger = logger
}

func (ctx *actionContext) getLogger() types.Logger {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.logger
}

func (ctx *actionContext) getStage() types.Stage {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.currentStage
}

func (ctx *actionContext) getAction() (types.Action, int, bool) {
	ctx.mu.RLock()
	defer ctx.mu.RUnlock()
	return ctx.currentAction, ctx.actionIndex, ctx.isLastAction
}

func (ctx *actionContext) consumeDynamicActions() []types.Action {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	actions := ctx.dynamicActions
	ctx.dynamicActions = nil
	return actions
}

func (ctx *actionContext) consumeDynamicStages() []types.Stage {
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

func (ctx *actionContext) populateActions(actions []types.Action) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	ctx.allActions = make([]types.Action, len(actions))
	copy(ctx.allActions, actions)
}

func (ctx *actionContext) addDynamicAction(action types.Action) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.dynamicActions = append(ctx.dynamicActions, action)
}

func (ctx *actionContext) addDynamicStage(stage types.Stage) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.dynamicStages = append(ctx.dynamicStages, stage)
}

func (ctx *actionContext) disableAction(id string) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.disabledActions == nil {
		ctx.disabledActions = make(map[string]bool)
	}
	ctx.disabledActions[id] = true
}

func (ctx *actionContext) enableAction(id string) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.disabledActions == nil {
		return
	}
	delete(ctx.disabledActions, id)
}

func (ctx *actionContext) disableStage(id string) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.disabledStages == nil {
		ctx.disabledStages = make(map[string]bool)
	}
	ctx.disabledStages[id] = true
}

func (ctx *actionContext) enableStage(id string) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.disabledStages == nil {
		return
	}
	delete(ctx.disabledStages, id)
}
