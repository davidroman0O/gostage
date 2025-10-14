package local

import (
	"time"

	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v3/runtime/core"
	"github.com/davidroman0O/gostage/v3/types"
	deadlock "github.com/sasha-s/go-deadlock"
)

// Factory builds execution contexts for the local runner backend.
type Factory struct{}

// New creates a new execution context for the provided workflow.
func (Factory) New(workflow types.Workflow, broker types.BrokerCall) core.ExecutionContext {
	actionCtx := newActionContext(workflow)
	actionCtx.setBroker(broker)
	return &contextImpl{
		actionContext: actionCtx,
		broker:        broker,
		done:          make(chan struct{}),
		values:        make(map[interface{}]interface{}),
	}
}

var _ core.ExecutionContext = (*contextImpl)(nil)

// contextImpl expands the standard execution context with local state management hooks.
type contextImpl struct {
	*actionContext
	broker types.BrokerCall

	valueMu deadlock.RWMutex

	deadline    time.Time
	hasDeadline bool
	done        chan struct{}
	err         error
	values      map[interface{}]interface{}
}

func (c *contextImpl) Deadline() (deadline time.Time, ok bool) {
	c.valueMu.RLock()
	defer c.valueMu.RUnlock()
	return c.deadline, c.hasDeadline
}

func (c *contextImpl) Done() <-chan struct{} { return c.done }

func (c *contextImpl) Err() error {
	c.valueMu.RLock()
	defer c.valueMu.RUnlock()
	return c.err
}

func (c *contextImpl) Value(key any) any {
	c.valueMu.RLock()
	defer c.valueMu.RUnlock()
	return c.values[key]
}

func (c *contextImpl) Stages() types.StageActionMutation { return newStageMutation(c.actionContext) }
func (c *contextImpl) Actions() types.ActionMutation     { return newActionMutation(c.actionContext) }
func (c *contextImpl) Broker() types.BrokerCall          { return c.broker }
func (c *contextImpl) Workflow() types.Workflow          { return c.actionContext.workflow }
func (c *contextImpl) Stage() types.Stage                { return c.actionContext.getStage() }
func (c *contextImpl) Action() types.Action {
	action, _, _ := c.actionContext.getAction()
	return action
}
func (c *contextImpl) ActionIndex() int {
	_, idx, _ := c.actionContext.getAction()
	return idx
}
func (c *contextImpl) IsLastAction() bool {
	_, _, last := c.actionContext.getAction()
	return last
}
func (c *contextImpl) Store() *store.KVStore {
	if c.actionContext.workflow == nil {
		return nil
	}
	return c.actionContext.workflow.Store()
}
func (c *contextImpl) Logger() types.Logger { return c.actionContext.getLogger() }

func (c *contextImpl) SetLogger(logger types.Logger) { c.actionContext.setLogger(logger) }
func (c *contextImpl) SetStage(stage types.Stage)    { c.actionContext.setStage(stage) }
func (c *contextImpl) ClearStage()                   { c.actionContext.clearStage() }
func (c *contextImpl) SetAction(action types.Action, index int, isLast bool) {
	c.actionContext.setAction(action, index, isLast)
}
func (c *contextImpl) ConsumeDynamicActions() []types.Action {
	return c.actionContext.consumeDynamicActions()
}
func (c *contextImpl) ConsumeDynamicStages() []types.Stage {
	return c.actionContext.consumeDynamicStages()
}
func (c *contextImpl) SetActionList(actions []types.Action) { c.actionContext.populateActions(actions) }
func (c *contextImpl) SetDisabledMaps(actions, stages map[string]bool) {
	c.actionContext.setDisabledMaps(actions, stages)
}
func (c *contextImpl) DisabledMaps() (map[string]bool, map[string]bool) {
	return c.actionContext.disabledMaps()
}

func (c *contextImpl) ConsumeRemovedAction(stageID, actionName string) (bool, string) {
	return c.actionContext.consumeRemovedAction(stageID, actionName)
}

func (c *contextImpl) ConsumeRemovedStages() map[string]string {
	return c.actionContext.consumeRemovedStages()
}

func (c *contextImpl) SetDeadline(deadline time.Time) {
	c.valueMu.Lock()
	defer c.valueMu.Unlock()
	c.deadline = deadline
	c.hasDeadline = true
}

func (c *contextImpl) SetValue(key, value interface{}) {
	c.valueMu.Lock()
	defer c.valueMu.Unlock()
	c.values[key] = value
}

func (c *contextImpl) Cancel(err error) {
	c.valueMu.Lock()
	defer c.valueMu.Unlock()
	c.err = err
	close(c.done)
}
