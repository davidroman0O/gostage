package local

import (
	"sync"
	"time"

	"github.com/davidroman0O/gostage/v3/internal/foundation/locks"
	rt "github.com/davidroman0O/gostage/v3/shared/runtime"
	"github.com/davidroman0O/gostage/v3/shared/runtime/core"
	store "github.com/davidroman0O/gostage/v3/shared/store"
)

// Factory builds execution contexts for the local runner backend.
type Factory struct{}

// New creates a new execution context for the provided workflow.
func (Factory) New(workflow rt.Workflow, broker rt.Broker) core.ExecutionContext {
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
	broker rt.Broker

	valueMu locks.RWMutex

	deadline    time.Time
	hasDeadline bool
	done        chan struct{}
	err         error
	values      map[interface{}]interface{}
	cancelOnce  sync.Once
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

func (c *contextImpl) Stages() rt.StageMutation   { return newStageMutation(c.actionContext) }
func (c *contextImpl) Actions() rt.ActionMutation { return newActionMutation(c.actionContext) }
func (c *contextImpl) Broker() rt.Broker          { return c.broker }
func (c *contextImpl) Workflow() rt.Workflow      { return c.workflow }
func (c *contextImpl) Stage() rt.Stage            { return c.getStage() }
func (c *contextImpl) Action() rt.Action {
	action, _, _ := c.getAction()
	return action
}

func (c *contextImpl) ActionIndex() int {
	_, idx, _ := c.getAction()
	return idx
}

func (c *contextImpl) IsLastAction() bool {
	_, _, last := c.getAction()
	return last
}

func (c *contextImpl) Store() store.Handle {
	if c.workflow == nil {
		return store.Handle{}
	}
	return c.workflow.Store()
}
func (c *contextImpl) Logger() rt.Logger { return c.getLogger() }

func (c *contextImpl) Query() rt.Query {
	return newQuery(c)
}

// Store convenience methods (non-generic)
func (c *contextImpl) Put(key string, value any) error {
	return store.Put(c, key, value)
}

func (c *contextImpl) Delete(key string) bool {
	return store.Delete(c, key)
}

func (c *contextImpl) Has(key string) bool {
	h := c.Store()
	if h.IsZero() {
		return false
	}
	// Use ExportAll to check if key exists
	all := store.ExportAll(c)
	_, exists := all[key]
	return exists
}

func (c *contextImpl) Export() map[string]any {
	return store.ExportAll(c)
}

func (c *contextImpl) SetLogger(logger rt.Logger) { c.setLogger(logger) }
func (c *contextImpl) SetStage(stage rt.Stage)    { c.setStage(stage) }
func (c *contextImpl) ClearStage()                { c.clearStage() }
func (c *contextImpl) SetAction(action rt.Action, index int, isLast bool) {
	c.setAction(action, index, isLast)
}

func (c *contextImpl) ConsumeDynamicActions() []rt.Action {
	return c.consumeDynamicActions()
}

func (c *contextImpl) ConsumeDynamicStages() []rt.Stage {
	return c.consumeDynamicStages()
}
func (c *contextImpl) SetActionList(actions []rt.Action) { c.populateActions(actions) }
func (c *contextImpl) SetDisabledMaps(actions, stages map[string]bool) {
	c.setDisabledMaps(actions, stages)
}

func (c *contextImpl) DisabledMaps() (map[string]bool, map[string]bool) {
	return c.disabledMaps()
}

func (c *contextImpl) ConsumeRemovedAction(stageID, actionName string) (bool, string) {
	return c.consumeRemovedAction(stageID, actionName)
}

func (c *contextImpl) ConsumeRemovedStages() map[string]string {
	return c.consumeRemovedStages()
}

func (c *contextImpl) SetDeadline(deadline time.Time) {
	c.valueMu.Lock()
	defer c.valueMu.Unlock()
	c.deadline = deadline
	c.hasDeadline = true
}

func (c *contextImpl) Cancel(err error) {
	c.cancelOnce.Do(func() {
		c.valueMu.Lock()
		c.err = err
		c.valueMu.Unlock()
		close(c.done)
	})
}

func (c *contextImpl) SetValue(key, value interface{}) {
	c.valueMu.Lock()
	defer c.valueMu.Unlock()
	c.values[key] = value
}
