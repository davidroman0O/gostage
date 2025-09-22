package runner

import (
	"time"

	"github.com/davidroman0O/gostage/v2/types"
	deadlock "github.com/sasha-s/go-deadlock"
)

// contextImpl implements the types.Context interface
type contextImpl struct {
	*actionContext
	broker types.BrokerCall

	// Separate mutex for context-specific fields to avoid lock contention
	valueMu deadlock.RWMutex

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
	c.valueMu.RLock()
	defer c.valueMu.RUnlock()

	return c.deadline, c.hasDeadline
}

func (c *contextImpl) Done() <-chan struct{} {
	return c.done
}

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
