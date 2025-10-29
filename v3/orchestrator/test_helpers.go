package orchestrator

import (
	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/child"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/scheduler"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

// NewTestNode constructs an empty Node for whitebox tests.
func NewTestNode() *Node {
	return &Node{}
}

// SetBaseForTest injects the node base implementation for tests.
func (n *Node) SetBaseForTest(base *node.Node) { n.base = base }

// SetDispatcherForTest injects the scheduler dispatcher for tests.
func (n *Node) SetDispatcherForTest(dispatcher *scheduler.Dispatcher) {
	n.dispatcher = dispatcher
}

// SetRemoteForTest injects the remote coordinator for tests.
func (n *Node) SetRemoteForTest(remote *RemoteCoordinator) { n.remote = remote }

// SetQueueForTest injects the queue used for submissions in tests.
func (n *Node) SetQueueForTest(queue state.Queue) { n.queue = queue }

// SetStoreForTest injects the store used for persistence in tests.
func (n *Node) SetStoreForTest(store state.Store) { n.store = store }

// SetPoolsForTest injects the pool bindings used in tests.
func (n *Node) SetPoolsForTest(pools []*PoolBinding) { n.pools = pools }

// SetLoggerForTest injects the logger used in tests.
func (n *Node) SetLoggerForTest(logger telemetry.Logger) { n.logger = logger }

// SetStateReaderForTest injects the read-only state facade for tests.
func (n *Node) SetStateReaderForTest(reader state.StateReader) { n.State = reader }

func NewChildHandlerRegistration(opts bootstrap.ChildOptions) *ChildHandlerRegistration {
	return &ChildHandlerRegistration{options: opts}
}

func ResetChildRegistrationsForTest() {
	childHandlersMu.Lock()
	defaultChildHandler = nil
	namedChildHandlers = make(map[string]*ChildHandlerRegistration)
	childHandlersMu.Unlock()
}

func SetChildDetectForTest(fn func([]string, child.GetenvFunc) (child.Config, bool, error)) {
	childDetect = fn
}

func ChildDetectForTest() func([]string, child.GetenvFunc) (child.Config, bool, error) {
	return childDetect
}

func MergeChildConfigForTest(base child.Config, reg *ChildHandlerRegistration) (child.Config, error) {
	return mergeChildConfig(base, reg)
}

func ResolveWorkflowReferenceForTest(ref WorkflowReference) (workflow.Definition, error) {
	return ref.resolve()
}

func ApplySubmitOptionForTest(opt SubmitOption, req *bootstrap.SubmitRequest) {
	if opt == nil || req == nil {
		return
	}
	opt.applySubmit(req)
}
