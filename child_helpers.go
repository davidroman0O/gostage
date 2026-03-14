package gostage

import (
	"context"
	"fmt"
)

// ChildRunState wraps a runState for child process usage.
// Provides the methods the spawn sub-package needs to set up and
// finalize child process state.
type ChildRunState struct {
	state *runState
}

// NewRunStateForChild creates a new memory-only run state for a child process.
func NewRunStateForChild(runID RunID) *ChildRunState {
	return &ChildRunState{state: newRunState(runID, nil)}
}

// SetClean stores a value without marking it dirty (parent data is NOT dirty).
func (c *ChildRunState) SetClean(key string, value any) {
	c.state.SetClean(key, value)
}

// SerializeDirty returns only dirty entries as JSON-encoded typedEntry values.
func (c *ChildRunState) SerializeDirty() (map[string][]byte, error) {
	return c.state.SerializeDirty()
}

// Accessor returns a RunStateAccessor for use with SerializeStateForChild.
func (c *ChildRunState) Accessor() *RunStateAccessor {
	return &RunStateAccessor{state: c.state}
}

// SetRunStateFromChild replaces the workflow's state with a child-provided state.
// Used by HandleChild when executing multi-stage workflows.
func (wf *Workflow) SetRunStateFromChild(cs *ChildRunState) {
	wf.state = cs.state
}

// ExecuteWorkflowForChild exposes executeWorkflow for the spawn sub-package.
// Used by HandleChild to execute multi-stage workflows in child processes.
func (e *Engine) ExecuteWorkflowForChild(ctx context.Context, wf *Workflow, runID RunID, resuming bool) error {
	return e.executeWorkflow(ctx, wf, runID, resuming)
}

// ExecuteSingleTaskForChild runs a single task in a child process using the
// engine's retry and middleware machinery.
func (e *Engine) ExecuteSingleTaskForChild(ctx context.Context, taskName string, childState *ChildRunState, storeVals map[string]any, ipcSendFn func(string, any) error) error {
	td := e.registry.lookupTask(taskName)
	if td == nil {
		return fmt.Errorf("task %q not registered in child", taskName)
	}

	taskCtx := newCtx(ctx, childState.state, NewDefaultLogger())
	taskCtx.sendFn = ipcSendFn
	taskCtx.engine = e

	// Set ForEach item/index on Ctx from store
	if item, ok := storeVals["__foreach_item"]; ok {
		taskCtx.forEachItem = item
	}
	if idx, ok := storeVals["__foreach_index"]; ok {
		if idxFloat, ok := idx.(float64); ok {
			taskCtx.forEachIndex = int(idxFloat)
		}
	}

	// Use the child engine's retry and middleware machinery
	retries := td.retries
	if retries < 0 {
		retries = 0
	}
	strategy := td.retryStrategy
	if strategy == nil && td.retryDelay > 0 {
		strategy = FixedDelay(td.retryDelay)
	}
	return e.retryTask(ctx, taskName, taskCtx, td.fn, retries, strategy, td.timeout)
}
