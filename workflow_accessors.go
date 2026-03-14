package gostage

import "context"

// Accessor methods for the spawn sub-package.
// These expose internal Workflow fields through exported methods so the
// spawn package can operate on them without import cycles.

// RunState returns the workflow's per-run state buffer.
// Used by the spawn package to serialize state for child processes
// and merge child results back.
func (wf *Workflow) RunState() *RunStateAccessor {
	return &RunStateAccessor{state: wf.state}
}

// RunStateAccessor provides spawn-safe access to the run state buffer.
// It wraps the unexported *runState and exposes only the methods the
// spawn package needs.
type RunStateAccessor struct {
	state *runState
}

// SerializeAll returns all entries as JSON-encoded typedEntry values.
func (a *RunStateAccessor) SerializeAll() (map[string][]byte, error) {
	return a.state.SerializeAll()
}

// SetBatch stores multiple values atomically, marking all as dirty.
func (a *RunStateAccessor) SetBatch(entries map[string]any) {
	a.state.SetBatch(entries)
}

// Flush writes dirty entries to persistence.
func (a *RunStateAccessor) Flush(ctx context.Context) error {
	return a.state.Flush(ctx)
}
