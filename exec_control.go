package gostage

import (
	"context"
	"fmt"
	"time"
)

// executeBranch evaluates conditions and runs the matching branch.
func (e *Engine) executeBranch(ctx context.Context, wf *Workflow, cases []BranchCase, runID RunID, resuming bool) error {
	branchCtx := newCtx(ctx, wf.state, e.logger)

	var defaultCase *BranchCase
	for i := range cases {
		c := &cases[i]
		if c.isDefault {
			defaultCase = c
			continue
		}
		condResult, condErr := func() (result bool, err error) {
			if c.condition == nil {
				return false, nil
			}
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("branch condition panic: %v", r)
				}
			}()
			return c.condition(branchCtx), nil
		}()
		if condErr != nil {
			return condErr
		}
		if condResult {
			return e.executeRef(ctx, wf, c.ref, runID, resuming)
		}
	}

	if defaultCase != nil {
		return e.executeRef(ctx, wf, defaultCase.ref, runID, resuming)
	}

	return nil // no branch matched, no default — skip
}

// executeDoUntil repeats: execute, then check condition. Stops when condition is true.
func (e *Engine) executeDoUntil(ctx context.Context, wf *Workflow, s *step, runID RunID, resuming bool) error {
	for i := 0; i < maxLoopIterations; i++ {
		if err := e.executeRef(ctx, wf, s.loopRef, runID, resuming); err != nil {
			return err
		}
		// Flush state between iterations for crash safety
		if flushErr := wf.state.Flush(ctx); flushErr != nil {
			return fmt.Errorf("flush state in DoUntil iteration %d: %w", i, flushErr)
		}
		condCtx := newCtx(ctx, wf.state, e.logger)
		condResult, condErr := func() (result bool, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("loop condition panic: %v", r)
				}
			}()
			return s.loopCond(condCtx), nil
		}()
		if condErr != nil {
			return condErr
		}
		if condResult {
			return nil
		}
	}
	return fmt.Errorf("DoUntil exceeded %d iterations", maxLoopIterations)
}

// executeDoWhile repeats: check condition, then execute. Stops when condition is false.
func (e *Engine) executeDoWhile(ctx context.Context, wf *Workflow, s *step, runID RunID, resuming bool) error {
	for i := 0; i < maxLoopIterations; i++ {
		condCtx := newCtx(ctx, wf.state, e.logger)
		condResult, condErr := func() (result bool, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("loop condition panic: %v", r)
				}
			}()
			return s.loopCond(condCtx), nil
		}()
		if condErr != nil {
			return condErr
		}
		if !condResult {
			return nil
		}
		if err := e.executeRef(ctx, wf, s.loopRef, runID, resuming); err != nil {
			return err
		}
		// Flush state between iterations for crash safety
		if flushErr := wf.state.Flush(ctx); flushErr != nil {
			return fmt.Errorf("flush state in DoWhile iteration %d: %w", i, flushErr)
		}
	}
	return fmt.Errorf("DoWhile exceeded %d iterations", maxLoopIterations)
}

// executeMap runs an inline data transformation.
func (e *Engine) executeMap(ctx context.Context, wf *Workflow, fn func(*Ctx) error) (err error) {
	mapCtx := newCtx(ctx, wf.state, e.logger)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("map function panic: %v", r)
		}
	}()
	return fn(mapCtx)
}

// executeSub runs a sub-workflow's steps inline, sharing the parent's state.
func (e *Engine) executeSub(ctx context.Context, parentWf *Workflow, subWf *Workflow, runID RunID, resuming bool) error {
	// Sub-workflow shares the parent's state
	subWf.state = parentWf.state
	for i := range subWf.steps {
		s := &subWf.steps[i]
		if err := e.executeStep(ctx, parentWf, s, runID, resuming); err != nil {
			return err
		}
	}
	return nil
}

// executeSleep handles a timed delay.
func (e *Engine) executeSleep(ctx context.Context, d time.Duration) error {
	// Zero or negative duration: no-op, execute immediately
	if d <= 0 {
		return nil
	}

	// With persistence, return sleepError so the engine saves state
	if _, ok := e.persistence.(InMemoryPersistence); !ok {
		return &sleepError{wakeAt: time.Now().Add(d)}
	}
	// Without persistence (memory only), block
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
