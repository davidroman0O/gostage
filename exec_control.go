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
		condFn := c.condition
		if condFn == nil && c.condName != "" {
			condFn = e.registry.lookupCondition(c.condName)
			if condFn == nil {
				return fmt.Errorf("condition %q not registered", c.condName)
			}
		}
		condResult, condErr := func() (result bool, err error) {
			if condFn == nil {
				return false, nil
			}
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("branch condition panic: %v", r)
				}
			}()
			return condFn(branchCtx), nil
		}()
		if condErr != nil {
			return condErr
		}
		if condResult {
			return e.executeRef(ctx, wf, c.ref, runID, resuming, "branch")
		}
	}

	if defaultCase != nil {
		return e.executeRef(ctx, wf, defaultCase.ref, runID, resuming, "branch")
	}

	return nil // no branch matched, no default — skip
}

// executeDoUntil repeats: execute, then check condition. Stops when condition is true.
func (e *Engine) executeDoUntil(ctx context.Context, wf *Workflow, s *step, runID RunID, resuming bool) error {
	loopCond := s.loopCond
	if loopCond == nil && s.loopCondName != "" {
		loopCond = e.registry.lookupCondition(s.loopCondName)
		if loopCond == nil {
			return fmt.Errorf("condition %q not registered", s.loopCondName)
		}
	}
	for i := 0; i < maxLoopIterations; i++ {
		if err := e.executeRef(ctx, wf, s.loopRef, runID, resuming, s.id); err != nil {
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
			return loopCond(condCtx), nil
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
	loopCond := s.loopCond
	if loopCond == nil && s.loopCondName != "" {
		loopCond = e.registry.lookupCondition(s.loopCondName)
		if loopCond == nil {
			return fmt.Errorf("condition %q not registered", s.loopCondName)
		}
	}
	for i := 0; i < maxLoopIterations; i++ {
		condCtx := newCtx(ctx, wf.state, e.logger)
		condResult, condErr := func() (result bool, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("loop condition panic: %v", r)
				}
			}()
			return loopCond(condCtx), nil
		}()
		if condErr != nil {
			return condErr
		}
		if !condResult {
			return nil
		}
		if err := e.executeRef(ctx, wf, s.loopRef, runID, resuming, s.id); err != nil {
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
// Per-step state flushing and completion tracking match the guarantees of
// executeWorkflow: state is persisted after each sub-step, and completed
// sub-steps are skipped on resume after a crash.
//
// parentStepKey is the step ID of the calling context (e.g. the ForEach item
// key "forEach:0", the Parallel ref key "parallel:ref_1", or the Sub step ID
// "outer-wf:3"). Sub-step IDs are generated as parentStepKey:sub_N, making
// them unique per execution context and preventing collisions when the same
// sub-workflow definition is used across multiple ForEach items or Parallel refs.
func (e *Engine) executeSub(ctx context.Context, parentWf *Workflow, subWf *Workflow, runID RunID, resuming bool, parentStepKey string) error {
	// Sub-workflow shares the parent's state.
	subWf.state = parentWf.state

	// Load step states for per-sub-step resume tracking.
	// Sub-step IDs are scoped to parentStepKey so that different ForEach items
	// or Parallel refs using the same sub-workflow definition never share keys.
	var stepStates map[string]Status
	if resuming {
		run, err := e.persistence.LoadRun(ctx, runID)
		if err == nil && run.StepStates != nil {
			stepStates = run.StepStates
		}
	}

	for i := range subWf.steps {
		s := &subWf.steps[i]
		// Sub-step key is scoped to the calling context, not to the sub-workflow
		// definition ID. This prevents collisions when the same sub-workflow is
		// used by multiple ForEach items or Parallel refs in the same run.
		subStepKey := fmt.Sprintf("%s:sub_%d", parentStepKey, i)

		// Skip completed sub-steps on resume.
		if resuming && stepStates != nil && stepStates[subStepKey] == Completed {
			continue
		}

		if err := e.executeStep(ctx, parentWf, s, runID, resuming); err != nil {
			// Mark the sub-step as failed so a future resume knows it did not
			// complete cleanly.
			if persistErr := e.persistence.UpdateStepStatus(ctx, runID, subStepKey, Failed); persistErr != nil {
				e.logger.Error("persist sub-step %q failed status: %v", s.name, persistErr)
			}
			return err
		}

		// Flush state to persistence after each sub-step so a crash does not
		// lose state written during completed sub-steps.
		if flushErr := parentWf.state.Flush(ctx); flushErr != nil {
			return fmt.Errorf("flush state after sub-step %q: %w", s.name, flushErr)
		}

		// Record the sub-step as completed so it is skipped on resume.
		if persistErr := e.persistence.UpdateStepStatus(ctx, runID, subStepKey, Completed); persistErr != nil {
			return fmt.Errorf("persist sub-step %q completed: %w", s.name, persistErr)
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
