package gostage

import (
	"context"
	"errors"
	"fmt"
)

const maxLoopIterations = 10000

// executeWorkflow walks through all steps in a workflow.
// When resuming is true, completed steps are skipped based on persisted StepStates.
func (e *Engine) executeWorkflow(ctx context.Context, wf *Workflow, runID RunID, resuming bool) error {
	// Load step states for resume checking
	var stepStates map[string]Status
	if resuming {
		run, err := e.persistence.LoadRun(ctx, runID)
		if err == nil && run.StepStates != nil {
			stepStates = run.StepStates
		}
	}

	// Compute merged step middleware once before the loop — the composition is
	// constant per run, so allocating inside the loop is unnecessary.
	allStepMW := e.stepMiddleware
	if len(wf.cfg.stepMiddleware) > 0 {
		allStepMW = append(append([]StepMiddleware{}, e.stepMiddleware...), wf.cfg.stepMiddleware...)
	}

	i := 0
	for i < len(wf.steps) {
		s := &wf.steps[i]

		// Skip completed steps on resume
		if resuming && stepStates != nil && stepStates[s.id] == Completed {
			i++
			continue
		}

		// Skip disabled steps (from dynamic mutations)
		if s.disabled {
			i++
			continue
		}

		// Update current step in persistence
		e.updateCurrentStep(ctx, runID, s.id)

		var err error
		if len(allStepMW) > 0 {
			final := func() error {
				return e.executeStep(ctx, wf, s, runID, resuming)
			}
			chain := final
			for j := 0; j < len(allStepMW); j++ {
				mw := allStepMW[j]
				next := chain
				stepInfo := s.info()
				chain = func() error {
					return mw(ctx, stepInfo, runID, next)
				}
			}
			err = func() (rerr error) {
				defer func() {
					if r := recover(); r != nil {
						rerr = fmt.Errorf("middleware panic: %v", r)
					}
				}()
				return chain()
			}()
		} else {
			err = e.executeStep(ctx, wf, s, runID, resuming)
		}

		if err != nil {
			// Don't retry bail/suspend/sleep — propagate immediately
			var bailErr *BailError
			var suspendErr *SuspendError
			var sleepErr *sleepError
			if errors.As(err, &bailErr) || errors.As(err, &suspendErr) || errors.As(err, &sleepErr) {
				return err
			}

			// Check context cancellation
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// Call error callback
			if wf.cfg.onError != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							e.logger.Warn("panic in onError callback: %v", r)
						}
					}()
					wf.cfg.onError(err)
				}()
			}

			// Update step status to Failed
			if persistErr := e.persistence.UpdateStepStatus(ctx, runID, s.id, Failed); persistErr != nil {
				e.logger.Error("persist step failed status: %v", persistErr)
			}
			return fmt.Errorf("step %q failed: %w", s.name, err)
		}

		// Per-step flush: write dirty state entries to persistence.
		// If the flush fails, the step must not be marked Completed — its state
		// writes would be lost on resume.
		if flushErr := wf.state.Flush(ctx); flushErr != nil {
			return fmt.Errorf("flush state after step %q: %w", s.name, flushErr)
		}

		// Step completed successfully — persist the completion status.
		// If this fails, the step would re-execute on resume (safe but wasteful).
		if persistErr := e.persistence.UpdateStepStatus(ctx, runID, s.id, Completed); persistErr != nil {
			return fmt.Errorf("persist step %q completed: %w", s.name, persistErr)
		}

		// Lifecycle callback
		if wf.cfg.onStepComplete != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						e.logger.Warn("panic in onStepComplete callback for step %q: %v", s.name, r)
					}
				}()
				stepCtx := newCtx(ctx, wf.state, e.logger)
				wf.cfg.onStepComplete(s.name, stepCtx)
			}()
		}

		// Apply mutations between steps
		if wf.mutations != nil {
			mutations := wf.mutations.Drain()
			for _, m := range mutations {
				switch m.Kind {
				case MutInsertAfter:
					wf.dynCounter++
					newStep := step{
						id:       fmt.Sprintf("%s:dyn:%d", wf.ID, wf.dynCounter),
						kind:     StepSingle,
						name:     m.TaskName,
						taskName: m.TaskName,
					}
					// Insert after current position
					pos := i + 1
					wf.steps = append(wf.steps, step{})
					copy(wf.steps[pos+1:], wf.steps[pos:])
					wf.steps[pos] = newStep
				case MutDisableStep:
					setStepDisabledByIDOrName(wf.steps, m.TargetID, true)
				case MutEnableStep:
					setStepDisabledByIDOrName(wf.steps, m.TargetID, false)
				}
			}
		}

		i++
	}
	return nil
}

// executeStep dispatches to the correct executor based on step kind.
func (e *Engine) executeStep(ctx context.Context, wf *Workflow, s *step, runID RunID, resuming bool) error {
	switch s.kind {
	case StepSingle:
		return e.executeSingle(ctx, wf, s.taskName, runID, resuming)
	case StepParallel:
		return e.executeParallel(ctx, wf, s.refs, runID, resuming, s.id)
	case StepStage:
		return e.executeStage(ctx, wf, s.refs, runID, resuming, s.id)
	case StepBranch:
		return e.executeBranch(ctx, wf, s.cases, runID, resuming)
	case StepForEach:
		return e.executeForEach(ctx, wf, s, runID, resuming)
	case StepMap:
		mapFn := s.mapFn
		if mapFn == nil && s.mapFnName != "" {
			mapFn = e.registry.lookupMapFn(s.mapFnName)
			if mapFn == nil {
				return fmt.Errorf("map function %q not registered", s.mapFnName)
			}
		}
		if mapFn == nil {
			return fmt.Errorf("map step has nil function and no name")
		}
		return e.executeMap(ctx, wf, mapFn)
	case StepDoUntil:
		return e.executeDoUntil(ctx, wf, s, runID, resuming)
	case StepDoWhile:
		return e.executeDoWhile(ctx, wf, s, runID, resuming)
	case StepSub:
		return e.executeSub(ctx, wf, s.subWorkflow, runID, resuming, s.id)
	case StepSleep:
		return e.executeSleep(ctx, s.sleepDuration)
	default:
		return fmt.Errorf("unknown step kind: %d", s.kind)
	}
}

// executeRef runs a single StepRef (task or sub-workflow).
// parentStepKey is the context-scoped key of the calling step (e.g. a Parallel
// ref key or a ForEach item key); it is passed through to executeSub so that
// sub-step tracking IDs are unique per calling context.
func (e *Engine) executeRef(ctx context.Context, wf *Workflow, ref StepRef, runID RunID, resuming bool, parentStepKey string) error {
	if ref.subWorkflow != nil {
		return e.executeSub(ctx, wf, ref.subWorkflow, runID, resuming, parentStepKey)
	}
	return e.executeSingle(ctx, wf, ref.taskName, runID, resuming)
}

// executeSingle runs a single task with retry logic.
func (e *Engine) executeSingle(ctx context.Context, wf *Workflow, taskName string, runID RunID, resuming bool) error {
	td := e.registry.lookupTask(taskName)
	if td == nil {
		return fmt.Errorf("task %q not registered", taskName)
	}

	retries, strategy := resolveRetryConfig(td, wf)

	taskCtx := newCtx(ctx, wf.state, e.logger)
	taskCtx.resuming = resuming
	taskCtx.workflow = wf
	taskCtx.mutations = wf.mutations
	taskCtx.engine = e

	return e.retryTask(ctx, taskName, taskCtx, td.fn, retries, strategy, td.timeout)
}

// updateCurrentStep updates the current step tracking in the run state.
func (e *Engine) updateCurrentStep(ctx context.Context, runID RunID, stepID string) {
	if err := e.persistence.UpdateCurrentStep(ctx, runID, stepID); err != nil {
		e.logger.Error("update current step tracking: %v", err)
	}
}
