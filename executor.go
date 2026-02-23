package gostage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
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

		// Execute step (with middleware if configured)
		// Merge engine-level and per-workflow step middleware
		allStepMW := e.stepMiddleware
		if len(wf.cfg.stepMiddleware) > 0 {
			allStepMW = append(append([]StepMiddleware{}, e.stepMiddleware...), wf.cfg.stepMiddleware...)
		}

		var err error
		if len(allStepMW) > 0 {
			final := func() error {
				return e.executeStep(ctx, wf, s, runID, resuming)
			}
			chain := final
			for j := len(allStepMW) - 1; j >= 0; j-- {
				mw := allStepMW[j]
				next := chain
				stepCopy := s
				chain = func() error {
					return mw(ctx, stepCopy, runID, next)
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
			var sleepErr *SleepError
			if errors.As(err, &bailErr) || errors.As(err, &suspendErr) || errors.As(err, &sleepErr) {
				return err
			}

			// Check context cancellation
			if ctx.Err() != nil {
				return ctx.Err()
			}

			// Call error callback
			if wf.cfg.onError != nil {
				wf.cfg.onError(err)
			}

			// Update step status to Failed
			e.persistence.UpdateStepStatus(ctx, runID, s.id, Failed)
			return fmt.Errorf("step %q failed: %w", s.name, err)
		}

		// Per-step flush: write dirty state entries to persistence
		wf.state.Flush(ctx)

		// Step completed successfully
		e.persistence.UpdateStepStatus(ctx, runID, s.id, Completed)

		// Lifecycle callback
		if wf.cfg.onStepComplete != nil {
			stepCtx := newCtx(ctx, wf.state, e.logger)
			wf.cfg.onStepComplete(s.name, stepCtx)
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
						kind:     stepSingle,
						name:     m.TaskName,
						taskName: m.TaskName,
					}
					// Insert after current position
					pos := i + 1
					wf.steps = append(wf.steps, step{})
					copy(wf.steps[pos+1:], wf.steps[pos:])
					wf.steps[pos] = newStep
				case MutDisableStep:
					for j := range wf.steps {
						if wf.steps[j].id == m.TargetID || wf.steps[j].name == m.TargetID {
							wf.steps[j].disabled = true
						}
					}
				case MutEnableStep:
					for j := range wf.steps {
						if wf.steps[j].id == m.TargetID || wf.steps[j].name == m.TargetID {
							wf.steps[j].disabled = false
						}
					}
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
	case stepSingle:
		return e.executeSingle(ctx, wf, s.taskName, runID, resuming)
	case stepParallel:
		return e.executeParallel(ctx, wf, s.refs, runID, resuming)
	case stepStage:
		return e.executeStage(ctx, wf, s.refs, runID, resuming)
	case stepBranch:
		return e.executeBranch(ctx, wf, s.cases, runID, resuming)
	case stepForEach:
		return e.executeForEach(ctx, wf, s, runID, resuming)
	case stepMap:
		return e.executeMap(ctx, wf, s.mapFn)
	case stepDoUntil:
		return e.executeDoUntil(ctx, wf, s, runID, resuming)
	case stepDoWhile:
		return e.executeDoWhile(ctx, wf, s, runID, resuming)
	case stepSub:
		return e.executeSub(ctx, wf, s.subWorkflow, runID, resuming)
	case stepSleep:
		return e.executeSleep(ctx, s.sleepDuration)
	default:
		return fmt.Errorf("unknown step kind: %d", s.kind)
	}
}

// executeTaskFn wraps a task function with the engine's task middleware chain and panic recovery.
// Used by both executeSingle and executeForEachItem to ensure consistent middleware application.
func (e *Engine) executeTaskFn(taskCtx *Ctx, taskName string, fn func(*Ctx) error) error {
	if len(e.taskMiddleware) > 0 {
		final := func() error { return fn(taskCtx) }
		chain := final
		for j := len(e.taskMiddleware) - 1; j >= 0; j-- {
			mw := e.taskMiddleware[j]
			next := chain
			chain = func() error {
				return mw(taskCtx, taskName, next)
			}
		}
		return func() (rerr error) {
			defer func() {
				if r := recover(); r != nil {
					rerr = fmt.Errorf("middleware panic: %v", r)
				}
			}()
			return chain()
		}()
	}
	return fn(taskCtx)
}

// executeSingle runs a single task with retry logic.
func (e *Engine) executeSingle(ctx context.Context, wf *Workflow, taskName string, runID RunID, resuming bool) error {
	td := lookupTask(taskName)
	if td == nil {
		return fmt.Errorf("task %q not registered", taskName)
	}

	retries := td.retries
	retryDelay := td.retryDelay
	if retries == 0 && wf.cfg.defaultRetries > 0 {
		retries = wf.cfg.defaultRetries
		retryDelay = wf.cfg.defaultRetryDelay
	}

	taskCtx := newCtx(ctx, wf.state, e.logger)
	taskCtx.resuming = resuming
	taskCtx.workflow = wf
	taskCtx.mutations = wf.mutations
	taskCtx.engine = e

	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 && retryDelay > 0 {
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Execute with task middleware if configured
		err := e.executeTaskFn(taskCtx, taskName, td.fn)

		if err == nil {
			return nil
		}

		// Don't retry bail/suspend/sleep/context errors
		var bailErr *BailError
		var suspendErr *SuspendError
		var sleepErr *SleepError
		if errors.As(err, &bailErr) || errors.As(err, &suspendErr) || errors.As(err, &sleepErr) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lastErr = err
	}

	return lastErr
}

// executeRef runs a single StepRef (task or sub-workflow).
func (e *Engine) executeRef(ctx context.Context, wf *Workflow, ref StepRef, runID RunID, resuming bool) error {
	if ref.subWorkflow != nil {
		return e.executeSub(ctx, wf, ref.subWorkflow, runID, resuming)
	}
	return e.executeSingle(ctx, wf, ref.taskName, runID, resuming)
}

// executeParallel runs steps concurrently using goroutines.
func (e *Engine) executeParallel(ctx context.Context, wf *Workflow, refs []StepRef, runID RunID, resuming bool) error {
	if len(refs) == 0 {
		return nil
	}
	if len(refs) == 1 {
		return e.executeRef(ctx, wf, refs[0], runID, resuming)
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, ref := range refs {
		ref := ref
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.executeRef(childCtx, wf, ref, runID, resuming); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel() // cancel other goroutines on first error
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return firstErr
}

// executeStage runs steps sequentially within a named group.
func (e *Engine) executeStage(ctx context.Context, wf *Workflow, refs []StepRef, runID RunID, resuming bool) error {
	for _, ref := range refs {
		if err := e.executeRef(ctx, wf, ref, runID, resuming); err != nil {
			return err
		}
	}
	return nil
}

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
		if c.condition != nil && c.condition(branchCtx) {
			return e.executeRef(ctx, wf, c.ref, runID, resuming)
		}
	}

	if defaultCase != nil {
		return e.executeRef(ctx, wf, defaultCase.ref, runID, resuming)
	}

	return nil // no branch matched, no default — skip
}

// executeForEach iterates over a collection with optional concurrency.
func (e *Engine) executeForEach(ctx context.Context, wf *Workflow, s *step, runID RunID, resuming bool) error {
	items := getSliceFromState(wf.state, s.collectionKey)
	if len(items) == 0 {
		return nil
	}

	// Spawn path: run each item in an isolated child process
	if s.useSpawn {
		return e.executeForEachSpawn(ctx, wf, s, items, runID, resuming)
	}

	// Load step states for per-item resume tracking
	var stepStates map[string]Status
	if resuming {
		run, err := e.persistence.LoadRun(ctx, runID)
		if err == nil && run.StepStates != nil {
			stepStates = run.StepStates
		}
	}

	if s.concurrency <= 1 {
		// Sequential
		for i, item := range items {
			// Skip completed items on resume
			itemKey := fmt.Sprintf("%s:%d", s.id, i)
			if resuming && stepStates != nil && stepStates[itemKey] == Completed {
				continue
			}

			if err := e.executeForEachItem(ctx, wf, s.forEachRef, item, i, runID, resuming); err != nil {
				return err
			}

			// Track per-item completion
			e.persistence.UpdateStepStatus(ctx, runID, itemKey, Completed)
		}
		return nil
	}

	// Concurrent with semaphore
	sem := make(chan struct{}, s.concurrency)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, item := range items {
		i, item := i, item

		// Skip completed items on resume
		itemKey := fmt.Sprintf("%s:%d", s.id, i)
		if resuming && stepStates != nil && stepStates[itemKey] == Completed {
			continue
		}

		// Check for prior error or context cancellation
		mu.Lock()
		hasErr := firstErr != nil
		mu.Unlock()
		if hasErr || childCtx.Err() != nil {
			break
		}

		sem <- struct{}{} // acquire
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }() // release

			if err := e.executeForEachItem(childCtx, wf, s.forEachRef, item, i, runID, resuming); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				mu.Unlock()
				return
			}

			// Track per-item completion
			e.persistence.UpdateStepStatus(ctx, runID, itemKey, Completed)
		}()
	}

	wg.Wait()
	return firstErr
}

// executeForEachItem runs a single ForEach iteration with item context.
func (e *Engine) executeForEachItem(ctx context.Context, wf *Workflow, ref StepRef, item any, index int, runID RunID, resuming bool) error {
	if ref.subWorkflow != nil {
		// For sub-workflows, set the item/index in the parent state before executing
		wf.state.Set("__foreach_item", item)
		wf.state.Set("__foreach_index", index)
		return e.executeSub(ctx, wf, ref.subWorkflow, runID, resuming)
	}

	td := lookupTask(ref.taskName)
	if td == nil {
		return fmt.Errorf("task %q not registered", ref.taskName)
	}

	retries := td.retries
	retryDelay := td.retryDelay
	if retries == 0 && wf.cfg.defaultRetries > 0 {
		retries = wf.cfg.defaultRetries
		retryDelay = wf.cfg.defaultRetryDelay
	}

	taskCtx := newCtx(ctx, wf.state, e.logger)
	taskCtx.forEachItem = item
	taskCtx.forEachIndex = index
	taskCtx.resuming = resuming
	taskCtx.workflow = wf
	taskCtx.mutations = wf.mutations
	taskCtx.engine = e

	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 && retryDelay > 0 {
			select {
			case <-time.After(retryDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := e.executeTaskFn(taskCtx, ref.taskName, td.fn)
		if err == nil {
			return nil
		}

		var bailErr *BailError
		var suspendErr *SuspendError
		if errors.As(err, &bailErr) || errors.As(err, &suspendErr) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lastErr = err
	}

	return lastErr
}

// executeMap runs an inline data transformation.
func (e *Engine) executeMap(ctx context.Context, wf *Workflow, fn func(*Ctx)) error {
	mapCtx := newCtx(ctx, wf.state, e.logger)
	fn(mapCtx)
	return nil
}

// executeDoUntil repeats: execute, then check condition. Stops when condition is true.
func (e *Engine) executeDoUntil(ctx context.Context, wf *Workflow, s *step, runID RunID, resuming bool) error {
	for i := 0; i < maxLoopIterations; i++ {
		if err := e.executeRef(ctx, wf, s.loopRef, runID, resuming); err != nil {
			return err
		}
		condCtx := newCtx(ctx, wf.state, e.logger)
		if s.loopCond(condCtx) {
			return nil
		}
	}
	return fmt.Errorf("DoUntil exceeded %d iterations", maxLoopIterations)
}

// executeDoWhile repeats: check condition, then execute. Stops when condition is false.
func (e *Engine) executeDoWhile(ctx context.Context, wf *Workflow, s *step, runID RunID, resuming bool) error {
	for i := 0; i < maxLoopIterations; i++ {
		condCtx := newCtx(ctx, wf.state, e.logger)
		if !s.loopCond(condCtx) {
			return nil
		}
		if err := e.executeRef(ctx, wf, s.loopRef, runID, resuming); err != nil {
			return err
		}
	}
	return fmt.Errorf("DoWhile exceeded %d iterations", maxLoopIterations)
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

	// With persistence, return SleepError so the engine saves state
	if _, ok := e.persistence.(*memoryPersistence); !ok {
		return &SleepError{WakeAt: time.Now().Add(d)}
	}
	// Without persistence (memory only), block
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// updateCurrentStep updates the current step tracking in the run state.
func (e *Engine) updateCurrentStep(ctx context.Context, runID RunID, stepID string) {
	run, err := e.persistence.LoadRun(ctx, runID)
	if err != nil {
		return
	}
	run.CurrentStep = stepID
	e.persistence.SaveRun(ctx, run)
}

// getSliceFromState extracts a slice from the run state for ForEach iteration.
func getSliceFromState(s *runState, key string) []any {
	val, ok := s.Get(key)
	if !ok {
		return nil
	}

	// Use reflection to handle any slice type
	rv := reflect.ValueOf(val)
	if rv.Kind() != reflect.Slice {
		return []any{val} // single item
	}

	result := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		result[i] = rv.Index(i).Interface()
	}
	return result
}
