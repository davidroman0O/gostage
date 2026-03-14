package gostage

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

// forEachCtxKey is a context key for propagating ForEach item/index
// through sub-workflow execution without shared state races.
type forEachCtxKey struct{}

type forEachCtxData struct {
	item  any
	index int
}

// executeForEach iterates over a collection with optional concurrency.
func (e *Engine) executeForEach(ctx context.Context, wf *Workflow, s *step, runID RunID, resuming bool) error {
	items := getSliceFromState(wf.state, s.collectionKey)
	if len(items) == 0 {
		return nil
	}

	// Spawn path: run each item in an isolated child process
	if s.useSpawn {
		if e.spawnRunner == nil {
			return fmt.Errorf("ForEach with UseSpawn requires a spawn runner; configure with spawn.WithSpawn()")
		}
		cfg := SpawnConfig{
			StepID:        s.id,
			TaskName:      s.forEachRef.taskName,
			SubWorkflow:   s.forEachRef.subWorkflow,
			Concurrency:   s.concurrency,
			CollectionKey: s.collectionKey,
		}
		return e.spawnRunner.ExecuteForEachSpawn(ctx, e, wf, cfg, items, runID, resuming)
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

			if err := e.executeForEachItem(ctx, wf, s.forEachRef, item, i, runID, resuming, itemKey); err != nil {
				return err
			}

			// Track per-item completion
			if persistErr := e.persistence.UpdateStepStatus(ctx, runID, itemKey, Completed); persistErr != nil {
				return fmt.Errorf("persist foreach item completed: %w", persistErr)
			}
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
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("foreach item panic: %v", r)
						cancel()
					}
					mu.Unlock()
				}
			}()

			if err := e.executeForEachItem(childCtx, wf, s.forEachRef, item, i, runID, resuming, itemKey); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				mu.Unlock()
				return
			}

			// Track per-item completion
			if persistErr := e.persistence.UpdateStepStatus(ctx, runID, itemKey, Completed); persistErr != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("persist foreach item completed: %w", persistErr)
					cancel()
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return firstErr
}

// executeForEachItem runs a single ForEach iteration with item context.
// parentStepKey is the ForEach item key (e.g. "forEach:items:0") used to scope
// sub-step IDs so items using the same sub-workflow do not share step_statuses keys.
func (e *Engine) executeForEachItem(ctx context.Context, wf *Workflow, ref StepRef, item any, index int, runID RunID, resuming bool, parentStepKey string) error {
	if ref.subWorkflow != nil {
		// Clone the sub-workflow so concurrent ForEach iterations don't share
		// mutable state (executeSub writes to subWf.state).
		subWf := ref.subWorkflow.clone()
		iterCtx := context.WithValue(ctx, forEachCtxKey{}, &forEachCtxData{item: item, index: index})
		return e.executeSub(iterCtx, wf, subWf, runID, resuming, parentStepKey)
	}

	td := e.registry.lookupTask(ref.taskName)
	if td == nil {
		return fmt.Errorf("task %q not registered", ref.taskName)
	}

	retries, strategy := resolveRetryConfig(td, wf)

	taskCtx := newCtx(ctx, wf.state, e.logger)
	taskCtx.forEachItem = item
	taskCtx.forEachIndex = index
	taskCtx.resuming = resuming
	taskCtx.workflow = wf
	taskCtx.mutations = wf.mutations
	taskCtx.engine = e

	return e.retryTask(ctx, ref.taskName, taskCtx, td.fn, retries, strategy, td.timeout)
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
