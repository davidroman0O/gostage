package gostage

import (
	"context"
	"fmt"
	"sync"
)

// executeParallel runs steps concurrently using goroutines.
// Per-ref completion tracking: each branch records its completion so that
// on resume after a crash, completed branches are skipped.
func (e *Engine) executeParallel(ctx context.Context, wf *Workflow, refs []StepRef, runID RunID, resuming bool, stepID string) error {
	if len(refs) == 0 {
		return nil
	}

	// Load step states for per-ref resume tracking
	var stepStates map[string]Status
	if resuming {
		run, err := e.persistence.LoadRun(ctx, runID)
		if err == nil && run.StepStates != nil {
			stepStates = run.StepStates
		}
	}

	if len(refs) == 1 {
		refKey := fmt.Sprintf("%s:ref_0", stepID)
		if resuming && stepStates != nil && stepStates[refKey] == Completed {
			return nil
		}
		if err := e.executeRef(ctx, wf, refs[0], runID, resuming); err != nil {
			return err
		}
		if persistErr := e.persistence.UpdateStepStatus(ctx, runID, refKey, Completed); persistErr != nil {
			e.logger.Warn("persist parallel ref status: %v", persistErr)
		}
		return nil
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i, ref := range refs {
		refKey := fmt.Sprintf("%s:ref_%d", stepID, i)

		// Skip completed refs on resume
		if resuming && stepStates != nil && stepStates[refKey] == Completed {
			continue
		}

		ref := ref
		refKeyCopy := refKey
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.executeRef(childCtx, wf, ref, runID, resuming); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				mu.Unlock()
				return
			}
			if persistErr := e.persistence.UpdateStepStatus(ctx, runID, refKeyCopy, Completed); persistErr != nil {
				e.logger.Warn("persist parallel ref status: %v", persistErr)
			}
		}()
	}

	wg.Wait()
	return firstErr
}

// executeStage runs steps sequentially within a named group.
// Per-ref completion tracking: each ref records its completion so that
// on resume after a crash, completed refs are skipped.
func (e *Engine) executeStage(ctx context.Context, wf *Workflow, refs []StepRef, runID RunID, resuming bool, stepID string) error {
	// Load step states for per-ref resume tracking
	var stepStates map[string]Status
	if resuming {
		run, err := e.persistence.LoadRun(ctx, runID)
		if err == nil && run.StepStates != nil {
			stepStates = run.StepStates
		}
	}

	for i, ref := range refs {
		refKey := fmt.Sprintf("%s:ref_%d", stepID, i)

		// Skip completed refs on resume
		if resuming && stepStates != nil && stepStates[refKey] == Completed {
			continue
		}

		if err := e.executeRef(ctx, wf, ref, runID, resuming); err != nil {
			return err
		}
		if persistErr := e.persistence.UpdateStepStatus(ctx, runID, refKey, Completed); persistErr != nil {
			e.logger.Warn("persist stage ref status: %v", persistErr)
		}
	}
	return nil
}
