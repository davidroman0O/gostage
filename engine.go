package gostage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Engine orchestrates workflow execution with optional persistence.
// It wraps the existing Runner and adds run tracking, checkpointing,
// and resume capabilities.
//
//	engine, _ := gostage.New(gostage.WithSQLite("app.db"))
//	defer engine.Close()
//
//	run, _ := engine.Run(ctx, wf, gostage.P{"order_id": "ORD-123"})
//	result, _ := engine.Wait(ctx, run)
type Engine struct {
	runner      *Runner
	persistence Persistence
	logger      Logger
}

// EngineOption configures the engine.
type EngineOption func(*Engine) error

// New creates a new Engine with the given options.
// Without any options, it uses in-memory persistence and the default runner.
func New(opts ...EngineOption) (*Engine, error) {
	e := &Engine{
		runner:      NewRunner(),
		persistence: newMemoryPersistence(),
		logger:      NewDefaultLogger(),
	}

	for _, opt := range opts {
		if err := opt(e); err != nil {
			return nil, fmt.Errorf("engine option failed: %w", err)
		}
	}

	return e, nil
}

// WithPersistence sets a custom persistence implementation.
func WithPersistence(p Persistence) EngineOption {
	return func(e *Engine) error {
		e.persistence = p
		return nil
	}
}

// WithEngineLogger sets the logger for the engine and runner.
func WithEngineLogger(logger Logger) EngineOption {
	return func(e *Engine) error {
		e.logger = logger
		return nil
	}
}

// WithRunner sets a custom runner for the engine.
func WithRunnerOption(runner *Runner) EngineOption {
	return func(e *Engine) error {
		e.runner = runner
		return nil
	}
}

// Run starts a workflow execution asynchronously and returns the RunID.
// Use Wait() to block until completion.
func (e *Engine) Run(ctx context.Context, wf *Workflow, params P) (RunID, error) {
	runID := RunID(uuid.New().String())
	now := time.Now()

	// Populate the store with params
	if params != nil {
		for key, value := range params {
			wf.Store.Put(key, value)
		}
	}

	// Create run state
	runState := &RunState{
		RunID:      runID,
		WorkflowID: wf.ID,
		Status:     StatusRunningRun,
		StepStates: make(map[string]Status),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := e.persistence.SaveRun(ctx, runState); err != nil {
		return "", fmt.Errorf("failed to save run: %w", err)
	}

	// Execute asynchronously
	go func() {
		result := e.executeRun(ctx, runID, wf)
		_ = result // Result is stored in persistence
	}()

	return runID, nil
}

// RunSync executes a workflow synchronously and returns the result.
// This is the simple path — no goroutines, no polling.
//
//	result, err := engine.RunSync(ctx, wf, gostage.P{"age": 25})
func (e *Engine) RunSync(ctx context.Context, wf *Workflow, params P) (*Result, error) {
	runID := RunID(uuid.New().String())
	now := time.Now()

	// Populate the store with params
	if params != nil {
		for key, value := range params {
			wf.Store.Put(key, value)
		}
	}

	// Create run state
	runState := &RunState{
		RunID:      runID,
		WorkflowID: wf.ID,
		Status:     StatusRunningRun,
		StepStates: make(map[string]Status),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := e.persistence.SaveRun(ctx, runState); err != nil {
		return nil, fmt.Errorf("failed to save run: %w", err)
	}

	return e.executeRun(ctx, runID, wf), nil
}

// Wait blocks until a run completes and returns the result.
func (e *Engine) Wait(ctx context.Context, runID RunID) (*Result, error) {
	// Poll persistence until the run is no longer running.
	// TODO: Use channels for more efficient notification.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			run, err := e.persistence.LoadRun(ctx, runID)
			if err != nil {
				return nil, err
			}
			if run.Status != StatusRunningRun {
				return e.buildResult(run), nil
			}
		}
	}
}

// Cancel cancels a running workflow.
func (e *Engine) Cancel(ctx context.Context, runID RunID) error {
	run, err := e.persistence.LoadRun(ctx, runID)
	if err != nil {
		return err
	}
	run.Status = StatusCancelled
	run.UpdatedAt = time.Now()
	return e.persistence.SaveRun(ctx, run)
}

// Resume resumes a suspended workflow with the provided data.
func (e *Engine) Resume(ctx context.Context, runID RunID, data P) error {
	run, err := e.persistence.LoadRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status != StatusSuspended {
		return fmt.Errorf("run %s is not suspended (status: %s)", runID, run.Status)
	}

	// TODO: Reload the workflow from the checkpoint, inject resume data,
	// and continue execution from where it was suspended.
	// For now, just mark it as running.
	run.Status = StatusRunningRun
	run.UpdatedAt = time.Now()
	return e.persistence.SaveRun(ctx, run)
}

// Close releases engine resources.
func (e *Engine) Close() error {
	return e.persistence.Close()
}

// executeRun runs the workflow and updates persistence.
func (e *Engine) executeRun(ctx context.Context, runID RunID, wf *Workflow) *Result {
	err := e.runner.Execute(ctx, wf, e.logger)

	run, loadErr := e.persistence.LoadRun(ctx, runID)
	if loadErr != nil {
		return &Result{
			RunID:  runID,
			Status: StatusFailedRun,
			Error:  loadErr,
		}
	}

	// Checkpoint the final store state
	storeData := wf.Store.ExportAll()
	if jsonBytes, marshalErr := json.Marshal(storeData); marshalErr == nil {
		e.persistence.SaveCheckpoint(ctx, runID, jsonBytes)
	}

	// Determine final status by unwrapping the error chain
	if err != nil {
		var bailErr *BailError
		var suspendErr *SuspendError
		if errors.As(err, &bailErr) {
			run.Status = StatusBailed
			run.BailReason = bailErr.Reason
		} else if errors.As(err, &suspendErr) {
			run.Status = StatusSuspended
			run.SuspendData = suspendErr.Data
		} else {
			run.Status = StatusFailedRun
		}
	} else {
		run.Status = StatusCompletedRun
	}

	run.UpdatedAt = time.Now()
	e.persistence.SaveRun(ctx, run)

	return e.buildResult(run)
}

// buildResult converts a RunState to a Result.
func (e *Engine) buildResult(run *RunState) *Result {
	result := &Result{
		RunID:      run.RunID,
		Status:     run.Status,
		BailReason: run.BailReason,
	}

	if run.Status == StatusFailedRun {
		result.Error = fmt.Errorf("workflow %s failed", run.WorkflowID)
	}

	// Load the checkpoint to get the final store state
	if data, err := e.persistence.LoadCheckpoint(context.Background(), run.RunID); err == nil {
		var storeData map[string]any
		if json.Unmarshal(data, &storeData) == nil {
			result.Store = storeData
		}
	}

	return result
}
