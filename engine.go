package gostage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Engine orchestrates workflow execution with persistence and checkpointing.
type Engine struct {
	persistence Persistence
	logger      Logger
	timeout     time.Duration
	pool        *workerPool
	poolSize    int // 0 = default

	mu        sync.Mutex
	runs      map[RunID]*runHandle
	closeCh   chan struct{}
	closeOnce sync.Once

	// workflowCache stores workflows by ID for timer-based recovery.
	workflowCacheMu sync.RWMutex
	workflowCache   map[string]*Workflow
	cacheOrder      []string       // insertion order for eviction
	cacheSize       int            // max cached workflows; 0 = unlimited
	pinnedWorkflows map[string]int // workflow ID → count of sleeping runs (protected from eviction)

	// messageHandlers stores IPC message handler callbacks.
	messageHandlersMu sync.RWMutex
	messageHandlers   map[string][]MessageHandler

	// scheduler manages timed wakeups for sleeping workflows.
	scheduler *timerScheduler

	// middleware chains
	engineMiddleware []EngineMiddleware
	stepMiddleware   []StepMiddleware
	taskMiddleware   []TaskMiddleware
	childMiddleware  []ChildMiddleware

	// registry holds task, condition, and map function registrations.
	registry *Registry

	// autoRecover enables crash recovery on startup.
	autoRecover bool

	// stateLimit is the max entries per run state, 0 = unlimited.
	stateLimit int

	// shutdownTimeout is how long Close() waits for workers, 0 = indefinite.
	shutdownTimeout time.Duration
}

// runHandle tracks an in-flight workflow execution.
type runHandle struct {
	cancel   context.CancelFunc
	done     chan *Result
	finished chan struct{} // closed when all persistence writes are done
}

const defaultCacheSize = 1000

// EngineOption configures the engine.
type EngineOption func(*Engine) error

// New creates a new Engine with the given options.
// If no persistence is configured, an in-memory store is used.
//
//	engine, err := gostage.New(gostage.WithSQLite("app.db"))
func New(opts ...EngineOption) (*Engine, error) {
	e := &Engine{
		persistence:     newMemoryPersistence(),
		logger:          NewDefaultLogger(),
		registry:        defaultRegistry,
		runs:            make(map[RunID]*runHandle),
		closeCh:         make(chan struct{}),
		workflowCache:   make(map[string]*Workflow),
		messageHandlers: make(map[string][]MessageHandler),
		cacheSize:       defaultCacheSize,
		pinnedWorkflows: make(map[string]int),
	}

	for _, opt := range opts {
		if err := opt(e); err != nil {
			return nil, err
		}
	}

	// Start worker pool (default size if not configured)
	e.pool = newWorkerPool(e.poolSize)

	// Start timer scheduler for non-blocking sleep
	e.scheduler = newTimerScheduler(e.wakeWorkflow)

	// Auto-recover if configured
	if e.autoRecover {
		if err := e.Recover(context.Background()); err != nil {
			return nil, fmt.Errorf("auto recover: %w", err)
		}
	}

	return e, nil
}

// RunSync executes a workflow synchronously and returns the result.
//
//	result, err := engine.RunSync(ctx, wf, gostage.Params{"order_id": "ORD-123"})
func (e *Engine) RunSync(ctx context.Context, wf *Workflow, params Params) (*Result, error) {
	// Guard: reject if engine is closed
	select {
	case <-e.closeCh:
		return nil, ErrEngineClosed
	default:
	}

	if wf == nil {
		return nil, ErrNilWorkflow
	}
	runID := RunID(uuid.New().String())

	// Apply timeout if configured
	runCtx := ctx
	var timeoutCancel context.CancelFunc
	if e.timeout > 0 {
		runCtx, timeoutCancel = context.WithTimeout(ctx, e.timeout)
	}

	// Create cancellable context for lifecycle management
	runCtx, cancel := context.WithCancel(runCtx)

	// Register with run tracking so Cancel/Close can reach us
	handle := &runHandle{
		cancel:   cancel,
		done:     make(chan *Result, 1),
		finished: make(chan struct{}),
	}
	e.mu.Lock()
	e.runs[runID] = handle
	e.mu.Unlock()

	defer func() {
		close(handle.finished)
		e.mu.Lock()
		delete(e.runs, runID)
		e.mu.Unlock()
		cancel()
		if timeoutCancel != nil {
			timeoutCancel()
		}
	}()

	return e.executeRun(runCtx, wf, runID, params)
}

// Run starts a workflow execution asynchronously and returns the run ID.
//
//	runID, err := engine.Run(ctx, wf, gostage.Params{"order_id": "ORD-123"})
func (e *Engine) Run(ctx context.Context, wf *Workflow, params Params) (RunID, error) {
	// Guard: reject if engine is closed
	select {
	case <-e.closeCh:
		return "", ErrEngineClosed
	default:
	}

	if wf == nil {
		return "", ErrNilWorkflow
	}

	runID := RunID(uuid.New().String())

	runCtx, cancel := context.WithCancel(ctx)

	handle := &runHandle{
		cancel:   cancel,
		done:     make(chan *Result, 1),
		finished: make(chan struct{}),
	}

	e.mu.Lock()
	e.runs[runID] = handle
	e.mu.Unlock()

	if !e.pool.Submit(func() {
		defer func() {
			close(handle.finished)
			e.mu.Lock()
			delete(e.runs, runID)
			e.mu.Unlock()
		}()
		result, _ := e.executeRun(runCtx, wf, runID, params)
		if result == nil {
			result = &Result{RunID: runID, Status: Failed, Error: fmt.Errorf("execution returned nil result")}
		}
		select {
		case handle.done <- result:
		default:
		}
	}) {
		e.mu.Lock()
		delete(e.runs, runID)
		e.mu.Unlock()
		cancel()
		return "", ErrEngineClosed
	}

	return runID, nil
}

// Wait blocks until a run completes and returns the result.
//
//	result, err := engine.Wait(ctx, runID)
func (e *Engine) Wait(ctx context.Context, runID RunID) (*Result, error) {
	e.mu.Lock()
	handle, ok := e.runs[runID]
	e.mu.Unlock()

	if !ok {
		// Run may have already completed via RunSync, check persistence
		run, err := e.persistence.LoadRun(ctx, runID)
		if err != nil {
			return nil, &RunNotFoundError{RunID: runID}
		}
		return &Result{
			RunID:      run.RunID,
			Status:     run.Status,
			BailReason: run.BailReason,
		}, nil
	}

	select {
	case result := <-handle.done:
		e.mu.Lock()
		delete(e.runs, runID)
		e.mu.Unlock()
		return result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Cancel cancels a running workflow.
func (e *Engine) Cancel(ctx context.Context, runID RunID) error {
	e.mu.Lock()
	handle, ok := e.runs[runID]
	e.mu.Unlock()

	if ok {
		handle.cancel()
	}

	run, err := e.persistence.LoadRun(ctx, runID)
	if err != nil {
		return err
	}

	// If the run was sleeping, cancel its timer and unpin the workflow
	if run.Status == Sleeping {
		e.scheduler.Cancel(runID)
		e.workflowCacheMu.Lock()
		if e.pinnedWorkflows[run.WorkflowID] > 0 {
			e.pinnedWorkflows[run.WorkflowID]--
		}
		e.workflowCacheMu.Unlock()
	}

	run.Status = Cancelled
	run.UpdatedAt = time.Now()
	return e.persistence.SaveRun(ctx, run)
}

// DeleteRun removes a run and all its associated state from persistence.
// If the run is currently active, it is cancelled and DeleteRun waits for
// all in-flight persistence writes to complete before deleting.
func (e *Engine) DeleteRun(ctx context.Context, runID RunID) error {
	e.mu.Lock()
	handle, active := e.runs[runID]
	e.mu.Unlock()
	if active {
		handle.cancel()
		// Wait for in-flight execution to finish all persistence writes
		select {
		case <-handle.finished:
		case <-ctx.Done():
			// Caller doesn't want to wait — proceed with deletion anyway
		}
	}
	// Cancel any scheduled wake timer for sleeping runs
	e.scheduler.Cancel(runID)
	// Unpin workflow if this was a sleeping run
	run, loadErr := e.persistence.LoadRun(ctx, runID)
	if loadErr == nil && run.Status == Sleeping {
		e.workflowCacheMu.Lock()
		if e.pinnedWorkflows[run.WorkflowID] > 0 {
			e.pinnedWorkflows[run.WorkflowID]--
		}
		e.workflowCacheMu.Unlock()
	}
	return e.persistence.DeleteRun(ctx, runID)
}

// Resume resumes a suspended workflow with the provided data.
//
//	result, err := engine.Resume(ctx, runID, gostage.Params{"approved": true})
func (e *Engine) Resume(ctx context.Context, wf *Workflow, runID RunID, data Params) (*Result, error) {
	// Guard: reject if engine is closed
	select {
	case <-e.closeCh:
		return nil, ErrEngineClosed
	default:
	}

	if wf == nil {
		return nil, ErrNilWorkflow
	}

	// Concurrent resume prevention: reserve slot before proceeding
	runCtx, runCancel := context.WithCancel(ctx)

	e.mu.Lock()
	if _, active := e.runs[runID]; active {
		e.mu.Unlock()
		runCancel()
		return nil, fmt.Errorf("%w: %s", ErrRunAlreadyActive, runID)
	}
	handle := &runHandle{
		cancel:   runCancel,
		done:     make(chan *Result, 1),
		finished: make(chan struct{}),
	}
	e.runs[runID] = handle
	e.mu.Unlock()

	// Clean up slot when done
	defer func() {
		close(handle.finished)
		e.mu.Lock()
		delete(e.runs, runID)
		e.mu.Unlock()
	}()

	run, err := e.persistence.LoadRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrRunNotFound, runID)
	}

	if run.Status != Suspended {
		return nil, fmt.Errorf("%w: status is %s", ErrRunNotSuspended, run.Status)
	}

	if wf.ID != run.WorkflowID {
		return nil, fmt.Errorf("%w: expected %s, got %s", ErrWorkflowMismatch, run.WorkflowID, wf.ID)
	}

	// Verify the workflow has at least as many steps as the run has completed.
	completedCount := 0
	for _, status := range run.StepStates {
		if status == Completed {
			completedCount++
		}
	}
	if len(wf.steps) < completedCount {
		return nil, fmt.Errorf("%w: workflow has %d steps but run completed %d",
			ErrWorkflowMismatch, len(wf.steps), completedCount)
	}

	wf = wf.clone()

	// Restore state from persistence
	wf.state = newRunState(runID, e.persistence)
	wf.state.setLimit(e.stateLimit)
	if err := wf.state.LoadFromPersistence(ctx); err != nil {
		return nil, fmt.Errorf("load state for resume: %w", err)
	}

	// Restore dynamic step counter and replay mutations from the suspended run
	wf.dynCounter = run.DynCounter
	if len(run.Mutations) > 0 {
		replayMutations(wf, run.Mutations, e.registry)
	}

	// Inject resume data
	wf.state.Set("__resuming", true)
	for k, v := range data {
		wf.state.Set("__resume:"+k, v)
	}

	// Update run status to Running
	run.Status = Running
	run.UpdatedAt = time.Now()
	if err := e.persistence.SaveRun(ctx, run); err != nil {
		return nil, fmt.Errorf("save run status for resume: %w", err)
	}

	// Re-execute the workflow (the task with IsResuming check will take the resume path)
	result := e.doExecute(runCtx, wf, runID, true)

	return result, nil
}

// OnMessage registers a handler for IPC messages of the given type.
// Handlers fire when a child process sends a message via Send(),
// or when Send() is called in the parent process (local routing).
func (e *Engine) OnMessage(msgType string, handler MessageHandler) {
	if handler == nil {
		return
	}
	e.messageHandlersMu.Lock()
	defer e.messageHandlersMu.Unlock()
	e.messageHandlers[msgType] = append(e.messageHandlers[msgType], handler)
}

// dispatchMessage routes an IPC message to all registered handlers.
func (e *Engine) dispatchMessage(msgType string, payload map[string]any) {
	e.messageHandlersMu.RLock()
	handlers := e.messageHandlers[msgType]
	// Also fire wildcard handlers (registered with "*")
	wildcardHandlers := e.messageHandlers["*"]
	e.messageHandlersMu.RUnlock()

	for _, h := range handlers {
		func() {
			defer func() {
				if r := recover(); r != nil {
					e.logger.Error("message handler panicked for %q: %v", msgType, r)
				}
			}()
			h(msgType, payload)
		}()
	}
	for _, h := range wildcardHandlers {
		func() {
			defer func() {
				if r := recover(); r != nil {
					e.logger.Error("wildcard message handler panicked for %q: %v", msgType, r)
				}
			}()
			h(msgType, payload)
		}()
	}
}

// Close releases engine resources with ordered shutdown.
// Safe to call multiple times — the second and subsequent calls are no-ops.
func (e *Engine) Close() error {
	var closeErr error
	e.closeOnce.Do(func() {
		close(e.closeCh)

		// 1. Stop scheduler (no more timer fires)
		if e.scheduler != nil {
			e.scheduler.Stop()
		}

		// 2. Cancel all active runs
		e.mu.Lock()
		for _, handle := range e.runs {
			if handle.cancel != nil {
				handle.cancel()
			}
		}
		e.mu.Unlock()

		// 3. Drain worker pool (waits for in-flight jobs)
		if e.pool != nil {
			if err := e.pool.Shutdown(e.shutdownTimeout); err != nil {
				e.logger.Warn("pool shutdown: %v", err)
				// Shutdown timed out — workers may still be doing final bgCtx
				// persistence writes. Wait a bounded grace period for those to finish
				// before closing the persistence layer.
				if !e.pool.WaitAll(time.Second) {
					e.logger.Warn("some workers did not finish within grace period")
				}
			}
		}

		// 4. Close persistence (last — active runs may flush during shutdown)
		closeErr = e.persistence.Close()
	})
	return closeErr
}

// cacheWorkflow stores a workflow in the engine's cache for timer-based recovery.
// Uses LRU eviction: accessed entries are promoted to the end of cacheOrder.
func (e *Engine) cacheWorkflow(wf *Workflow) {
	e.workflowCacheMu.Lock()
	defer e.workflowCacheMu.Unlock()

	if _, exists := e.workflowCache[wf.ID]; exists {
		// LRU: move to end of order
		for i, id := range e.cacheOrder {
			if id == wf.ID {
				e.cacheOrder = append(e.cacheOrder[:i], e.cacheOrder[i+1:]...)
				break
			}
		}
		e.cacheOrder = append(e.cacheOrder, wf.ID)
	} else {
		// New entry: evict LRU if at capacity (skip pinned entries)
		if e.cacheSize > 0 && len(e.workflowCache) >= e.cacheSize {
			evicted := false
			for i := 0; i < len(e.cacheOrder); i++ {
				if e.pinnedWorkflows[e.cacheOrder[i]] == 0 {
					delete(e.workflowCache, e.cacheOrder[i])
					e.cacheOrder = append(e.cacheOrder[:i], e.cacheOrder[i+1:]...)
					evicted = true
					break
				}
			}
			if !evicted {
				// All entries are pinned — allow cache to grow beyond limit.
				// This is safer than losing a sleeping workflow.
				e.logger.Warn("workflow cache exceeds limit: all %d entries are pinned by sleeping runs", len(e.workflowCache))
			}
		}
		e.cacheOrder = append(e.cacheOrder, wf.ID)
	}
	e.workflowCache[wf.ID] = wf
}

// lookupCachedWorkflow retrieves a workflow from the cache.
func (e *Engine) lookupCachedWorkflow(id string) *Workflow {
	e.workflowCacheMu.RLock()
	defer e.workflowCacheMu.RUnlock()
	return e.workflowCache[id]
}

// wakeWorkflow is called by the timer scheduler when a sleeping workflow's timer fires.
func (e *Engine) wakeWorkflow(runID RunID) {
	// Don't wake if engine is shutting down
	select {
	case <-e.closeCh:
		return
	default:
	}

	if !e.pool.Submit(func() {
		// Create cancellable context so woken workflows can be cancelled
		ctx, cancel := context.WithCancel(context.Background())

		// Register handle BEFORE execution so Cancel() can reach this run
		handle := &runHandle{
			cancel:   cancel,
			done:     make(chan *Result, 1),
			finished: make(chan struct{}),
		}
		e.mu.Lock()
		if _, active := e.runs[runID]; active {
			e.mu.Unlock()
			cancel()
			return // already running
		}
		e.runs[runID] = handle
		e.mu.Unlock()

		// Deferred cleanup
		defer func() {
			close(handle.finished)
			e.mu.Lock()
			delete(e.runs, runID)
			e.mu.Unlock()
			cancel()
		}()

		run, err := e.persistence.LoadRun(ctx, runID)
		if err != nil || run.Status != Sleeping {
			return
		}

		cached := e.lookupCachedWorkflow(run.WorkflowID)
		if cached == nil && run.WorkflowDef != nil {
			// Cache miss — try to rebuild from persisted definition
			if rebuilt, rebuildErr := NewWorkflowFromDefWithRegistry(run.WorkflowDef, e.registry); rebuildErr == nil {
				cached = rebuilt
				e.cacheWorkflow(rebuilt)
			} else {
				e.logger.Warn("rebuild workflow from definition: %v", rebuildErr)
			}
		}
		if cached == nil {
			// Workflow not in cache and can't be rebuilt — mark as failed
			run.Status = Failed
			run.UpdatedAt = time.Now()
			if err := e.persistence.SaveRun(ctx, run); err != nil {
				e.logger.Error("persist failed wake (no cache): %v", err)
			}
			// Unpin (defensive — should not happen if pinning is working correctly)
			e.workflowCacheMu.Lock()
			if e.pinnedWorkflows[run.WorkflowID] > 0 {
				e.pinnedWorkflows[run.WorkflowID]--
			}
			e.workflowCacheMu.Unlock()
			return
		}

		// Clone so this wake doesn't mutate the cached template
		wf := cached.clone()

		// Restore state from persistence
		wf.state = newRunState(runID, e.persistence)
		wf.state.setLimit(e.stateLimit)
		if err := wf.state.LoadFromPersistence(ctx); err != nil {
			run.Status = Failed
			run.UpdatedAt = time.Now()
			if saveErr := e.persistence.SaveRun(ctx, run); saveErr != nil {
				e.logger.Error("persist failed wake (state load): %v", saveErr)
			}
			return
		}

		// Restore dynamic step counter and replay mutations from the sleeping run
		wf.dynCounter = run.DynCounter
		if len(run.Mutations) > 0 {
			replayMutations(wf, run.Mutations, e.registry)
		}

		// Mark the sleep step as completed so the executor skips it on resume.
		// The sleep step returned sleepError before being marked Completed,
		// so without this the executor would re-execute the sleep and loop forever.
		if run.CurrentStep != "" {
			if err := e.persistence.UpdateStepStatus(ctx, runID, run.CurrentStep, Completed); err != nil {
				// CRITICAL: without this, the sleep step re-executes on every wake, looping forever.
				e.logger.Error("persist wake step completed: %v", err)
				run.Status = Failed
				run.UpdatedAt = time.Now()
				if saveErr := e.persistence.SaveRun(ctx, run); saveErr != nil {
					e.logger.Error("persist failed wake (step complete): %v", saveErr)
				}
				return
			}
			// Also update local copy — SaveRun below overwrites step_states,
			// so without this the UpdateStepStatus write is lost.
			if run.StepStates == nil {
				run.StepStates = make(map[string]Status)
			}
			run.StepStates[run.CurrentStep] = Completed
		}

		// Update status to Running
		run.Status = Running
		run.UpdatedAt = time.Now()
		if err := e.persistence.SaveRun(ctx, run); err != nil {
			// Cannot proceed without marking the run as Running — abort.
			e.logger.Error("persist wake status update: %v", err)
			return
		}

		// Re-execute (resuming skips completed steps)
		result := e.doExecute(ctx, wf, runID, true)

		// Unpin workflow — this sleeping run is done
		e.workflowCacheMu.Lock()
		if e.pinnedWorkflows[run.WorkflowID] > 0 {
			e.pinnedWorkflows[run.WorkflowID]--
		}
		e.workflowCacheMu.Unlock()

		// Send result to waiting handle
		select {
		case handle.done <- result:
		default:
		}
	}) {
		e.logger.Warn("failed to wake workflow %s: pool is closed", runID)
	}
}

// Recover scans persistence for interrupted workflows and handles them.
// - Running workflows are marked as Failed (crashed mid-execution)
// - Sleeping workflows past their wake time are resumed immediately
// - Sleeping workflows with future wake times are registered with the timer scheduler
// - Suspended workflows are left alone (waiting for external input)
func (e *Engine) Recover(ctx context.Context) error {
	// Recover crashed running workflows
	runningRuns, err := e.persistence.ListRuns(ctx, RunFilter{Status: Running})
	if err != nil {
		return fmt.Errorf("list running runs: %w", err)
	}
	for _, run := range runningRuns {
		run.Status = Failed
		run.UpdatedAt = time.Now()
		if err := e.persistence.SaveRun(ctx, run); err != nil {
			e.logger.Error("persist recovery (mark failed): %v", err)
		}
	}

	// Recover sleeping workflows
	sleepingRuns, err := e.persistence.ListRuns(ctx, RunFilter{Status: Sleeping})
	if err != nil {
		return fmt.Errorf("list sleeping runs: %w", err)
	}

	now := time.Now()
	for _, run := range sleepingRuns {
		if run.WakeAt.IsZero() {
			// No wake time — mark as failed
			run.Status = Failed
			run.UpdatedAt = time.Now()
			if err := e.persistence.SaveRun(ctx, run); err != nil {
				e.logger.Error("persist recovery (no wake time): %v", err)
			}
			continue
		}

		// Pre-populate cache from persisted definition so wakeWorkflow
		// can find the workflow without relying on in-memory cache.
		if e.lookupCachedWorkflow(run.WorkflowID) == nil && run.WorkflowDef != nil {
			if rebuilt, rebuildErr := NewWorkflowFromDefWithRegistry(run.WorkflowDef, e.registry); rebuildErr == nil {
				e.cacheWorkflow(rebuilt)
			} else {
				e.logger.Warn("rebuild workflow from definition during recovery: %v", rebuildErr)
			}
		}

		if !run.WakeAt.After(now) {
			// Wake time has passed — resume immediately
			e.wakeWorkflow(run.RunID)
		} else {
			// Wake time in the future — register with scheduler
			e.scheduler.Schedule(run.RunID, run.WakeAt)
		}
	}

	return nil
}

// executeRun is the core execution path shared by RunSync and Run.
// It clones the workflow so each run has independent mutable state.
func (e *Engine) executeRun(ctx context.Context, wf *Workflow, runID RunID, params Params) (*Result, error) {
	// Cache the original (immutable template) for timer-based recovery
	e.cacheWorkflow(wf)

	// Attempt to serialize workflow definition for persistence-based recovery.
	// Anonymous-closure workflows will fail serialization — that's fine,
	// the cache is the primary recovery path for those.
	var wfDef *WorkflowDef
	if def, err := WorkflowToDefinition(wf); err == nil {
		wfDef = def
	}

	// Clone so this run has its own state, mutation queue, and step state
	wf = wf.clone()

	// Create per-run state backed by persistence
	wf.state = newRunState(runID, e.persistence)
	wf.state.setLimit(e.stateLimit)

	// Initialize state with params
	for k, v := range params {
		wf.state.Set(k, v)
	}

	now := time.Now()
	run := &RunState{
		RunID:       runID,
		WorkflowID:  wf.ID,
		Status:      Running,
		StepStates:  make(map[string]Status),
		WorkflowDef: wfDef,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := e.persistence.SaveRun(ctx, run); err != nil {
		return nil, fmt.Errorf("save initial run: %w", err)
	}

	result := e.doExecute(ctx, wf, runID, false)
	return result, nil
}

// doExecute runs the workflow and translates errors to Result.
func (e *Engine) doExecute(ctx context.Context, wf *Workflow, runID RunID, resuming bool) *Result {
	// Execute with engine middleware chain
	var err error
	if len(e.engineMiddleware) > 0 {
		final := func() error {
			return e.executeWorkflow(ctx, wf, runID, resuming)
		}
		chain := final
		for i := 0; i < len(e.engineMiddleware); i++ {
			mw := e.engineMiddleware[i]
			next := chain
			chain = func() error {
				return mw(ctx, wf, runID, next)
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
		err = e.executeWorkflow(ctx, wf, runID, resuming)
	}

	// Clean up internal double-underscore keys before building the result
	// so they don't leak into the caller's result store.
	for _, key := range wf.state.Keys() {
		if strings.HasPrefix(key, "__") {
			wf.state.Delete(key)
		}
	}

	result := &Result{
		RunID: runID,
		Store: wf.state.ExportAll(),
	}

	switch {
	case err == nil:
		result.Status = Completed

	case errors.As(err, new(*BailError)):
		var bailErr *BailError
		errors.As(err, &bailErr)
		result.Status = Bailed
		result.BailReason = bailErr.Reason

	case errors.As(err, new(*SuspendError)):
		var suspErr *SuspendError
		errors.As(err, &suspErr)
		result.Status = Suspended
		result.SuspendData = suspErr.Data

	case errors.As(err, new(*sleepError)):
		result.Status = Sleeping
		var sleepErr *sleepError
		errors.As(err, &sleepErr)
		result.SuspendData = map[string]any{"wake_at": sleepErr.wakeAt.Format(time.RFC3339Nano)}

		// Non-blocking: schedule with timer scheduler so goroutine can exit
		if e.scheduler != nil {
			e.scheduler.Schedule(runID, sleepErr.wakeAt)
		}

		// Pin workflow in cache so eviction doesn't kill sleeping runs
		e.workflowCacheMu.Lock()
		e.pinnedWorkflows[wf.ID]++
		e.workflowCacheMu.Unlock()

	case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		result.Status = Cancelled
		result.Error = err

	default:
		result.Status = Failed
		result.Error = err
	}

	// Update persistence — use background context so writes succeed even if
	// the workflow's context was cancelled.
	bgCtx := context.Background()
	run, loadErr := e.persistence.LoadRun(bgCtx, runID)
	if loadErr == nil {
		run.Status = result.Status
		run.UpdatedAt = time.Now()
		if result.BailReason != "" {
			run.BailReason = result.BailReason
		}
		if result.Status == Suspended {
			var suspendErr *SuspendError
			if errors.As(err, &suspendErr) {
				run.SuspendData = suspendErr.Data
			}
		}
		if result.Status == Sleeping {
			var sleepErr *sleepError
			if errors.As(err, &sleepErr) {
				run.WakeAt = sleepErr.wakeAt
			}
		}
		// Capture dynamic mutations and counter for resume/wake replay
		if result.Status == Suspended || result.Status == Sleeping {
			run.Mutations = captureDynamicState(wf)
			run.DynCounter = wf.dynCounter
		}
		if saveErr := e.persistence.SaveRun(bgCtx, run); saveErr != nil {
			e.logger.Error("persist final run state: %v", saveErr)
		}
	}

	// Final flush: write any remaining dirty state to persistence
	if flushErr := wf.state.Flush(bgCtx); flushErr != nil {
		e.logger.Error("flush final state: %v", flushErr)
	}

	// Clean up persisted state for terminal runs — the final state is captured in Result.Store
	switch result.Status {
	case Completed, Failed, Bailed, Cancelled:
		if delErr := e.persistence.DeleteState(bgCtx, runID); delErr != nil {
			e.logger.Warn("cleanup terminal state: %v", delErr)
		}
	}

	return result
}

// ListRuns returns runs matching the given filter criteria.
// Delegates to the persistence layer's filter-based query.
func (e *Engine) ListRuns(ctx context.Context, filter RunFilter) ([]*RunState, error) {
	select {
	case <-e.closeCh:
		return nil, ErrEngineClosed
	default:
	}
	return e.persistence.ListRuns(ctx, filter)
}

// GetRun returns the current state of a run without blocking.
// Unlike Wait, this returns immediately regardless of run status.
func (e *Engine) GetRun(ctx context.Context, runID RunID) (*RunState, error) {
	select {
	case <-e.closeCh:
		return nil, ErrEngineClosed
	default:
	}
	return e.persistence.LoadRun(ctx, runID)
}
