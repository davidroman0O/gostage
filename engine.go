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
	cacheOrder      []string // insertion order for eviction
	cacheSize       int      // max cached workflows; 0 = unlimited

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

	// autoRecover enables crash recovery on startup.
	autoRecover bool

	// stateLimit is the max entries per run state, 0 = unlimited.
	stateLimit int

	// shutdownTimeout is how long Close() waits for workers, 0 = indefinite.
	shutdownTimeout time.Duration
}

// runHandle tracks an in-flight workflow execution.
type runHandle struct {
	cancel context.CancelFunc
	done   chan *Result
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
		runs:            make(map[RunID]*runHandle),
		closeCh:         make(chan struct{}),
		workflowCache:   make(map[string]*Workflow),
		messageHandlers: make(map[string][]MessageHandler),
		cacheSize:       defaultCacheSize,
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
//	result, err := engine.RunSync(ctx, wf, gostage.P{"order_id": "ORD-123"})
func (e *Engine) RunSync(ctx context.Context, wf *Workflow, params P) (*Result, error) {
	if wf == nil {
		return nil, ErrNilWorkflow
	}
	runID := RunID(uuid.New().String())

	// Apply timeout if configured
	if e.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.timeout)
		defer cancel()
	}

	return e.executeRun(ctx, wf, runID, params)
}

// Run starts a workflow execution asynchronously and returns the run ID.
//
//	runID, err := engine.Run(ctx, wf, gostage.P{"order_id": "ORD-123"})
func (e *Engine) Run(ctx context.Context, wf *Workflow, params P) (RunID, error) {
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
		cancel: cancel,
		done:   make(chan *Result, 1),
	}

	e.mu.Lock()
	e.runs[runID] = handle
	e.mu.Unlock()

	if !e.pool.Submit(func() {
		result, _ := e.executeRun(runCtx, wf, runID, params)
		if result == nil {
			result = &Result{RunID: runID, Status: Failed, Error: fmt.Errorf("execution returned nil result")}
		}
		select {
		case handle.done <- result:
		default:
		}
		e.mu.Lock()
		delete(e.runs, runID)
		e.mu.Unlock()
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
			return nil, fmt.Errorf("run %s not found", runID)
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

	run.Status = Cancelled
	run.UpdatedAt = time.Now()
	return e.persistence.SaveRun(ctx, run)
}

// Resume resumes a suspended workflow with the provided data.
//
//	result, err := engine.Resume(ctx, runID, gostage.P{"approved": true})
func (e *Engine) Resume(ctx context.Context, wf *Workflow, runID RunID, data P) (*Result, error) {
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
		cancel: runCancel,
		done:   make(chan *Result, 1),
	}
	e.runs[runID] = handle
	e.mu.Unlock()

	// Clean up slot when done
	defer func() {
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

	wf = wf.clone()

	// Restore state from persistence
	wf.state = newRunState(runID, e.persistence)
	wf.state.setLimit(e.stateLimit)
	if err := wf.state.LoadFromPersistence(ctx); err != nil {
		return nil, fmt.Errorf("load state for resume: %w", err)
	}

	// Replay dynamic mutations from the suspended run
	if len(run.Mutations) > 0 {
		replayMutations(wf, run.Mutations)
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
		h(msgType, payload)
	}
	for _, h := range wildcardHandlers {
		h(msgType, payload)
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
			}
		}

		// 4. Close persistence (last — active runs may flush during shutdown)
		closeErr = e.persistence.Close()
	})
	return closeErr
}

// cacheWorkflow stores a workflow in the engine's cache for timer-based recovery.
func (e *Engine) cacheWorkflow(wf *Workflow) {
	e.workflowCacheMu.Lock()
	defer e.workflowCacheMu.Unlock()

	if _, exists := e.workflowCache[wf.ID]; !exists {
		// Evict oldest if at capacity
		if e.cacheSize > 0 && len(e.workflowCache) >= e.cacheSize {
			oldest := e.cacheOrder[0]
			delete(e.workflowCache, oldest)
			e.cacheOrder = e.cacheOrder[1:]
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
			cancel: cancel,
			done:   make(chan *Result, 1),
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
		if cached == nil {
			// Workflow not in cache — mark as failed
			run.Status = Failed
			run.UpdatedAt = time.Now()
			if err := e.persistence.SaveRun(ctx, run); err != nil {
				e.logger.Warn("persist failed wake (no cache): %v", err)
			}
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
				e.logger.Warn("persist failed wake (state load): %v", saveErr)
			}
			return
		}

		// Replay dynamic mutations from the sleeping run
		if len(run.Mutations) > 0 {
			replayMutations(wf, run.Mutations)
		}

		// Update status to Running
		run.Status = Running
		run.UpdatedAt = time.Now()
		if err := e.persistence.SaveRun(ctx, run); err != nil {
			e.logger.Warn("persist wake status update: %v", err)
		}

		// Re-execute (resuming skips completed steps)
		result := e.doExecute(ctx, wf, runID, true)

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
			e.logger.Warn("persist recovery (mark failed): %v", err)
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
				e.logger.Warn("persist recovery (no wake time): %v", err)
			}
			continue
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
func (e *Engine) executeRun(ctx context.Context, wf *Workflow, runID RunID, params P) (*Result, error) {
	// Cache the original (immutable template) for timer-based recovery
	e.cacheWorkflow(wf)

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
		RunID:      runID,
		WorkflowID: wf.ID,
		Status:     Running,
		StepStates: make(map[string]Status),
		CreatedAt:  now,
		UpdatedAt:  now,
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
		// Capture dynamic mutations for resume/wake replay
		if result.Status == Suspended || result.Status == Sleeping {
			run.Mutations = captureDynamicState(wf)
		}
		if saveErr := e.persistence.SaveRun(bgCtx, run); saveErr != nil {
			e.logger.Warn("persist final run state: %v", saveErr)
		}
	}

	// Final flush: write any remaining dirty state to persistence
	if flushErr := wf.state.Flush(bgCtx); flushErr != nil {
		e.logger.Warn("flush final state: %v", flushErr)
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
