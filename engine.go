package gostage

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Engine orchestrates durable workflow execution with persistence, checkpointing,
// and timer-based sleep/wake scheduling. It manages a bounded worker pool for
// asynchronous runs, an LRU workflow cache for resume and wake operations, and
// a registry of IPC message handlers for child process communication.
//
// An Engine is safe for concurrent use by multiple goroutines. All public methods
// guard against use after Close by checking an internal shutdown channel and
// returning ErrEngineClosed.
//
// Create an Engine with New and shut it down with Close when finished:
//
//	engine, err := gostage.New(gostage.WithSQLite("app.db"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer engine.Close()
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
	workflowCacheMu   sync.RWMutex
	workflowCache     map[string]*Workflow
	workflowCacheList *list.List              // front = MRU; back = LRU
	workflowCacheNode map[string]*list.Element // wf.ID → list node for O(1) promotion
	cacheSize         int                      // max cached workflows; 0 = unlimited
	pinnedWorkflows   map[string]int           // workflow ID → count of sleeping runs (protected from eviction)

	// messageHandlers stores IPC message handler callbacks.
	messageHandlersMu      sync.RWMutex
	messageHandlers        map[string][]handlerEntry // msgType → entries
	messageHandlerIndex    map[HandlerID]string      // id → msgType for O(1) OffMessage
	messageHandlerCounter  atomic.Int64

	// scheduler manages timed wakeups for sleeping workflows.
	scheduler *timerScheduler

	// middleware chains
	engineMiddleware []EngineMiddleware
	stepMiddleware   []StepMiddleware
	taskMiddleware   []TaskMiddleware
	childMiddleware  []ChildMiddleware

	// registry holds task, condition, and map function registrations.
	registry *Registry

	// eventHandlers receive lifecycle events outside the middleware chain.
	eventHandlers []EventHandler

	// gcTTL and gcInterval configure background run garbage collection.
	// Zero gcInterval means GC is disabled.
	gcTTL      time.Duration
	gcInterval time.Duration

	// autoRecover enables crash recovery on startup.
	autoRecover bool

	// stateLimit is the max entries per run state, 0 = unlimited.
	stateLimit int

	// shutdownTimeout is how long Close() waits for workers, 0 = indefinite.
	shutdownTimeout time.Duration

	// noPool and noScheduler skip creating the worker pool and timer scheduler.
	// Used by HandleChild to avoid idle goroutines in short-lived child processes.
	noPool      bool
	noScheduler bool

	// spawnRunner handles ForEach with child process spawning.
	// nil when spawn support is not configured.
	spawnRunner SpawnRunner
}

// runHandle tracks an in-flight workflow execution.
type runHandle struct {
	cancel   context.CancelFunc
	done     chan *Result
	finished chan struct{} // closed when all persistence writes are done
}

// EngineOption configures an Engine during construction via New. Each option
// is applied in order, and any option may return an error to abort creation.
// See the With* functions in options.go for the available options.
type EngineOption func(*Engine) error

// New creates and starts a new Engine with the given options. If no persistence
// backend is configured, an in-memory store is used (state does not survive
// process restarts). The default logger is a no-op that discards all messages.
//
// New starts the worker pool and timer scheduler immediately. If WithAutoRecover
// is set, New also scans persistence for interrupted runs and recovers them
// before returning; a recovery failure causes New to return an error.
//
// The caller must call Close when the Engine is no longer needed to release
// resources and flush in-flight persistence writes.
//
// Returns an error if any option fails or if auto-recovery encounters an
// unrecoverable problem.
//
//	engine, err := gostage.New(
//	    gostage.WithSQLite("app.db"),
//	    gostage.WithWorkerPoolSize(8),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer engine.Close()
func New(opts ...EngineOption) (*Engine, error) {
	e := &Engine{
		persistence:     newMemoryPersistence(),
		logger:          NewDefaultLogger(),
		registry:        defaultRegistry,
		runs:            make(map[RunID]*runHandle),
		closeCh:         make(chan struct{}),
		workflowCache:     make(map[string]*Workflow),
		workflowCacheList: list.New(),
		workflowCacheNode: make(map[string]*list.Element),
		messageHandlers:     make(map[string][]handlerEntry),
		messageHandlerIndex: make(map[HandlerID]string),
		cacheSize:       defaultCacheSize,
		pinnedWorkflows: make(map[string]int),
	}

	for _, opt := range opts {
		if err := opt(e); err != nil {
			return nil, err
		}
	}

	// Start worker pool (default size if not configured).
	// Skipped for child processes that never submit async jobs.
	if !e.noPool {
		e.pool = newWorkerPool(e.poolSize)
	}

	// Start timer scheduler for non-blocking sleep.
	// Skipped for child processes that use in-memory persistence and never sleep.
	if !e.noScheduler {
		e.scheduler = newTimerScheduler(e.wakeWorkflow)
	}

	// Auto-recover if configured
	if e.autoRecover {
		if err := e.Recover(context.Background()); err != nil {
			return nil, fmt.Errorf("auto recover: %w", err)
		}
	}

	// Start run garbage collection if configured
	if e.gcInterval > 0 {
		go e.runGCLoop()
	}

	return e, nil
}

// RunSync executes a workflow synchronously, blocking the calling goroutine
// until the workflow completes, fails, suspends, bails, or enters a sleep.
// The returned Result contains the final status, any error, the bail reason or
// suspend data (if applicable), and a snapshot of the workflow's state store.
//
// The params map is written into the workflow's state store before execution
// begins, making the values available to all tasks via ctx.Get.
//
// If the engine-level timeout (WithTimeout) is configured, a deadline is applied
// to the run context. The run is also registered with the engine's tracking so
// it can be cancelled via Cancel or stopped by Close.
//
// Returns ErrEngineClosed if the engine has been shut down. Returns
// ErrNilWorkflow if wf is nil.
//
//	result, err := engine.RunSync(ctx, wf, gostage.Params{"order_id": "ORD-123"})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(result.Status) // "completed", "failed", "suspended", etc.
func (e *Engine) RunSync(ctx context.Context, wf *Workflow, params Params) (*Result, error) {
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

	// Register with run tracking so Cancel/Close can reach us.
	// Close-check and registration are atomic under the same lock
	// to prevent a run from starting on a shutting-down engine.
	handle := &runHandle{
		cancel:   cancel,
		done:     make(chan *Result, 1),
		finished: make(chan struct{}),
	}
	e.mu.Lock()
	select {
	case <-e.closeCh:
		e.mu.Unlock()
		cancel()
		if timeoutCancel != nil {
			timeoutCancel()
		}
		return nil, ErrEngineClosed
	default:
	}
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

// Run starts a workflow execution asynchronously on the engine's worker pool
// and returns the RunID immediately without waiting for completion. Use Wait
// to block until the run finishes and retrieve its Result.
//
// The workflow executes on a pooled goroutine. If the worker pool is shut down
// or the engine is closed, Run returns ErrEngineClosed. Returns ErrNilWorkflow
// if wf is nil.
//
// The provided context is propagated to the workflow execution; cancelling it
// cancels the run. Unlike RunSync, Run does not apply the engine-level timeout
// automatically -- the caller controls the context lifetime.
//
//	runID, err := engine.Run(ctx, wf, gostage.Params{"order_id": "ORD-123"})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	result, err := engine.Wait(ctx, runID)
func (e *Engine) Run(ctx context.Context, wf *Workflow, params Params) (RunID, error) {
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

	// Close-check and registration are atomic under the same lock.
	e.mu.Lock()
	select {
	case <-e.closeCh:
		e.mu.Unlock()
		cancel()
		return "", ErrEngineClosed
	default:
	}
	e.runs[runID] = handle
	e.mu.Unlock()

	if e.pool == nil {
		e.mu.Lock()
		delete(e.runs, runID)
		e.mu.Unlock()
		cancel()
		return "", ErrEngineClosed
	}

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

// Wait blocks the calling goroutine until the specified run finishes and
// returns its Result. If the run is still in-flight, Wait blocks on the
// run's internal completion channel. If the run has already finished (the
// handle is no longer tracked), Wait reconstructs the Result from the
// persistence layer, including the full state store.
//
// Returns a *RunNotFoundError (matching ErrRunNotFound via errors.Is) if the
// run ID does not exist in either the active run map or persistence. Returns
// ctx.Err() if the provided context is cancelled or times out before the run
// completes.
//
//	runID, _ := engine.Run(ctx, wf, nil)
//	result, err := engine.Wait(ctx, runID)
func (e *Engine) Wait(ctx context.Context, runID RunID) (*Result, error) {
	e.mu.Lock()
	handle, ok := e.runs[runID]
	e.mu.Unlock()

	if !ok {
		// Run may have already completed, reconstruct full result from persistence
		run, err := e.persistence.LoadRun(ctx, runID)
		if err != nil {
			return nil, &RunNotFoundError{RunID: runID}
		}
		result := &Result{
			RunID:       run.RunID,
			Status:      run.Status,
			BailReason:  run.BailReason,
			SuspendData: run.SuspendData,
		}
		// Load the state store so the result is complete
		stateEntries, stateErr := e.persistence.LoadState(ctx, runID)
		if stateErr == nil && len(stateEntries) > 0 {
			store := make(map[string]any, len(stateEntries))
			for k, entry := range stateEntries {
				var val any
				if jsonErr := json.Unmarshal(entry.Value, &val); jsonErr == nil {
					store[k] = convertType(val, entry.TypeName)
				}
			}
			result.Store = store
		}
		return result, nil
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

// Cancel cancels a workflow run. For actively running workflows, it triggers
// the context cancellation so the executing goroutine observes the signal.
// For sleeping workflows, it also cancels the scheduled wake timer and unpins
// the workflow from the cache.
//
// The run's persisted status is updated to Cancelled and an EventRunCancelled
// event is emitted. Returns the persistence error from LoadRun if the run ID
// does not exist, or from SaveRun if the status update fails.
//
// Cancel is safe to call on runs in any status. Calling it on an already
// terminal run (Completed, Failed, Bailed, Cancelled) updates the status
// to Cancelled but has no other side effects.
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
	if run.Status == Sleeping && e.scheduler != nil {
		e.scheduler.Cancel(runID)
		e.workflowCacheMu.Lock()
		if e.pinnedWorkflows[run.WorkflowID] > 0 {
			e.pinnedWorkflows[run.WorkflowID]--
		}
		e.workflowCacheMu.Unlock()
	}

	run.Status = Cancelled
	run.UpdatedAt = time.Now()
	saveErr := e.persistence.SaveRun(ctx, run)
	if saveErr == nil {
		e.emitEvent(EngineEvent{
			Type:       EventRunCancelled,
			RunID:      runID,
			WorkflowID: run.WorkflowID,
			Status:     Cancelled,
			Timestamp:  time.Now(),
		})
	}
	return saveErr
}

// DeleteRun removes a run and all its associated state from persistence. If
// the run is currently active, DeleteRun cancels it and waits for all in-flight
// persistence writes to finish before deleting. If the provided context is
// cancelled while waiting, DeleteRun proceeds with deletion anyway to avoid
// leaving orphaned data.
//
// For sleeping runs, DeleteRun also cancels the scheduled wake timer and
// unpins the workflow from the cache. An EventRunDeleted event is emitted
// on successful deletion.
//
// Returns the persistence error if the delete operation fails. If the run
// does not exist in persistence, the behavior depends on the persistence
// implementation (the default backends return nil).
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
	if e.scheduler != nil {
		e.scheduler.Cancel(runID)
	}
	// Unpin workflow if this was a sleeping run
	run, loadErr := e.persistence.LoadRun(ctx, runID)
	if loadErr == nil && run.Status == Sleeping {
		e.workflowCacheMu.Lock()
		if e.pinnedWorkflows[run.WorkflowID] > 0 {
			e.pinnedWorkflows[run.WorkflowID]--
		}
		e.workflowCacheMu.Unlock()
	}
	delErr := e.persistence.DeleteRun(ctx, runID)
	if delErr == nil {
		e.emitEvent(EngineEvent{
			Type:      EventRunDeleted,
			RunID:     runID,
			Timestamp: time.Now(),
		})
	}
	return delErr
}

// runGCLoop periodically purges terminal runs older than the configured TTL.
func (e *Engine) runGCLoop() {
	ticker := time.NewTicker(e.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.closeCh:
			return
		case <-ticker.C:
			if _, err := e.PurgeRuns(context.Background(), e.gcTTL); err != nil {
				e.logger.Warn("run GC: %v", err)
			}
		}
	}
}

// PurgeRuns garbage-collects terminal runs (Completed, Failed, Bailed,
// Cancelled) whose UpdatedAt timestamp is older than the given TTL. A TTL of 0
// deletes all terminal runs regardless of age. Non-terminal runs (Running,
// Sleeping, Suspended) are never purged.
//
// Returns the number of runs successfully deleted. If some deletions fail,
// PurgeRuns logs warnings and continues with the remaining runs, returning the
// count of those that succeeded along with the first listing error encountered.
//
// Returns ErrEngineClosed if the engine has been shut down.
//
//	purged, err := engine.PurgeRuns(ctx, 24*time.Hour) // delete runs older than 1 day
func (e *Engine) PurgeRuns(ctx context.Context, ttl time.Duration) (int, error) {
	select {
	case <-e.closeCh:
		return 0, ErrEngineClosed
	default:
	}

	var before time.Time
	if ttl > 0 {
		before = time.Now().Add(-ttl)
	} else {
		before = time.Now().Add(time.Hour) // far future = match everything
	}

	terminalStatuses := []Status{Completed, Failed, Bailed, Cancelled}
	purged := 0

	for _, status := range terminalStatuses {
		runs, err := e.persistence.ListRuns(ctx, RunFilter{
			Status: status,
			Before: before,
		})
		if err != nil {
			return purged, fmt.Errorf("list %s runs for purge: %w", status, err)
		}
		for _, run := range runs {
			if err := e.persistence.DeleteRun(ctx, run.RunID); err != nil {
				e.logger.Warn("purge run %s: %v", run.RunID, err)
				continue
			}
			e.emitEvent(EngineEvent{
				Type:       EventRunDeleted,
				RunID:      run.RunID,
				WorkflowID: run.WorkflowID,
				Timestamp:  time.Now(),
			})
			purged++
		}
	}

	return purged, nil
}

// Resume resumes a suspended workflow run, blocking the calling goroutine until
// the workflow completes (like RunSync). The workflow structure is rebuilt from
// the persisted definition, not supplied by the caller, ensuring structural
// consistency between the original run and the resumed execution.
//
// The data map is injected into the workflow's state store under "__resume:"
// prefixed keys, making the values accessible to the resuming task via
// ctx.Get("__resume:key"). A "__resuming" flag is also set to true.
//
// Resume uses cache-first lookup: it checks the engine's in-memory workflow
// cache before falling back to the persisted WorkflowDef. If neither is
// available (e.g., the workflow was built with anonymous closures that cannot
// be serialized), Resume returns ErrWorkflowNotResumable.
//
// Returns ErrEngineClosed if the engine is shut down. Returns ErrRunNotFound
// if the run ID does not exist. Returns ErrRunNotSuspended if the run is not
// in Suspended status. Returns ErrRunAlreadyActive if another goroutine is
// already executing or resuming this run.
//
//	result, err := engine.Resume(ctx, runID, gostage.Params{"approved": true})
func (e *Engine) Resume(ctx context.Context, runID RunID, data Params) (*Result, error) {
	// Guard: reject if engine is closed
	select {
	case <-e.closeCh:
		return nil, ErrEngineClosed
	default:
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

	// Rebuild workflow: cache first (same-engine lifetime), then persisted definition
	wf := e.lookupCachedWorkflow(run.WorkflowID)
	if wf == nil && run.WorkflowDef != nil {
		if rebuilt, rebuildErr := NewWorkflowFromDefWithRegistry(run.WorkflowDef, e.registry); rebuildErr == nil {
			wf = rebuilt
			e.cacheWorkflow(rebuilt)
		} else {
			return nil, fmt.Errorf("rebuild workflow from definition: %w", rebuildErr)
		}
	}
	if wf == nil {
		return nil, ErrWorkflowNotResumable
	}

	wf = wf.clone()

	// Restore state from persistence
	wf.state = newRunState(runID, e.persistence)
	wf.state.setLimit(e.stateLimit)
	if err := wf.state.LoadFromPersistence(ctx); err != nil {
		return nil, fmt.Errorf("load state for resume: %w", err)
	}

	// Replay mutations first (regenerates original step IDs from counter=0),
	// then restore the persisted counter for future new mutations.
	if len(run.Mutations) > 0 {
		replayMutations(wf, run.Mutations, e.registry)
	}
	wf.dynCounter = run.DynCounter

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

// Close performs an ordered shutdown of the engine and releases all resources.
// The shutdown sequence is:
//
//  1. Signal all subsystems via the close channel (prevents new runs from starting).
//  2. Stop the timer scheduler (no more sleep-wake timer fires).
//  3. Cancel all active runs via their context cancel functions.
//  4. Drain the worker pool, waiting for in-flight jobs to complete. If
//     WithShutdownTimeout is configured and workers do not finish in time,
//     Close proceeds after a bounded grace period and logs a warning.
//  5. Close the spawn runner (flushes child process state).
//  6. Close the persistence layer (last, so in-flight runs can flush writes).
//
// Close is safe to call multiple times; the second and subsequent calls are
// no-ops and return nil. The returned error, if any, comes from closing the
// persistence layer.
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

		// 4. Close spawn runner (before persistence so child state flushes complete)
		if e.spawnRunner != nil {
			if err := e.spawnRunner.Close(); err != nil {
				e.logger.Warn("spawn runner close: %v", err)
			}
		}

		// 5. Close persistence (last — active runs may flush during shutdown)
		closeErr = e.persistence.Close()
	})
	return closeErr
}

// wakeWorkflow is called by the timer scheduler when a sleeping workflow's timer fires.
func (e *Engine) wakeWorkflow(runID RunID) {
	// Don't wake if engine is shutting down
	select {
	case <-e.closeCh:
		return
	default:
	}

	if e.pool == nil {
		e.logger.Warn("failed to wake workflow %s: no worker pool", runID)
		return
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

		// Replay mutations first (regenerates original step IDs from counter=0),
		// then restore the persisted counter for future new mutations.
		if len(run.Mutations) > 0 {
			replayMutations(wf, run.Mutations, e.registry)
		}
		wf.dynCounter = run.DynCounter

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
		}

		// Update status to Running
		run.Status = Running
		run.UpdatedAt = time.Now()
		if err := e.persistence.SaveRun(ctx, run); err != nil {
			// Cannot proceed without marking the run as Running — abort.
			e.logger.Error("persist wake status update: %v", err)
			return
		}

		e.emitEvent(EngineEvent{
			Type:       EventRunWoken,
			RunID:      runID,
			WorkflowID: run.WorkflowID,
			Status:     Running,
			Timestamp:  time.Now(),
		})

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

// Recover scans persistence for interrupted workflows and handles crash
// recovery. It is designed to be called after a process restart to restore
// engine state. The recovery logic handles each status as follows:
//
//   - Running: Marked as Failed, since a running workflow found in persistence
//     after restart indicates a crash mid-execution.
//   - Sleeping (wake time in the past): Resumed immediately by calling the
//     internal wake handler. The workflow's persisted definition is rebuilt
//     and cached so the wake handler can find it.
//   - Sleeping (wake time in the future): Registered with the timer scheduler
//     for automatic wake at the originally scheduled time.
//   - Suspended: Left untouched, as these runs are waiting for explicit
//     external input via Resume.
//
// Recover is called automatically during New when WithAutoRecover is set.
// It can also be called manually at any time.
//
// Returns an error if listing runs from persistence fails. Individual run
// recovery failures are logged but do not abort the overall recovery.
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

	e.emitEvent(EngineEvent{
		Type:       EventRunCreated,
		RunID:      runID,
		WorkflowID: wf.ID,
		Status:     Running,
		Timestamp:  now,
	})

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

	// Emit lifecycle event for the terminal status.
	// Cancelled is emitted by Cancel() — skip here to avoid duplicates.
	var eventType EventType
	switch result.Status {
	case Completed:
		eventType = EventRunCompleted
	case Failed:
		eventType = EventRunFailed
	case Bailed:
		eventType = EventRunBailed
	case Suspended:
		eventType = EventRunSuspended
	case Sleeping:
		eventType = EventRunSleeping
	}
	if eventType > 0 {
		e.emitEvent(EngineEvent{
			Type:       eventType,
			RunID:      runID,
			WorkflowID: wf.ID,
			Status:     result.Status,
			Timestamp:  time.Now(),
		})
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
		// Reject suspension of anonymous-closure workflows — they cannot be resumed
		// because the definition is not serializable. Fail early so the developer
		// sees the problem during development, not when Resume is called in production.
		if (result.Status == Suspended || result.Status == Sleeping) && run.WorkflowDef == nil {
			result.Status = Failed
			result.Error = ErrWorkflowNotResumable
			run.Status = Failed
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

// ListRuns queries the persistence layer for runs matching the given filter
// criteria. Filters can narrow results by workflow ID, status, update time,
// and paginate with offset and limit. An empty filter returns all runs.
//
// Returns ErrEngineClosed if the engine has been shut down. Returns nil (not
// an empty slice) when no runs match the filter.
//
//	runs, err := engine.ListRuns(ctx, gostage.RunFilter{Status: gostage.Completed, Limit: 10})
func (e *Engine) ListRuns(ctx context.Context, filter RunFilter) ([]*RunState, error) {
	select {
	case <-e.closeCh:
		return nil, ErrEngineClosed
	default:
	}
	return e.persistence.ListRuns(ctx, filter)
}

// GetRun returns the current persisted state of a run without blocking.
// Unlike Wait, GetRun returns immediately regardless of whether the run is
// still in progress, making it suitable for polling or status checks.
//
// Returns ErrEngineClosed if the engine has been shut down. Returns a
// *RunNotFoundError (matching ErrRunNotFound via errors.Is) if the run
// does not exist in persistence.
//
//	run, err := engine.GetRun(ctx, runID)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Println(run.Status)
func (e *Engine) GetRun(ctx context.Context, runID RunID) (*RunState, error) {
	select {
	case <-e.closeCh:
		return nil, ErrEngineClosed
	default:
	}
	return e.persistence.LoadRun(ctx, runID)
}
