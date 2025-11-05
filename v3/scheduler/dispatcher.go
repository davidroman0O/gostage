package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/runner"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

type Dispatcher struct {
	ctx    context.Context
	cancel context.CancelFunc

	queue   Queue
	store   SummaryStore
	manager ExecutionManager

	runner WorkflowRunner

	bindings []*binding

	telemetry        TelemetryDispatcher
	diagnostics      DiagnosticsWriter
	health           HealthPublisher
	healthDispatcher *node.HealthDispatcher

	logger telemetry.Logger

	claimInterval    time.Duration
	jitter           time.Duration
	maxInFlight      int
	failurePolicy    bootstrap.FailurePolicy
	healthStates     map[string]node.HealthStatus
	healthDetails    map[string]string
	healthTimes      map[string]time.Time
	healthErrorTimes map[string]time.Time
	healthErrorInfo  map[string]string
	healthMu         sync.RWMutex

	cancelMu       sync.RWMutex
	cancels        map[state.WorkflowID]context.CancelFunc
	pendingCancels map[state.WorkflowID]struct{}

	inflight  atomic.Int32
	completed atomic.Int64
	failed    atomic.Int64
	cancelled atomic.Int64
	skipped   atomic.Int64
	wg        sync.WaitGroup
	clock     func() time.Time
}

const defaultClaimInterval = 50 * time.Millisecond

type binding struct {
	pool   *pools.Local
	remote RemoteExecutor
}

// Start launches the dispatcher processing loop.
func (d *Dispatcher) Start() {
	d.start()
}

// Stop gracefully stops the dispatcher and waits for inflight work.
func (d *Dispatcher) Stop() {
	d.stop()
}

// PollOnce executes a single scheduling cycle.
func (d *Dispatcher) PollOnce() {
	d.pollOnce()
}

// RegisterCancel associates a cancellation callback with a workflow ID.
func (d *Dispatcher) RegisterCancel(id state.WorkflowID, cancel context.CancelFunc) {
	d.registerCancel(id, cancel)
}

// UnregisterCancel removes the cancellation callback for a workflow ID.
func (d *Dispatcher) UnregisterCancel(id state.WorkflowID) {
	d.unregisterCancel(id)
}

// CancelWorkflow requests cancellation for the given workflow ID.
func (d *Dispatcher) CancelWorkflow(id state.WorkflowID) {
	d.cancelWorkflow(id)
}

// SuppressWorkflowTelemetry mutes specified telemetry events for a workflow.
func (d *Dispatcher) SuppressWorkflowTelemetry(id state.WorkflowID, kinds ...telemetry.EventKind) {
	d.suppressWorkflowTelemetry(id, kinds...)
}

// StatsCounters returns completion/failure/cancellation/skip counters.
func (d *Dispatcher) StatsCounters() (completed, failed, cancelled, skipped int64) {
	c, f, cn, sk := d.statsCounters()
	return int64(c), int64(f), int64(cn), int64(sk)
}

// HealthInfo returns health metadata for the specified pool.
func (d *Dispatcher) HealthInfo(pool string) (node.HealthStatus, string, time.Time, string, time.Time) {
	return d.healthInfo(pool)
}

// PublishHealth publishes a health event for the given pool.
func (d *Dispatcher) PublishHealth(pool string, status node.HealthStatus, detail string) {
	d.publishHealth(pool, status, detail)
}

// Inflight returns the current number of inflight workflows.
func (d *Dispatcher) Inflight() int32 {
	if d == nil {
		return 0
	}
	return d.inflight.Load()
}

// ExecuteForTest executes a claimed workflow synchronously (test helper).
func (d *Dispatcher) ExecuteForTest(pool *pools.Local, release func(), claimed *state.ClaimedWorkflow) {
	if d == nil {
		return
	}
	d.inflight.Add(1)
	d.wg.Add(1)
	d.execute(pool, release, claimed)
}

// HealthDispatcher exposes the underlying health dispatcher when available.
func (d *Dispatcher) HealthDispatcher() *node.HealthDispatcher {
	return d.healthDispatcher
}

// ReportError records a diagnostics event for dispatcher components.
func (d *Dispatcher) ReportError(component string, err error) {
	d.reportError(component, err)
}

// Manager exposes the state manager backing the dispatcher.
func (d *Dispatcher) Manager() ExecutionManager {
	return d.manager
}

// TelemetryDispatcher exposes the telemetry dispatcher backing the scheduler.
func (d *Dispatcher) TelemetryDispatcher() TelemetryDispatcher {
	return d.telemetry
}

// BeginRemoteDispatch prepares dispatcher bookkeeping for a remote workflow.
func (d *Dispatcher) BeginRemoteDispatch(id state.WorkflowID, cancel context.CancelFunc) {
	d.inflight.Add(1)
	d.wg.Add(1)
	d.registerCancel(id, cancel)
	d.clearTelemetry(id)
	d.suppressWorkflowTelemetry(id,
		telemetry.EventWorkflowStarted,
		telemetry.EventWorkflowCompleted,
		telemetry.EventWorkflowFailed,
		telemetry.EventWorkflowCancelled,
		telemetry.EventWorkflowSummary,
	)
}

// AbortRemoteDispatch cleans up dispatcher bookkeeping when a remote workflow is aborted before completion.
func (d *Dispatcher) AbortRemoteDispatch(id state.WorkflowID) {
	d.unregisterCancel(id)
	d.inflight.Add(-1)
	d.wg.Done()
	d.clearTelemetry(id)
}

func New(ctx context.Context, queue Queue, store SummaryStore, manager ExecutionManager, runner WorkflowRunner, telemetryDisp TelemetryDispatcher, diag DiagnosticsWriter, health HealthPublisher, logger telemetry.Logger, bindings []*Binding, opts Options) *Dispatcher {
	if ctx == nil {
		ctx = context.Background()
	}
	dctx, cancel := context.WithCancel(ctx)
	internalBindings := make([]*binding, 0, len(bindings))
	for _, b := range bindings {
		if b == nil || b.Pool == nil {
			continue
		}
		internalBindings = append(internalBindings, &binding{
			pool:   b.Pool,
			remote: b.Remote,
		})
	}
	d := &Dispatcher{
		ctx:           dctx,
		cancel:        cancel,
		queue:         queue,
		store:         store,
		runner:        runner,
		manager:       manager,
		bindings:      internalBindings,
		telemetry:     telemetryDisp,
		diagnostics:   diag,
		health:        health,
		logger:        logger,
		claimInterval: opts.ClaimInterval,
		jitter:        opts.Jitter,
		maxInFlight:   opts.MaxInFlight,
		failurePolicy: opts.FailurePolicy,
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	d.clock = opts.Clock
	if d.claimInterval <= 0 {
		d.claimInterval = defaultClaimInterval
	}
	if health != nil {
		d.healthStates = make(map[string]node.HealthStatus)
		d.healthDetails = make(map[string]string)
		d.healthTimes = make(map[string]time.Time)
		d.healthErrorTimes = make(map[string]time.Time)
		d.healthErrorInfo = make(map[string]string)
	}
	if hd, ok := health.(*node.HealthDispatcher); ok {
		d.healthDispatcher = hd
	}
	d.cancels = make(map[state.WorkflowID]context.CancelFunc)
	d.pendingCancels = make(map[state.WorkflowID]struct{})
	return d
}

func (d *Dispatcher) emitWorkflowEvent(kind telemetry.EventKind, id state.WorkflowID, attempt int, metadata map[string]any, err error) {
	if d == nil || d.telemetry == nil {
		return
	}
	evt := telemetry.Event{
		Kind:       kind,
		WorkflowID: string(id),
		Attempt:    attempt,
		Timestamp:  d.now(),
		Metadata:   copyMap(metadata),
	}
	if err != nil {
		evt.Error = err.Error()
	}
	if derr := d.telemetry.Dispatch(evt); derr != nil {
		d.reportError("telemetry.dispatch", derr)
	}
}

func (d *Dispatcher) start() {
	d.wg.Add(1)
	go d.loop()
	if d.health != nil {
		for _, binding := range d.bindings {
			d.publishHealth(binding.pool.Name(), node.HealthHealthy, "ready")
		}
	}
}

func (d *Dispatcher) loop() {
	defer d.wg.Done()
	for {
		if d.ctx.Err() != nil {
			return
		}
		d.pollOnce()
		sleep := d.claimInterval
		if d.jitter > 0 {
			delta := rand.Int63n(int64(d.jitter))
			if rand.Intn(2) == 0 {
				sleep += time.Duration(delta)
			} else {
				sleep -= time.Duration(delta)
				if sleep < 10*time.Millisecond {
					sleep = 10 * time.Millisecond
				}
			}
		}
		select {
		case <-time.After(sleep):
		case <-d.ctx.Done():
			return
		}
	}
}

func (d *Dispatcher) pollOnce() {
	if max := d.maxInFlight; max > 0 && int(d.inflight.Load()) >= max {
		return
	}
	for _, binding := range d.bindings {
		if d.maxInFlight > 0 && int(d.inflight.Load()) >= d.maxInFlight {
			return
		}
		pool := binding.pool
		release, ok := pool.TryAcquire(d.ctx)
		if !ok {
			continue
		}
		claimed, err := d.queue.Claim(d.ctx, pool.Selector(), pool.Name())
		if err != nil {
			release()
			if !errors.Is(err, state.ErrNoPending) {
				d.reportError("dispatcher.claim", fmt.Errorf("pool %s claim: %w", pool.Name(), err))
				d.publishHealth(pool.Name(), node.HealthUnavailable, err.Error())
			}
			continue
		}
		if claimed == nil {
			release()
			continue
		}
		d.emitWorkflowEvent(telemetry.EventWorkflowClaimed, claimed.ID, claimed.Attempt, map[string]any{
			"pool": pool.Name(),
		}, nil)
		if binding.remote != nil && binding.remote.Ready() {
			if err := binding.remote.Dispatch(claimed, release); err != nil {
				release()
				d.reportError("dispatcher.remote", fmt.Errorf("dispatch workflow %s: %w", claimed.ID, err))
				d.publishHealth(pool.Name(), node.HealthUnavailable, err.Error())
				if relErr := d.queue.Release(d.ctx, claimed.ID); relErr != nil {
					d.reportError("dispatcher.remote.release", fmt.Errorf("release workflow %s: %w", claimed.ID, relErr))
				}
			}
			continue
		}
		d.inflight.Add(1)
		d.wg.Add(1)
		go d.execute(pool, release, claimed)
	}
}

func (d *Dispatcher) execute(pool *pools.Local, release func(), claimed *state.ClaimedWorkflow) {
	defer d.wg.Done()
	defer d.inflight.Add(-1)
	defer release()

	d.clearTelemetry(claimed.ID)

	execCtx, cancel := context.WithCancel(d.ctx)
	defer cancel()
	d.registerCancel(claimed.ID, cancel)
	defer d.unregisterCancel(claimed.ID)

	d.emitWorkflowEvent(telemetry.EventWorkflowStarted, claimed.ID, claimed.Attempt, map[string]any{
		"pool": pool.Name(),
	}, nil)

	initialStore := ExtractInitialStore(claimed.Metadata)
	result, err := d.runWorkflow(execCtx, pool, claimed, initialStore)
	if err != nil {
		d.reportError("dispatcher.run", err)
		d.publishHealth(pool.Name(), node.HealthUnavailable, err.Error())
	} else if d.diagnostics != nil {
		d.diagnostics.Write(diagnostics.Event{
			OccurredAt: d.now(),
			Component:  "dispatcher.run",
			Severity:   diagnostics.SeverityInfo,
			Metadata: map[string]any{
				"workflow": claimed.ID,
			},
		})
	}

	if err == nil {
		if result.result.Success {
			d.publishHealth(pool.Name(), node.HealthHealthy, "workflow completed")
		} else {
			d.publishHealth(pool.Name(), node.HealthDegraded, errorMessage(result.result.Error))
		}
	}

	outcome := d.decideFailure(claimed, result)
	finalState := result.state
	if outcome.FinalState != "" {
		finalState = outcome.FinalState
	}
	result.state = finalState
	result.report.Status = executionStatusToWorkflow(result.state)
	result.reason = d.normalizeReason(claimed.ID, outcome.Reason, finalState, result.result.Success, claimed.Attempt, pool.Name())

	switch outcome.Action {
	case bootstrap.FailureActionRetry:
		preserveInitialStoreForRetry(claimed, result.result.FinalStore)
		d.emitWorkflowEvent(telemetry.EventWorkflowRetry, claimed.ID, claimed.Attempt, map[string]any{
			"pool":   pool.Name(),
			"error":  errorMessage(result.result.Error),
			"reason": result.reason,
		}, result.result.Error)
		if err := d.queue.Release(d.ctx, claimed.ID); err != nil {
			d.reportError("dispatcher.release", fmt.Errorf("release workflow %s: %w", claimed.ID, err))
			d.publishHealth(pool.Name(), node.HealthUnavailable, err.Error())
		}
		d.clearTelemetry(claimed.ID)
		return
	}

	if result.state == runner.StatusCancelled {
		metadata := map[string]any{
			"pool": pool.Name(),
		}
		if msg := errorMessage(result.result.Error); msg != "" {
			metadata["error"] = msg
		}
		switch result.reason {
		case state.TerminationReasonPolicyCancel:
			metadata["reason"] = "failure_policy"
		case state.TerminationReasonTimeout:
			metadata["reason"] = "timeout"
		default:
			metadata["reason"] = "explicit_request"
		}
		d.emitWorkflowEvent(telemetry.EventWorkflowCancelled, claimed.ID, claimed.Attempt, metadata, result.result.Error)
	}

	result.report.Reason = result.reason
	summary := result.toSummary()
	if d.manager != nil {
		if result.report.Attempt == 0 {
			result.report.Attempt = summary.Attempt
		}
		if err := d.manager.StoreExecutionSummary(d.ctx, string(claimed.ID), result.report); err != nil {
			d.reportError("dispatcher.summary", fmt.Errorf("store execution summary %s: %w", claimed.ID, err))
		}
	} else if d.store != nil {
		if err := d.store.StoreSummary(d.ctx, claimed.ID, summary); err != nil {
			d.reportError("dispatcher.store", fmt.Errorf("store summary %s: %w", claimed.ID, err))
		}
	}
	if err := d.queue.Ack(d.ctx, claimed.ID, summary); err != nil {
		d.reportError("dispatcher.ack", fmt.Errorf("ack workflow %s: %w", claimed.ID, err))
		d.publishHealth(pool.Name(), node.HealthUnavailable, err.Error())
	} else {
		d.recordCompletion(result.state)
	}

	attempt := summary.Attempt
	if attempt == 0 {
		attempt = claimed.Attempt
	}
	poolName := ""
	if pool != nil {
		poolName = pool.Name()
	}
	d.verifyTelemetryCoverage(claimed.ID, attempt, result.state, poolName)
}

func (d *Dispatcher) runWorkflow(ctx context.Context, pool *pools.Local, claimed *state.ClaimedWorkflow, initialStore map[string]any) (runResult, error) {
	def := claimed.Definition.Clone()
	def.ID = string(claimed.ID)
	wf, err := workflow.Materialize(def, registry.Default())
	if err != nil {
		return runResult{}, fmt.Errorf("materialize workflow %s: %w", def.ID, err)
	}

	opts := runner.RunOptions{
		Context:      ctx,
		Logger:       d.logger,
		InitialStore: initialStore,
		Attempt:      claimed.Attempt,
	}
	start := d.now()
	res := d.runner.Run(wf, opts)
	finalState := runner.StatusCompleted
	if !res.Success {
		if errors.Is(res.Error, context.Canceled) {
			finalState = runner.StatusCancelled
		} else {
			finalState = runner.StatusFailed
		}
	}
	report := res.ToExecutionReport(wf, finalState, start)
	report.Attempt = claimed.Attempt

	d.emitWorkflowEvent(telemetry.EventWorkflowExecution, claimed.ID, claimed.Attempt, map[string]any{
		"success": res.Success,
		"pool":    pool.Name(),
	}, res.Error)

	return runResult{
		claimed:    claimed,
		result:     res,
		report:     report,
		state:      finalState,
		clock:      d.clock,
		dispatcher: d,
		pool:       pool.Name(),
	}, nil
}

func (d *Dispatcher) suppressWorkflowTelemetry(id state.WorkflowID, kinds ...telemetry.EventKind) {
	if d == nil || len(kinds) == 0 {
		return
	}
	if suppressor, ok := d.manager.(interface {
		SuppressWorkflowEvents(string, ...telemetry.EventKind)
	}); ok {
		suppressor.SuppressWorkflowEvents(string(id), kinds...)
	}
}

func (d *Dispatcher) stop() {
	d.cancel()
	d.wg.Wait()
}

func (d *Dispatcher) registerCancel(id state.WorkflowID, cancel context.CancelFunc) {
	if cancel == nil {
		return
	}
	d.cancelMu.Lock()
	d.cancels[id] = cancel
	if _, pending := d.pendingCancels[id]; pending {
		delete(d.pendingCancels, id)
		cancel()
	}
	d.cancelMu.Unlock()
}

func (d *Dispatcher) unregisterCancel(id state.WorkflowID) {
	d.cancelMu.Lock()
	delete(d.cancels, id)
	d.cancelMu.Unlock()
}

func (d *Dispatcher) cancelWorkflow(id state.WorkflowID) {
	d.cancelMu.RLock()
	cancel := d.cancels[id]
	d.cancelMu.RUnlock()
	if cancel != nil {
		d.emitWorkflowEvent(telemetry.EventWorkflowCancelRequest, id, 0, map[string]any{"inflight": true}, nil)
		cancel()
		return
	}
	d.cancelMu.Lock()
	d.pendingCancels[id] = struct{}{}
	d.cancelMu.Unlock()
	d.emitWorkflowEvent(telemetry.EventWorkflowCancelRequest, id, 0, map[string]any{"pending": true}, nil)
}

func (d *Dispatcher) publishHealth(pool string, status node.HealthStatus, detail string) {
	if d.health == nil {
		return
	}
	now := d.now()
	d.healthMu.Lock()
	if d.healthStates == nil {
		d.healthStates = make(map[string]node.HealthStatus)
	}
	if d.healthDetails == nil {
		d.healthDetails = make(map[string]string)
	}
	if d.healthTimes == nil {
		d.healthTimes = make(map[string]time.Time)
	}
	if d.healthErrorTimes == nil {
		d.healthErrorTimes = make(map[string]time.Time)
	}
	if d.healthErrorInfo == nil {
		d.healthErrorInfo = make(map[string]string)
	}
	previousStatus := d.healthStates[pool]
	previousDetail := d.healthDetails[pool]
	if previousStatus == status && previousDetail == detail {
		d.healthMu.Unlock()
		return
	}
	d.healthStates[pool] = status
	d.healthDetails[pool] = detail
	d.healthTimes[pool] = now
	if status != node.HealthHealthy {
		d.healthErrorTimes[pool] = now
		d.healthErrorInfo[pool] = detail
	}
	d.healthMu.Unlock()

	d.health.Publish(node.HealthEvent{
		Timestamp: now,
		Pool:      pool,
		Status:    status,
		Detail:    detail,
	})
}

func (d *Dispatcher) healthInfo(pool string) (node.HealthStatus, string, time.Time, string, time.Time) {
	if d.health == nil {
		return node.HealthHealthy, "", time.Time{}, "", time.Time{}
	}
	d.healthMu.RLock()
	defer d.healthMu.RUnlock()
	status := d.healthStates[pool]
	detail := d.healthDetails[pool]
	if status == "" {
		status = node.HealthHealthy
	}
	lastChange := d.healthTimes[pool]
	lastErrDetail := d.healthErrorInfo[pool]
	lastErr := d.healthErrorTimes[pool]
	return status, detail, lastChange, lastErrDetail, lastErr
}

func (d *Dispatcher) recordCompletion(state runner.ExecutionStatus) {
	switch state {
	case runner.StatusCompleted:
		d.completed.Add(1)
	case runner.StatusCancelled:
		d.cancelled.Add(1)
	case runner.StatusSkipped:
		d.skipped.Add(1)
	default:
		d.failed.Add(1)
	}
}

func (d *Dispatcher) statsCounters() (completed, failed, cancelled, skipped int) {
	if d == nil {
		return 0, 0, 0, 0
	}
	return int(d.completed.Load()), int(d.failed.Load()), int(d.cancelled.Load()), int(d.skipped.Load())
}

func (d *Dispatcher) decideFailure(claimed *state.ClaimedWorkflow, result runResult) bootstrap.FailureOutcome {
	if result.result.Success {
		return bootstrap.FailureOutcome{
			Action:     bootstrap.FailureActionAck,
			FinalState: runner.StatusCompleted,
			Reason:     state.TerminationReasonSuccess,
		}
	}
	if result.state == runner.StatusCancelled {
		return bootstrap.FailureOutcome{
			Action:     bootstrap.FailureActionAck,
			FinalState: runner.StatusCancelled,
			Reason:     state.TerminationReasonUserCancel,
		}
	}
	if d.failurePolicy == nil {
		return bootstrap.FailureOutcome{
			Action:     bootstrap.FailureActionAck,
			FinalState: result.state,
			Reason:     state.TerminationReasonFailure,
		}
	}
	ctx := bootstrap.FailureContext{
		WorkflowID: claimed.ID,
		Attempt:    claimed.Attempt,
		Err:        result.result.Error,
	}
	outcome := d.failurePolicy.Decide(d.ctx, ctx)
	if outcome.Action == bootstrap.FailureActionRetry {
		if outcome.Reason == "" {
			outcome.Reason = state.TerminationReasonFailure
		}
		return outcome
	}
	if outcome.FinalState == "" {
		outcome.FinalState = result.state
	}
	if outcome.Reason == "" {
		if d != nil {
			d.reportErrorWithMetadata("dispatcher.reason.missing", fmt.Errorf("workflow %s missing termination reason", claimed.ID), map[string]any{
				"workflow_id": string(claimed.ID),
				"attempt":     claimed.Attempt,
			})
		}
		switch outcome.FinalState {
		case runner.StatusCancelled:
			outcome.Reason = state.TerminationReasonPolicyCancel
		case runner.StatusCompleted:
			outcome.Reason = state.TerminationReasonSuccess
		default:
			outcome.Reason = state.TerminationReasonFailure
		}
	}
	if outcome.Action == bootstrap.FailureActionFinalize {
		if outcome.FinalState == "" {
			outcome.FinalState = runner.StatusCancelled
		}
		return outcome
	}
	// Default: acknowledge.
	if outcome.Action == bootstrap.FailureActionAck {
		if outcome.Reason == "" {
			outcome.Reason = state.TerminationReasonFailure
		}
		return outcome
	}
	return bootstrap.FailureOutcome{
		Action:     bootstrap.FailureActionAck,
		FinalState: result.state,
		Reason:     state.TerminationReasonFailure,
	}
}

func (d *Dispatcher) reportError(component string, err error) {
	d.reportErrorWithMetadata(component, err, nil)
}

func (d *Dispatcher) reportErrorWithMetadata(component string, err error, metadata map[string]any) {
	if err == nil && len(metadata) == 0 {
		return
	}
	if d.diagnostics != nil {
		d.diagnostics.Write(diagnostics.Event{
			OccurredAt: d.now(),
			Component:  component,
			Severity:   diagnostics.SeverityError,
			Err:        err,
			Metadata:   copyMap(metadata),
		})
	}
	if err != nil && d.logger != nil {
		d.logger.Error(component, "error", err)
	}
}

func (d *Dispatcher) now() time.Time {
	if d != nil && d.clock != nil {
		return d.clock()
	}
	return time.Now()
}

func (d *Dispatcher) clearTelemetry(id state.WorkflowID) {
	if d == nil || d.telemetry == nil || id == "" {
		return
	}
	d.telemetry.ClearCoverage(string(id))
}

func (d *Dispatcher) verifyTelemetryCoverage(id state.WorkflowID, attempt int, finalState runner.ExecutionStatus, pool string) {
	if d == nil || d.telemetry == nil || id == "" {
		return
	}
	coverage := d.telemetry.Coverage(string(id))
	required := requiredWorkflowEvents(finalState)
	required = append(required, telemetry.EventWorkflowSummary)
	missing := missingTelemetry(required, coverage)
	if len(missing) == 0 {
		d.telemetry.ClearCoverage(string(id))
		return
	}
	missingStrings := eventKindsToStrings(missing)
	meta := map[string]any{
		"workflow_id": string(id),
		"missing":     missingStrings,
	}
	if attempt > 0 {
		meta["attempt"] = attempt
	}
	if pool != "" {
		meta["pool"] = pool
	}
	d.reportErrorWithMetadata("telemetry.missing", fmt.Errorf("workflow %s missing telemetry events: %s", id, strings.Join(missingStrings, ",")), meta)
	d.telemetry.ClearCoverage(string(id))
}

func requiredWorkflowEvents(state runner.ExecutionStatus) []telemetry.EventKind {
	switch state {
	case runner.StatusCompleted:
		return []telemetry.EventKind{telemetry.EventWorkflowCompleted}
	case runner.StatusCancelled:
		return []telemetry.EventKind{telemetry.EventWorkflowCancelled}
	case runner.StatusFailed:
		return []telemetry.EventKind{telemetry.EventWorkflowFailed}
	default:
		return nil
	}
}

func missingTelemetry(required []telemetry.EventKind, coverage map[telemetry.EventKind]int) []telemetry.EventKind {
	if len(required) == 0 {
		return nil
	}
	missing := make([]telemetry.EventKind, 0, len(required))
	for _, kind := range required {
		if kind == "" {
			continue
		}
		if coverage == nil || coverage[kind] == 0 {
			missing = append(missing, kind)
		}
	}
	return missing
}

func eventKindsToStrings(kinds []telemetry.EventKind) []string {
	if len(kinds) == 0 {
		return nil
	}
	out := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		if kind != "" {
			out = append(out, string(kind))
		}
	}
	return out
}

func (d *Dispatcher) CompleteRemote(claimed *state.ClaimedWorkflow, pool *pools.Local, summary state.ResultSummary, release func()) {
	if claimed == nil {
		if release != nil {
			release()
		}
		d.inflight.Add(-1)
		d.wg.Done()
		return
	}

	poolName := ""
	if pool != nil {
		poolName = pool.Name()
	}

	defer d.wg.Done()
	defer d.inflight.Add(-1)
	if release != nil {
		defer release()
	}

	attempt := summary.Attempt
	if attempt == 0 && claimed.Attempt > 0 {
		attempt = claimed.Attempt
	}

	var runErr error
	if summary.Error != "" {
		runErr = errors.New(summary.Error)
	}

	run := runResult{
		dispatcher: d,
		claimed:    claimed,
		result: runner.RunResult{
			Success:         summary.Success,
			Error:           runErr,
			FinalStore:      copyMap(summary.Output),
			DisabledStages:  copyBoolMap(summary.DisabledStages),
			DisabledActions: copyBoolMap(summary.DisabledActions),
			RemovedStages:   copyStringMap(summary.RemovedStages),
			RemovedActions:  copyStringMap(summary.RemovedActions),
			Attempt:         attempt,
			Duration:        summary.Duration,
		},
		report: state.ExecutionReport{
			WorkflowID:      string(claimed.ID),
			WorkflowName:    claimed.Definition.Name,
			WorkflowType:    claimed.Definition.Type,
			WorkflowTags:    append([]string(nil), claimed.Definition.Tags...),
			Description:     claimed.Definition.Description,
			Status:          state.WorkflowPending,
			Success:         summary.Success,
			ErrorMessage:    summary.Error,
			Reason:          summary.Reason,
			StartedAt:       claimed.ClaimedAt,
			Duration:        summary.Duration,
			CompletedAt:     summary.CompletedAt,
			FinalStore:      copyMap(summary.Output),
			DisabledStages:  copyBoolMap(summary.DisabledStages),
			DisabledActions: copyBoolMap(summary.DisabledActions),
			RemovedStages:   copyStringMap(summary.RemovedStages),
			RemovedActions:  copyStringMap(summary.RemovedActions),
			Attempt:         attempt,
		},
		state:  runner.StatusCompleted,
		reason: summary.Reason,
		clock:  d.clock,
		pool:   poolName,
	}

	if run.report.StartedAt.IsZero() {
		run.report.StartedAt = claimed.ClaimedAt
	}
	if run.report.CompletedAt.IsZero() {
		run.report.CompletedAt = d.now()
	}

	if !summary.Success {
		switch summary.Reason {
		case state.TerminationReasonUserCancel, state.TerminationReasonPolicyCancel, state.TerminationReasonTimeout:
			run.state = runner.StatusCancelled
		default:
			run.state = runner.StatusFailed
		}
	}

	if poolName != "" {
		if summary.Success {
			d.publishHealth(poolName, node.HealthHealthy, "workflow completed")
		} else {
			d.publishHealth(poolName, node.HealthDegraded, errorMessage(runErr))
		}
	}

	d.applyRemoteStatuses(claimed, summary)

	outcome := d.decideFailure(claimed, run)
	finalState := run.state
	if outcome.FinalState != "" {
		finalState = outcome.FinalState
	}
	run.state = finalState
	run.report.Status = executionStatusToWorkflow(run.state)
	run.reason = d.normalizeReason(claimed.ID, outcome.Reason, run.state, run.result.Success, attempt, poolName)
	run.report.Reason = run.reason

	switch outcome.Action {
	case bootstrap.FailureActionRetry:
		preserveInitialStoreForRetry(claimed, run.result.FinalStore)
		metadata := map[string]any{
			"pool":   poolName,
			"reason": run.reason,
		}
		if msg := errorMessage(runErr); msg != "" {
			metadata["error"] = msg
		}
		d.emitWorkflowEvent(telemetry.EventWorkflowRetry, claimed.ID, attempt, metadata, runErr)
		if err := d.queue.Release(d.ctx, claimed.ID); err != nil {
			d.reportError("dispatcher.remote.release", fmt.Errorf("release workflow %s: %w", claimed.ID, err))
			if poolName != "" {
				d.publishHealth(poolName, node.HealthUnavailable, err.Error())
			}
		}
		d.clearTelemetry(claimed.ID)
		return
	}

	if run.state == runner.StatusCancelled {
		metadata := map[string]any{
			"pool": poolName,
		}
		if msg := errorMessage(runErr); msg != "" {
			metadata["error"] = msg
		}
		switch run.reason {
		case state.TerminationReasonPolicyCancel:
			metadata["reason"] = "failure_policy"
		case state.TerminationReasonTimeout:
			metadata["reason"] = "timeout"
		default:
			metadata["reason"] = "explicit_request"
		}
		d.emitWorkflowEvent(telemetry.EventWorkflowCancelled, claimed.ID, attempt, metadata, runErr)
	}

	finalSummary := run.toSummary()
	if finalSummary.Attempt == 0 {
		finalSummary.Attempt = attempt
	}

	if d.manager != nil {
		if run.report.Attempt == 0 {
			run.report.Attempt = attempt
		}
		if err := d.manager.StoreExecutionSummary(d.ctx, string(claimed.ID), run.report); err != nil {
			d.reportError("dispatcher.remote.summary", fmt.Errorf("store execution summary %s: %w", claimed.ID, err))
		}
	} else if d.store != nil {
		if err := d.store.StoreSummary(d.ctx, claimed.ID, finalSummary); err != nil {
			d.reportError("dispatcher.remote.store", fmt.Errorf("store summary %s: %w", claimed.ID, err))
		}
	}

	if d.manager != nil {
		if err := d.manager.WorkflowStatus(d.ctx, string(claimed.ID), executionStatusToWorkflow(run.state)); err != nil {
			d.reportError("dispatcher.remote.workflow_status", fmt.Errorf("update workflow %s status: %w", claimed.ID, err))
		}
	}

	if err := d.queue.Ack(d.ctx, claimed.ID, finalSummary); err != nil {
		d.reportError("dispatcher.remote.ack", fmt.Errorf("ack workflow %s: %w", claimed.ID, err))
		if poolName != "" {
			d.publishHealth(poolName, node.HealthUnavailable, err.Error())
		}
	} else {
		d.recordCompletion(run.state)
	}

	telemetryAttempt := finalSummary.Attempt
	if telemetryAttempt == 0 {
		telemetryAttempt = attempt
	}
	d.verifyTelemetryCoverage(claimed.ID, telemetryAttempt, run.state, poolName)
}

type runResult struct {
	claimed    *state.ClaimedWorkflow
	result     runner.RunResult
	report     state.ExecutionReport
	state      runner.ExecutionStatus
	reason     state.TerminationReason
	clock      func() time.Time
	dispatcher *Dispatcher
	pool       string
}

func (r runResult) toSummary() state.ResultSummary {
	completedAt := r.report.CompletedAt
	if completedAt.IsZero() {
		if r.clock != nil {
			completedAt = r.clock()
		} else {
			completedAt = time.Now()
		}
	}
	wfID := state.WorkflowID(r.report.WorkflowID)
	if wfID == "" && r.claimed != nil {
		wfID = r.claimed.ID
	}
	attemptVal := r.report.Attempt
	if attemptVal == 0 && r.claimed != nil && r.claimed.Attempt > 0 {
		attemptVal = r.claimed.Attempt
	}
	if attemptVal == 0 && r.result.Attempt > 0 {
		attemptVal = r.result.Attempt
	}

	var reason state.TerminationReason
	if r.dispatcher != nil {
		reason = r.dispatcher.normalizeReason(wfID, r.reason, r.state, r.result.Success, attemptVal, r.pool)
	} else {
		reason = normalizeReasonValue(r.reason, r.state, r.result.Success)
	}
	summary := state.ResultSummary{
		Success:         r.result.Success,
		Duration:        r.result.Duration,
		Output:          copyMap(r.result.FinalStore),
		DisabledStages:  copyBoolMap(r.result.DisabledStages),
		DisabledActions: copyBoolMap(r.result.DisabledActions),
		RemovedStages:   copyStringMap(r.result.RemovedStages),
		RemovedActions:  copyStringMap(r.result.RemovedActions),
		CompletedAt:     completedAt,
		Reason:          reason,
	}
	if stages := r.report.Stages; len(stages) > 0 {
		summary.StageStatuses = make([]state.StageStatusRecord, 0, len(stages))
		for _, stage := range stages {
			rec := state.StageStatusRecord{
				ID:          stage.ID,
				Name:        stage.Name,
				Description: stage.Description,
				Tags:        append([]string(nil), stage.Tags...),
				Dynamic:     stage.Dynamic,
				CreatedBy:   stage.CreatedBy,
				Status:      stage.Status,
			}
			summary.StageStatuses = append(summary.StageStatuses, rec)
			if len(stage.Actions) > 0 {
				if summary.ActionStatuses == nil {
					summary.ActionStatuses = make([]state.ActionStatusRecord, 0)
				}
				for _, action := range stage.Actions {
					actionRec := state.ActionStatusRecord{
						StageID:     action.StageID,
						ActionID:    action.Name,
						Name:        action.Name,
						Description: action.Description,
						Tags:        append([]string(nil), action.Tags...),
						Dynamic:     action.Dynamic,
						CreatedBy:   action.CreatedBy,
						Status:      action.Status,
					}
					summary.ActionStatuses = append(summary.ActionStatuses, actionRec)
				}
			}
		}
	}
	if summary.Attempt == 0 && attemptVal > 0 {
		summary.Attempt = attemptVal
	}
	if r.result.Error != nil {
		summary.Error = r.result.Error.Error()
	}
	return summary
}

func ExtractInitialStore(metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	raw, ok := metadata[MetadataInitialStore]
	if !ok {
		return nil
	}
	if value, ok := raw.(map[string]any); ok {
		return copyMap(value)
	}
	return nil
}

func (d *Dispatcher) applyRemoteStatuses(claimed *state.ClaimedWorkflow, summary state.ResultSummary) {
	if d == nil || d.manager == nil || claimed == nil {
		return
	}
	workflowID := string(claimed.ID)
	ctx := d.ctx
	actionStage := make(map[string]string)
	for _, stage := range claimed.Definition.Stages {
		stageID := stage.ID
		if stageID == "" {
			continue
		}
		for _, action := range stage.Actions {
			actionID := action.ID
			if actionID == "" {
				actionID = action.Ref
			}
			if actionID != "" {
				actionStage[actionID] = stageID
			}
		}
	}
	for _, st := range summary.StageStatuses {
		if st.ID == "" {
			continue
		}
		if st.Dynamic {
			stageRec := state.StageRecord{
				ID:          st.ID,
				Name:        st.Name,
				Description: st.Description,
				Tags:        append([]string(nil), st.Tags...),
				Dynamic:     st.Dynamic,
				CreatedBy:   st.CreatedBy,
				Status:      st.Status,
			}
			if err := d.manager.StageRegistered(ctx, workflowID, stageRec); err != nil {
				d.reportError("dispatcher.remote.stage_registered", fmt.Errorf("register stage %s: %w", st.ID, err))
			}
		}
		if st.Status != "" {
			if err := d.manager.StageStatus(ctx, workflowID, st.ID, st.Status); err != nil {
				d.reportError("dispatcher.remote.stage_status", fmt.Errorf("update stage %s status: %w", st.ID, err))
			}
		}
	}
	for _, act := range summary.ActionStatuses {
		if act.StageID == "" || act.ActionID == "" {
			continue
		}
		if act.Dynamic {
			actionRec := state.ActionRecord{
				Ref:         act.ActionID,
				Name:        act.Name,
				Description: act.Description,
				Tags:        append([]string(nil), act.Tags...),
				Dynamic:     act.Dynamic,
				CreatedBy:   act.CreatedBy,
				Status:      act.Status,
			}
			if err := d.manager.ActionRegistered(ctx, workflowID, act.StageID, actionRec); err != nil {
				d.reportError("dispatcher.remote.action_registered", fmt.Errorf("register action %s/%s: %w", act.StageID, act.ActionID, err))
			}
		}
		if act.Status != "" {
			if err := d.manager.ActionStatus(ctx, workflowID, act.StageID, act.ActionID, act.Status); err != nil {
				d.reportError("dispatcher.remote.action_status", fmt.Errorf("update action %s/%s status: %w", act.StageID, act.ActionID, err))
			}
		}
	}
	for stageID := range summary.DisabledStages {
		if stageID == "" {
			continue
		}
		if err := d.manager.StageStatus(ctx, workflowID, stageID, state.WorkflowSkipped); err != nil {
			d.reportError("dispatcher.remote.stage_status", fmt.Errorf("update stage %s skipped: %w", stageID, err))
		}
	}
	for stageID := range summary.RemovedStages {
		if stageID == "" {
			continue
		}
		if err := d.manager.StageStatus(ctx, workflowID, stageID, state.WorkflowRemoved); err != nil {
			d.reportError("dispatcher.remote.stage_status", fmt.Errorf("update stage %s removed: %w", stageID, err))
		}
	}
	for actionID := range summary.DisabledActions {
		stageID, ok := actionStage[actionID]
		if !ok {
			continue
		}
		if err := d.manager.ActionStatus(ctx, workflowID, stageID, actionID, state.WorkflowSkipped); err != nil {
			d.reportError("dispatcher.remote.action_status", fmt.Errorf("update action %s/%s skipped: %w", stageID, actionID, err))
		}
	}
	for key := range summary.RemovedActions {
		parts := strings.SplitN(key, "::", 2)
		if len(parts) != 2 {
			continue
		}
		stageID, actionID := parts[0], parts[1]
		if stageID == "" || actionID == "" {
			continue
		}
		if err := d.manager.ActionStatus(ctx, workflowID, stageID, actionID, state.WorkflowRemoved); err != nil {
			d.reportError("dispatcher.remote.action_status", fmt.Errorf("update action %s/%s removed: %w", stageID, actionID, err))
		}
	}
}

func copyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyBoolMap(src map[string]bool) map[string]bool {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (d *Dispatcher) normalizeReason(id state.WorkflowID, reason state.TerminationReason, finalState runner.ExecutionStatus, success bool, attempt int, pool string) state.TerminationReason {
	normalized := normalizeReasonValue(reason, finalState, success)
	if reason == "" && d != nil {
		meta := map[string]any{
			"workflow_id": string(id),
			"final_state": string(finalState),
			"success":     success,
		}
		if attempt > 0 {
			meta["attempt"] = attempt
		}
		if pool != "" {
			meta["pool"] = pool
		}
		d.reportErrorWithMetadata("dispatcher.reason.missing", fmt.Errorf("workflow %s missing termination reason", id), meta)
	}
	return normalized
}

func normalizeReasonValue(reason state.TerminationReason, finalState runner.ExecutionStatus, success bool) state.TerminationReason {
	if reason != "" {
		return reason
	}
	switch finalState {
	case runner.StatusCancelled:
		return state.TerminationReasonUserCancel
	case runner.StatusCompleted:
		if success {
			return state.TerminationReasonSuccess
		}
		return state.TerminationReasonFailure
	default:
		return state.TerminationReasonFailure
	}
}

func MetadataWithoutInitialStore(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		if k == MetadataInitialStore {
			continue
		}
		dst[k] = v
	}
	if len(dst) == 0 {
		return nil
	}
	return dst
}

func preserveInitialStoreForRetry(claimed *state.ClaimedWorkflow, store map[string]any) {
	if claimed == nil {
		return
	}
	if len(store) == 0 {
		return
	}
	if claimed.Metadata == nil {
		claimed.Metadata = make(map[string]any, 1)
	}
	claimed.Metadata[MetadataInitialStore] = copyMap(store)
}

func executionStatusToWorkflow(status runner.ExecutionStatus) state.WorkflowState {
	switch status {
	case runner.StatusPending:
		return state.WorkflowPending
	case runner.StatusRunning:
		return state.WorkflowRunning
	case runner.StatusCompleted:
		return state.WorkflowCompleted
	case runner.StatusFailed:
		return state.WorkflowFailed
	case runner.StatusSkipped:
		return state.WorkflowSkipped
	case runner.StatusRemoved:
		return state.WorkflowRemoved
	case runner.StatusCancelled:
		return state.WorkflowCancelled
	default:
		return state.WorkflowPending
	}
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
