package gostage

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/runner"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

const metadataInitialStore = "gostage.initial_store"

type dispatcher struct {
	ctx    context.Context
	cancel context.CancelFunc

	queue   state.Queue
	store   state.Store
	manager state.Manager

	runner *runner.Runner

	pools []*poolBinding

	telemetry   *node.TelemetryDispatcher
	diagnostics node.DiagnosticsWriter
	health      *node.HealthDispatcher

	logger telemetry.Logger

	claimInterval    time.Duration
	jitter           time.Duration
	maxInFlight      int
	failurePolicy    FailurePolicy
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
	wg        sync.WaitGroup
}

type poolBinding struct {
	pool *pools.Local
}

func newDispatcher(ctx context.Context, queue state.Queue, store state.Store, manager state.Manager, runner *runner.Runner, telemetryDisp *node.TelemetryDispatcher, diag node.DiagnosticsWriter, health *node.HealthDispatcher, logger telemetry.Logger, claimInterval, jitter time.Duration, maxInFlight int, failure FailurePolicy, poolBindings []*poolBinding) *dispatcher {
	if ctx == nil {
		ctx = context.Background()
	}
	dctx, cancel := context.WithCancel(ctx)
	d := &dispatcher{
		ctx:           dctx,
		cancel:        cancel,
		queue:         queue,
		store:         store,
		runner:        runner,
		manager:       manager,
		pools:         poolBindings,
		telemetry:     telemetryDisp,
		diagnostics:   diag,
		health:        health,
		logger:        logger,
		claimInterval: claimInterval,
		jitter:        jitter,
		maxInFlight:   maxInFlight,
		failurePolicy: failure,
	}
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
	d.cancels = make(map[state.WorkflowID]context.CancelFunc)
	d.pendingCancels = make(map[state.WorkflowID]struct{})
	return d
}

func (d *dispatcher) emitWorkflowEvent(kind telemetry.EventKind, id state.WorkflowID, attempt int, metadata map[string]any, err error) {
	if d == nil || d.telemetry == nil {
		return
	}
	evt := telemetry.Event{
		Kind:       kind,
		WorkflowID: string(id),
		Attempt:    attempt,
		Timestamp:  time.Now(),
		Metadata:   copyMap(metadata),
	}
	if err != nil {
		evt.Error = err.Error()
	}
	d.telemetry.Dispatch(evt)
}

func (d *dispatcher) start() {
	d.wg.Add(1)
	go d.loop()
	if d.health != nil {
		for _, binding := range d.pools {
			d.publishHealth(binding.pool.Name(), node.HealthHealthy, "ready")
		}
	}
}

func (d *dispatcher) loop() {
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

func (d *dispatcher) pollOnce() {
	if max := d.maxInFlight; max > 0 && int(d.inflight.Load()) >= max {
		return
	}
	for _, binding := range d.pools {
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
		d.inflight.Add(1)
		d.wg.Add(1)
		go d.execute(pool, release, claimed)
	}
}

func (d *dispatcher) execute(pool *pools.Local, release func(), claimed *state.ClaimedWorkflow) {
	defer d.wg.Done()
	defer d.inflight.Add(-1)
	defer release()

	execCtx, cancel := context.WithCancel(d.ctx)
	defer cancel()
	d.registerCancel(claimed.ID, cancel)
	defer d.unregisterCancel(claimed.ID)

	d.emitWorkflowEvent(telemetry.EventWorkflowStarted, claimed.ID, claimed.Attempt, map[string]any{
		"pool": pool.Name(),
	}, nil)

	result, err := d.runWorkflow(execCtx, pool, claimed)
	if err != nil {
		d.reportError("dispatcher.run", err)
		d.publishHealth(pool.Name(), node.HealthUnavailable, err.Error())
	} else if d.diagnostics != nil {
		d.diagnostics.Write(diagnostics.Event{
			OccurredAt: time.Now(),
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

	decision := d.decideFailure(claimed, result)
	cancelledEmitted := false
	switch decision {
	case FailureDecisionRetry:
		d.emitWorkflowEvent(telemetry.EventWorkflowRetry, claimed.ID, claimed.Attempt, map[string]any{
			"pool":  pool.Name(),
			"error": errorMessage(result.result.Error),
		}, result.result.Error)
		if err := d.queue.Release(d.ctx, claimed.ID); err != nil {
			d.reportError("dispatcher.release", fmt.Errorf("release workflow %s: %w", claimed.ID, err))
			d.publishHealth(pool.Name(), node.HealthUnavailable, err.Error())
		}
		return
	case FailureDecisionCancel:
		d.emitWorkflowEvent(telemetry.EventWorkflowCancelled, claimed.ID, claimed.Attempt, map[string]any{
			"pool":   pool.Name(),
			"reason": "failure_policy",
		}, result.result.Error)
		cancelledEmitted = true
	}

	if !cancelledEmitted && result.state == runner.StatusCancelled {
		metadata := map[string]any{
			"pool":   pool.Name(),
			"reason": "explicit_request",
		}
		if msg := errorMessage(result.result.Error); msg != "" {
			metadata["error"] = msg
		}
		d.emitWorkflowEvent(telemetry.EventWorkflowCancelled, claimed.ID, claimed.Attempt, metadata, result.result.Error)
	}

	summary := result.toSummary()
	if err := d.queue.Ack(d.ctx, claimed.ID, summary); err != nil {
		d.reportError("dispatcher.ack", fmt.Errorf("ack workflow %s: %w", claimed.ID, err))
		d.publishHealth(pool.Name(), node.HealthUnavailable, err.Error())
	} else {
		d.recordCompletion(result.state)
	}
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
}

func (d *dispatcher) runWorkflow(ctx context.Context, pool *pools.Local, claimed *state.ClaimedWorkflow) (runResult, error) {
	def := claimed.Definition.Clone()
	def.ID = string(claimed.ID)
	wf, err := workflow.Materialize(def, registry.Default())
	if err != nil {
		return runResult{}, fmt.Errorf("materialize workflow %s: %w", def.ID, err)
	}

	initialStore := extractInitialStore(claimed.Metadata)

	opts := runner.RunOptions{
		Context:      ctx,
		Logger:       d.logger,
		InitialStore: initialStore,
		Attempt:      claimed.Attempt,
	}
	start := time.Now()
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

	return runResult{claimed: claimed, result: res, report: report, state: finalState}, nil
}

func (d *dispatcher) stop() {
	d.cancel()
	d.wg.Wait()
}

func (d *dispatcher) registerCancel(id state.WorkflowID, cancel context.CancelFunc) {
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

func (d *dispatcher) unregisterCancel(id state.WorkflowID) {
	d.cancelMu.Lock()
	delete(d.cancels, id)
	d.cancelMu.Unlock()
}

func (d *dispatcher) cancelWorkflow(id state.WorkflowID) {
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

func (d *dispatcher) publishHealth(pool string, status node.HealthStatus, detail string) {
	if d.health == nil {
		return
	}
	now := time.Now()
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

func (d *dispatcher) healthInfo(pool string) (node.HealthStatus, string, time.Time, string, time.Time) {
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

func (d *dispatcher) recordCompletion(state runner.ExecutionStatus) {
	switch state {
	case runner.StatusCompleted:
		d.completed.Add(1)
	case runner.StatusCancelled:
		d.cancelled.Add(1)
	default:
		d.failed.Add(1)
	}
}

func (d *dispatcher) statsCounters() (completed, failed, cancelled int) {
	if d == nil {
		return 0, 0, 0
	}
	return int(d.completed.Load()), int(d.failed.Load()), int(d.cancelled.Load())
}

func (d *dispatcher) decideFailure(claimed *state.ClaimedWorkflow, result runResult) FailureDecision {
	if result.state == runner.StatusCancelled {
		return FailureDecisionAck
	}
	if result.result.Success || d.failurePolicy == nil {
		return FailureDecisionAck
	}
	ctx := FailureContext{
		WorkflowID: claimed.ID,
		Attempt:    claimed.Attempt,
		Err:        result.result.Error,
	}
	switch decision := d.failurePolicy.Decide(d.ctx, ctx); decision {
	case FailureDecisionRetry:
		return FailureDecisionRetry
	case FailureDecisionCancel:
		return FailureDecisionCancel
	default:
		return FailureDecisionAck
	}
}

func (d *dispatcher) reportError(component string, err error) {
	if err == nil {
		return
	}
	if d.diagnostics != nil {
		d.diagnostics.Write(diagnostics.Event{
			OccurredAt: time.Now(),
			Component:  component,
			Severity:   diagnostics.SeverityError,
			Err:        err,
		})
	}
	if d.logger != nil {
		d.logger.Error(component, "error", err)
	}
}

type runResult struct {
	claimed *state.ClaimedWorkflow
	result  runner.RunResult
	report  state.ExecutionReport
	state   runner.ExecutionStatus
}

func (r runResult) toSummary() state.ResultSummary {
	summary := state.ResultSummary{
		Success:         r.result.Success,
		Duration:        r.result.Duration,
		Output:          copyMap(r.result.FinalStore),
		DisabledStages:  copyBoolMap(r.result.DisabledStages),
		DisabledActions: copyBoolMap(r.result.DisabledActions),
		RemovedStages:   copyStringMap(r.result.RemovedStages),
		RemovedActions:  copyStringMap(r.result.RemovedActions),
		CompletedAt:     time.Now(),
	}
	if r.claimed != nil && r.claimed.Attempt > 0 {
		summary.Attempt = r.claimed.Attempt
	} else if r.result.Attempt > 0 {
		summary.Attempt = r.result.Attempt
	}
	if r.result.Error != nil {
		summary.Error = r.result.Error.Error()
	}
	return summary
}

func extractInitialStore(metadata map[string]any) map[string]any {
	if metadata == nil {
		return nil
	}
	raw, ok := metadata[metadataInitialStore]
	if !ok {
		return nil
	}
	if value, ok := raw.(map[string]any); ok {
		return copyMap(value)
	}
	return nil
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

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
