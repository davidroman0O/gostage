package gostage

import (
	"context"
	"encoding/json"
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

	queue state.Queue
	store state.Store

	runner *runner.Runner

	pools []*poolBinding

	telemetry   *node.TelemetryDispatcher
	diagnostics node.DiagnosticsWriter
	health      *node.HealthDispatcher

	logger telemetry.Logger

	claimInterval time.Duration
	jitter        time.Duration

	inflight atomic.Int32
	wg       sync.WaitGroup
}

type poolBinding struct {
	pool *pools.Local
}

func newDispatcher(ctx context.Context, queue state.Queue, store state.Store, runner *runner.Runner, telemetryDisp *node.TelemetryDispatcher, diag node.DiagnosticsWriter, health *node.HealthDispatcher, logger telemetry.Logger, claimInterval, jitter time.Duration, poolBindings []*poolBinding) *dispatcher {
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
		pools:         poolBindings,
		telemetry:     telemetryDisp,
		diagnostics:   diag,
		health:        health,
		logger:        logger,
		claimInterval: claimInterval,
		jitter:        jitter,
	}
	if d.claimInterval <= 0 {
		d.claimInterval = defaultClaimInterval
	}
	return d
}

func (d *dispatcher) start() {
	d.wg.Add(1)
	go d.loop()
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
	for _, binding := range d.pools {
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
			}
			continue
		}
		if claimed == nil {
			release()
			continue
		}
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

	result, err := d.runWorkflow(execCtx, pool, claimed)
	if err != nil {
		d.reportError("dispatcher.run", err)
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

	summary := result.toSummary()
	if err := d.queue.Ack(d.ctx, claimed.ID, summary); err != nil {
		d.reportError("dispatcher.ack", fmt.Errorf("ack workflow %s: %w", claimed.ID, err))
	}
	if d.store != nil {
		if err := d.store.StoreSummary(d.ctx, claimed.ID, summary); err != nil {
			d.reportError("dispatcher.store", fmt.Errorf("store summary %s: %w", claimed.ID, err))
		}
	}
}

func (d *dispatcher) runWorkflow(ctx context.Context, pool *pools.Local, claimed *state.ClaimedWorkflow) (runResult, error) {
	def := claimed.Definition.Clone()
	wf, err := workflow.Materialize(def, registry.Default())
	if err != nil {
		return runResult{}, fmt.Errorf("materialize workflow %s: %w", def.ID, err)
	}

	initialStore := extractInitialStore(claimed.Metadata)

	opts := runner.RunOptions{
		Context:      ctx,
		Logger:       d.logger,
		InitialStore: initialStore,
	}
	res := d.runner.Run(wf, opts)

	evt := telemetry.Event{
		Kind:       "workflow.completed",
		WorkflowID: string(claimed.ID),
		Timestamp:  time.Now(),
		Metadata: map[string]any{
			"success": res.Success,
			"pool":    pool.Name(),
		},
	}
	if res.Error != nil {
		evt.Err = res.Error
	}
	if d.telemetry != nil {
		d.telemetry.Dispatch(evt)
	}

	return runResult{claimed: claimed, result: res}, nil
}

func (d *dispatcher) stop() {
	d.cancel()
	d.wg.Wait()
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
	switch value := raw.(type) {
	case map[string]any:
		return copyMap(value)
	case string:
		var data map[string]any
		if err := json.Unmarshal([]byte(value), &data); err == nil {
			return data
		}
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
