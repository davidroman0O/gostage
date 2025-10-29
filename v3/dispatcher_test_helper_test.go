package gostage

import (
	"context"
	"time"

	"github.com/davidroman0O/gostage/v3/runner"
	"github.com/davidroman0O/gostage/v3/scheduler"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

func newTestDispatcher(
	ctx context.Context,
	queue state.Queue,
	store state.Store,
	manager state.Manager,
	run *runner.Runner,
	telemetryDisp scheduler.TelemetryDispatcher,
	diag scheduler.DiagnosticsWriter,
	health scheduler.HealthPublisher,
	logger telemetry.Logger,
	claimInterval, jitter time.Duration,
	maxInFlight int,
	failure FailurePolicy,
	poolBindings []*poolBinding,
	clock func() time.Time,
) *scheduler.Dispatcher {
	schedBindings := convertBindings(poolBindings)
	opts := scheduler.Options{
		ClaimInterval: claimInterval,
		Jitter:        jitter,
		MaxInFlight:   maxInFlight,
		FailurePolicy: failure,
		Clock:         clock,
	}
	return scheduler.New(ctx, queue, store, manager, run, telemetryDisp, diag, health, logger, schedBindings, opts)
}

func convertBindings(bindings []*poolBinding) []*scheduler.Binding {
	out := make([]*scheduler.Binding, 0, len(bindings))
	for _, binding := range bindings {
		if binding == nil {
			continue
		}
	if binding.Sched == nil {
		binding.Sched = &scheduler.Binding{Pool: binding.Pool}
		if binding.Remote != nil {
			binding.Remote.Parent = binding
			binding.Sched.Remote = binding.Remote
		}
	}
	out = append(out, binding.Sched)
	}
	return out
}
