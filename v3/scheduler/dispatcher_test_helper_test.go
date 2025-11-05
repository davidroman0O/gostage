package scheduler_test

import (
    "context"
    "time"

    "github.com/davidroman0O/gostage/v3/bootstrap"
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
    failure bootstrap.FailurePolicy,
    bindings []*scheduler.Binding,
    clock func() time.Time,
) *scheduler.Dispatcher {
    opts := scheduler.Options{
        ClaimInterval: claimInterval,
        Jitter:        jitter,
        MaxInFlight:   maxInFlight,
        FailurePolicy: failure,
        Clock:         clock,
    }
    return scheduler.New(ctx, queue, store, manager, run, telemetryDisp, diag, health, logger, bindings, opts)
}
