# gostage

A workflow engine for Go with persistence, suspend/resume, child process spawning, and type-safe state.

## What it does

gostage runs multi-step workflows that can pause, survive crashes, and resume. A workflow that suspends waiting for a payment callback will pick up exactly where it left off — even after a process restart. State is persisted per-step, so a crash mid-workflow loses at most the current step's writes.

```go
gostage.Task("greet", func(ctx *gostage.Ctx) error {
    name := gostage.GetOr[string](ctx, "name", "World")
    return gostage.Set(ctx, "greeting", "Hello, "+name+"!")
})

wf, _ := gostage.NewWorkflow("hello").Step("greet").Commit()
engine, _ := gostage.New()
defer engine.Close()

result, _ := engine.RunSync(ctx, wf, gostage.Params{"name": "Alice"})
fmt.Println(result.Store["greeting"]) // Hello, Alice!
```

## Install

```bash
go get github.com/davidroman0O/gostage
```

Requires Go 1.24+. No CGo — cross-compiles cleanly.

## Step kinds

The builder supports 10 step kinds that compose freely:

```go
wf, _ := gostage.NewWorkflow("order").
    Step("validate").                                             // single task
    Stage("checks", gostage.Step("credit"), gostage.Step("fraud")).  // sequential group
    Parallel(gostage.Step("reserve"), gostage.Step("charge")).    // concurrent fan-out
    Branch(                                                      // conditional
        gostage.WhenNamed("is-priority").Step("expedite"),
        gostage.Default().Step("standard"),
    ).
    ForEach("items", gostage.Step("process"), gostage.WithConcurrency(4)). // collection iteration
    MapNamed("summarize").                                       // inline transform
    DoUntilNamed(gostage.Step("poll"), "is-ready").              // repeat until condition
    Sub(otherWorkflow).                                          // nested workflow
    Sleep(time.Hour).                                            // persistent timed delay
    Step("notify").
    Commit()
```

## Suspend and resume

A task can suspend the workflow to wait for external input. The engine persists all state and resumes from the exact same step.

```go
gostage.Task("wait-for-payment", func(ctx *gostage.Ctx) error {
    if gostage.IsResuming(ctx) {
        txID := gostage.ResumeData[string](ctx, "transaction_id")
        gostage.Set(ctx, "paid", true)
        return nil
    }
    return gostage.Suspend(ctx, gostage.Params{"reason": "awaiting_payment"})
})

// Start — suspends at payment step
result, _ := engine.RunSync(ctx, wf, gostage.Params{"order_id": "ORD-1"})
// result.Status == Suspended

// Later — webhook arrives, resume with payment data
result, _ = engine.Resume(ctx, result.RunID, gostage.Params{"transaction_id": "TXN-42"})
// result.Status == Completed
```

Resume rebuilds the workflow from the persisted definition — the caller doesn't pass the workflow object, eliminating version mismatch bugs.

## Persistence

SQLite is the default durable backend. State is flushed after each step and survives crashes.

```go
engine, _ := gostage.New(gostage.WithSQLite("app.db"))
```

Sleeping workflows wake automatically after restart:

```go
engine, _ := gostage.New(
    gostage.WithSQLite("app.db"),
    gostage.WithAutoRecover(),
)
```

## Retry with backoff

Tasks can retry with pluggable strategies. Built-in: fixed delay, exponential backoff, exponential with jitter.

```go
gostage.Task("call-api", handler,
    gostage.WithRetry(5),
    gostage.WithRetryStrategy(
        gostage.ExponentialBackoffWithJitter(100*time.Millisecond, 30*time.Second),
    ),
)
```

## Child process spawning

ForEach items can run in isolated child processes for crash isolation. The spawn subsystem is in a separate `spawn/` package — if you don't import it, the gRPC dependency is not pulled in.

```go
import "github.com/davidroman0O/gostage/spawn"

engine, _ := gostage.New(
    gostage.WithSQLite("app.db"),
    spawn.WithSpawn(),
)

wf, _ := gostage.NewWorkflow("batch").
    ForEach("items", gostage.Step("process"), gostage.WithSpawn()).
    Commit()
```

## Middleware

Four levels of middleware wrap execution at engine, step, task, and child process levels:

```go
engine, _ := gostage.New(
    gostage.WithStepMiddleware(func(ctx context.Context, info gostage.StepInfo, runID gostage.RunID, next func() error) error {
        start := time.Now()
        err := next()
        fmt.Printf("step %s took %s\n", info.Name, time.Since(start))
        return err
    }),
)
```

Or use the Plugin interface to register at all levels at once.

## Observability

Event handlers receive lifecycle events that middleware cannot observe — run creation, status transitions, cancellation, deletion, and timer wake events:

```go
engine, _ := gostage.New(
    gostage.WithEventHandler(myHandler), // implements gostage.EventHandler
)
```

## Run garbage collection

Terminal runs are purged automatically or on demand:

```go
engine, _ := gostage.New(
    gostage.WithRunGC(24*time.Hour, 5*time.Minute), // purge runs older than 24h every 5m
)

// Or manually:
purged, _ := engine.PurgeRuns(ctx, time.Hour) // purge runs older than 1h
```

## Engine isolation

Each engine can have its own task registry for multi-tenant or testing scenarios:

```go
reg := gostage.NewRegistry()
reg.RegisterTask("compute", myHandler)

engine, _ := gostage.New(gostage.WithRegistry(reg))
```

## Scoped message handlers

IPC message handlers can be scoped to a specific run:

```go
engine.OnMessageForRun("progress", runID, func(msgType string, payload map[string]any) {
    fmt.Printf("run %s: %v\n", runID, payload)
})
```

## Examples

The `examples/` directory has 16 standalone programs organized by audience:

**Getting started:**
- [01-hello-world](examples/01-hello-world) — single task, state read/write
- [02-multi-step](examples/02-multi-step) — sequential steps with state flow
- [03-parallel](examples/03-parallel) — concurrent fan-out
- [04-foreach](examples/04-foreach) — collection iteration with concurrency
- [05-branching](examples/05-branching) — conditional execution with named conditions

**Core features:**
- [06-suspend-resume](examples/06-suspend-resume) — full suspend/resume lifecycle
- [07-sleep-recovery](examples/07-sleep-recovery) — crash recovery for sleeping workflows
- [08-retry-backoff](examples/08-retry-backoff) — exponential backoff with jitter
- [09-order-processing](examples/09-order-processing) — e-commerce workflow with payment callback

**Architecture patterns:**
- [10-etl-pipeline](examples/10-etl-pipeline) — parallel extract, named transform, load
- [11-approval-workflow](examples/11-approval-workflow) — branching with dynamic step insertion
- [12-deep-composition](examples/12-deep-composition) — ForEach + Sub + Branch + Parallel nested

**Advanced:**
- [13-middleware](examples/13-middleware) — custom step and task middleware
- [14-events](examples/14-events) — lifecycle event monitoring
- [15-custom-registry](examples/15-custom-registry) — engine isolation with separate registries
- [16-run-gc](examples/16-run-gc) — automatic and manual run garbage collection
- [17-spawn-processes](examples/17-spawn-processes) — ForEach with child process spawning for crash isolation

## Testing

```bash
go test ./... -race        # 266 tests, race detector
go test -run TestE2E_ -v   # end-to-end workflow patterns
go test -run TestStress_ -v # adversarial stress tests
```

## License

MIT
