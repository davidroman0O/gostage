# Middleware

GoStage middleware lets you wrap execution at four levels, from coarse to fine.
Each level gives you a `next` function. Call it to proceed, or skip it to
short-circuit. This is the primary mechanism for cross-cutting concerns like
logging, timing, error wrapping, and access control.


## The four levels

| Level | Type | What it wraps |
|-------|------|---------------|
| **Engine** | `EngineMiddleware` | The entire workflow run |
| **Step** | `StepMiddleware` | A single step (task, ForEach, group, sub-workflow) |
| **Task** | `TaskMiddleware` | The task function invocation within a step |
| **Child** | `ChildMiddleware` | A child process execution in spawned ForEach |

Each has a distinct signature:

```go
type EngineMiddleware func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error
type StepMiddleware   func(ctx context.Context, info StepInfo, runID RunID, next func() error) error
type TaskMiddleware   func(tctx *Ctx, taskName string, next func() error) error
type ChildMiddleware  func(ctx context.Context, job *SpawnJob, next func() error) error
```


## The next() pattern

Every middleware receives `next`. Calling it runs the wrapped operation (or the
next middleware in the chain). You can run code before and after, short-circuit
by returning an error without calling `next()`, or inspect and modify the
result:

```go
func timingMW(ctx context.Context, info gostage.StepInfo, runID gostage.RunID, next func() error) error {
    start := time.Now()
    err := next()
    log.Printf("step %s took %s", info.ID, time.Since(start))
    return err
}
```

If you never call `next()`, the wrapped operation never runs. This is
intentional -- middleware can act as a gate.


## Registration order

Middleware is registered via engine options and builds an onion-style chain. The
**first registered** middleware is the **outermost wrapper**:

```go
engine, _ := gostage.New(
    gostage.WithEngineMiddleware(loggingMW),   // outermost
    gostage.WithEngineMiddleware(timingMW),    // inner
    gostage.WithEngineMiddleware(retryMW),     // innermost
)
```

Execution flows: `loggingMW -> timingMW -> retryMW -> workflow`. Errors bubble
back in reverse.


## Engine middleware

Wraps the entire `executeWorkflow` call. Fires once per run and has access to
the workflow and run ID. Panics inside the chain are recovered and surfaced as
errors.

```go
gostage.WithEngineMiddleware(func(ctx context.Context, wf *gostage.Workflow, runID gostage.RunID, next func() error) error {
    log.Printf("run %s starting", runID)
    err := next()
    if err != nil {
        log.Printf("run %s failed: %v", runID, err)
    }
    return err
})
```


## Step middleware

Wraps each step. Receives `StepInfo` with step metadata:

```go
type StepInfo struct {
    ID       string
    Name     string
    Kind     StepKind
    Tags     []string
    Disabled bool
    TaskName string
}
```

This lets middleware make decisions based on tags, kind, or name:

```go
gostage.WithStepMiddleware(func(ctx context.Context, info gostage.StepInfo, runID gostage.RunID, next func() error) error {
    if info.Disabled {
        return next() // still runs -- disabled steps are skipped by the engine, not middleware
    }
    for _, tag := range info.Tags {
        if tag == "critical" {
            log.Printf("CRITICAL step %s starting", info.ID)
        }
    }
    return next()
})
```

Engine-level step middleware applies to every step in every workflow.


## Task middleware

Wraps the task function itself. Receives `*Ctx` and the task name. This is the
right place for concerns that need access to the task context:

```go
gostage.WithTaskMiddleware(func(tctx *gostage.Ctx, taskName string, next func() error) error {
    err := next()
    if err != nil {
        return fmt.Errorf("task %s: %w", taskName, err)
    }
    return nil
})
```


## Child middleware

Wraps spawning and execution of a child process. Runs in the **parent**, around
the child's entire lifecycle. If it returns an error without calling `next()`,
the child is never spawned:

```go
gostage.WithChildMiddleware(func(ctx context.Context, job *gostage.SpawnJob, next func() error) error {
    log.Printf("spawning child for job %s", job.ID)
    start := time.Now()
    err := next()
    log.Printf("child %s finished in %s", job.ID, time.Since(start))
    return err
})
```


## Workflow-scoped middleware

Attach step middleware to a specific workflow with `WithWorkflowMiddleware`. It
runs for every step in that workflow, in addition to engine-level step
middleware. Engine middleware forms the outer layers; workflow middleware forms
the inner layers:

```go
wf, _ := gostage.NewWorkflow("monitored",
    gostage.WithWorkflowMiddleware(timingMW),
).Step("fetch").Step("transform").Build()
```


## The Plugin interface

When you need middleware at multiple levels, implement `Plugin`:

```go
type Plugin interface {
    EngineMiddleware() EngineMiddleware  // return nil to skip
    StepMiddleware()   StepMiddleware
    TaskMiddleware()   TaskMiddleware
    ChildMiddleware()  ChildMiddleware
}
```

Register with `WithPlugin`. Each non-nil return is registered at its level:

```go
engine, _ := gostage.New(gostage.WithPlugin(myPlugin))
```


## Built-in LoggingPlugin

GoStage ships with `LoggingPlugin` that logs start/end events with durations at
engine, step, and task levels:

```go
engine, _ := gostage.New(gostage.WithPlugin(gostage.LoggingPlugin(myLogger)))
```

It installs engine, step, and task middleware. Child middleware returns nil.
The engine-level middleware as reference:

```go
func (p *loggingPlugin) EngineMiddleware() EngineMiddleware {
    return func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error {
        p.logger.Info("workflow %s run %s started", wf.ID, runID)
        start := time.Now()
        err := next()
        dur := time.Since(start)
        if err != nil {
            p.logger.Error("workflow %s run %s failed after %s: %v", wf.ID, runID, dur, err)
        } else {
            p.logger.Info("workflow %s run %s completed in %s", wf.ID, runID, dur)
        }
        return err
    }
}
```


## Practical examples

**Circuit breaker** -- skip execution when a service is down:

```go
gostage.WithStepMiddleware(func(ctx context.Context, info gostage.StepInfo, runID gostage.RunID, next func() error) error {
    if info.TaskName != "" && breaker.IsOpen(info.TaskName) {
        return fmt.Errorf("circuit open for %s", info.TaskName)
    }
    err := next()
    if err != nil { breaker.RecordFailure(info.TaskName) } else { breaker.RecordSuccess(info.TaskName) }
    return err
})
```

**Access control** -- gate tasks on permissions:

```go
gostage.WithTaskMiddleware(func(tctx *gostage.Ctx, taskName string, next func() error) error {
    role := gostage.Get[string](tctx, "user.role")
    if strings.HasPrefix(taskName, "admin.") && role != "admin" {
        return fmt.Errorf("unauthorized: %s requires admin role", taskName)
    }
    return next()
})
```


## Panic safety

The engine recovers panics in engine middleware and child middleware chains. A
panicking middleware produces an error rather than crashing the process.
Middleware should still be written defensively -- a recovered panic may leave
shared state inconsistent.
