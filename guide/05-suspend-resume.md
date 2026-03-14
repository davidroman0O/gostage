# Suspend and Resume

Some workflows cannot run to completion in a single pass. An order needs human
approval. A document requires a signature from an external service. A payment
waits for a webhook callback. The workflow reaches a point where it must stop,
save its state, and let something outside the system provide the missing input
before it can continue.

GoStage handles this with **suspend and resume**. A task calls `Suspend` to
pause the workflow. The engine persists the full state and returns a
`Suspended` result to the caller. Later -- seconds, hours, or days later -- the
caller invokes `Resume` with the external input. The engine rebuilds the
workflow from its persisted definition, injects the resume data, and
re-executes. Completed steps are skipped. The suspending task detects the resume
and takes a different code path.

## The pattern

A suspend/resume task has two branches in the same function: one that suspends,
and one that handles the resume.

```go
gostage.Task("wait-for-approval", func(ctx *gostage.Ctx) error {
    if gostage.IsResuming(ctx) {
        // Resume path: read the external input
        approved := gostage.ResumeData[bool](ctx, "approved")
        reviewer := gostage.ResumeData[string](ctx, "reviewer")
        gostage.Set(ctx, "approved", approved)
        gostage.Set(ctx, "reviewer", reviewer)
        return nil
    }

    // First-run path: suspend and wait
    reqID := gostage.Get[string](ctx, "request_id")
    return gostage.Suspend(ctx, gostage.Params{
        "reason":     "needs_approval",
        "request_id": reqID,
    })
})
```

The workflow using this task looks like any other:

```go
wf, _ := gostage.NewWorkflow("approval-flow").
    Step("submit-request").
    Step("wait-for-approval").
    Step("finalize").
    Commit()
```

The calling code drives the lifecycle:

```go
// Phase 1: run until suspension
result, _ := engine.RunSync(ctx, wf, nil)
// result.Status == "suspended"
// result.SuspendData == {"reason": "needs_approval", "request_id": "REQ-42"}

// ... time passes, external system approves ...

// Phase 2: resume with external input
result, _ = engine.Resume(ctx, result.RunID, gostage.Params{
    "approved": true,
    "reviewer": "alice@example.com",
})
// result.Status == "completed"
```

## IsResuming and ResumeData

When `Resume` is called, the engine sets an internal `__resuming` flag in the
store and re-executes the workflow from the beginning. All tasks run again,
but completed steps are skipped thanks to the persisted step statuses. The
suspending task is the first step that was **not** yet completed, so it
re-executes.

Inside that task, `IsResuming(ctx)` returns `true`. This is the signal to
take the resume code path instead of suspending again.

`ResumeData[T](ctx, key)` retrieves a typed value from the data map that was
passed to `engine.Resume`. Internally, resume data is stored under
`"__resume:"` prefixed keys in the workflow's state store, so
`ResumeData[bool](ctx, "approved")` is equivalent to
`Get[bool](ctx, "__resume:approved")`.

The resume data is available only during the resume execution. It is cleaned
up (along with all `__` prefixed internal keys) before the `Result` is
returned to the caller.

## The __resume: key prefix

The engine keeps resume data separate from application state using a naming
convention. When you call `engine.Resume(ctx, runID, Params{"approved": true})`,
the engine writes `"__resume:approved" = true` and `"__resuming" = true` into
the store. `ResumeData[T](ctx, "approved")` looks up `"__resume:approved"`
internally -- you never construct the prefixed key yourself.

After the workflow finishes, the engine strips all `"__"` prefixed keys from
`Result.Store`, so internal bookkeeping never leaks to the caller.

## Resume from persisted definition

When you call `Resume`, you pass only the run ID and the resume data. You do
**not** pass the workflow definition:

```go
result, err := engine.Resume(ctx, runID, gostage.Params{"approved": true})
```

The engine rebuilds the workflow from two sources, checked in order:

1. **In-memory cache.** If the workflow was run on this engine instance and is
   still in the LRU cache, the engine uses the cached definition.

2. **Persisted WorkflowDef.** When the engine first runs a workflow, it
   serializes the workflow's structure (step kinds, task names, condition names,
   loop configurations) into a `WorkflowDef` and saves it alongside the run
   record. On resume, the engine deserializes this definition and rebuilds the
   workflow.

This design means the caller does not need to keep the workflow object around
between suspend and resume. The run is fully self-contained in persistence.

## Anonymous closures cannot be resumed

The persisted `WorkflowDef` stores task references by name (e.g. `"charge"`,
`"validate"`), branch conditions by registered name, map functions by
registered name, and loop conditions by registered name. Everything must be
a **named, registry-resolvable reference**.

If your workflow uses anonymous closures for `Map`, `DoUntil`, `DoWhile`, or
`When`, the workflow definition is not serializable. The engine detects this
at run time and **fails the workflow immediately** rather than letting it
suspend into a state that can never be resumed:

```go
// This workflow CANNOT be suspended or resumed.
wf, _ := gostage.NewWorkflow("broken").
    Map(func(ctx *gostage.Ctx) error {       // anonymous closure
        // ...
        return nil
    }).
    Step("wait-for-approval").
    Commit()

result, _ := engine.RunSync(ctx, wf, nil)
// result.Status == "failed"
// result.Error == ErrWorkflowNotResumable
```

The fix is to use the Named variants and register functions in the registry:

```go
// Register the map function by name
gostage.MapFn("transform-data", func(ctx *gostage.Ctx) error {
    // ...
    return nil
})

// Register the condition by name
gostage.Condition("is-ready", func(ctx *gostage.Ctx) bool {
    return gostage.Get[string](ctx, "status") == "ready"
})

// Use named references in the workflow
wf, _ := gostage.NewWorkflow("resumable").
    MapNamed("transform-data").
    Step("wait-for-approval").
    DoUntilNamed(gostage.Step("poll"), "is-ready").
    Branch(
        gostage.WhenNamed("is-high-priority").Step("urgent"),
        gostage.Default().Step("normal"),
    ).
    Commit()
```

The rule is simple: if a workflow might suspend, sleep, or need crash recovery,
every closure must be replaced with a named registry reference.

## Bail: intentional early exit

`Bail` is a related control flow primitive, but it serves a different purpose.
While `Suspend` pauses the workflow for later continuation, `Bail` terminates
it permanently with an intentional, non-error exit.

```go
gostage.Task("check-eligibility", func(ctx *gostage.Ctx) error {
    age := gostage.Get[int](ctx, "age")
    if age < 18 {
        return gostage.Bail(ctx, "Must be 18 or older")
    }
    return nil
})
```

A bailed workflow has `result.Status == "bailed"` and the reason is available
in `result.BailReason`. Unlike a failed workflow, bail is not an error -- it is
a business logic decision that the workflow should stop. The engine does not
retry bailed steps.

Key differences between bail, suspend, and failure:

| Signal | Status | Retried? | Resumable? | Purpose |
|--------|--------|----------|------------|---------|
| `return nil` | completed | -- | -- | Step succeeded |
| `return err` | failed | Yes (if retries configured) | No | Step error |
| `Bail` | bailed | No | No | Intentional early exit |
| `Suspend` | suspended | No | Yes, via `Resume` | Waiting for external input |
| `Sleep` | sleeping | No | Yes, via timer | Timed delay |

## Multi-cycle suspend/resume

A workflow can suspend and resume multiple times. The task checks
`IsResuming`, handles the resume data, and either completes or suspends again
to wait for the next piece of input. From the caller's side this looks like
repeated `Resume` calls:

```go
result, _ := engine.RunSync(ctx, wf, nil)                                    // -> suspended
result, _ = engine.Resume(ctx, result.RunID, gostage.Params{"approved": true}) // -> suspended again
result, _ = engine.Resume(ctx, result.RunID, gostage.Params{"approved": true}) // -> completed
```

Each cycle persists the updated state, so the workflow survives crashes
between any two resume calls.

## Dynamic mutations and concurrency

Dynamic mutations (via `InsertAfter`, `DisableStep`, `EnableStep`) made before
a suspend are captured and persisted. On resume, the engine replays them to
reconstruct the same step sequence. The dynamic step counter is also restored
so post-resume mutations generate unique, non-colliding IDs.

The engine prevents concurrent resumes on the same run. If two goroutines call
`Resume` with the same run ID simultaneously, one succeeds and the other
receives `ErrRunAlreadyActive`.

## Error reference

| Error | When |
|-------|------|
| `ErrRunNotFound` | The run ID does not exist in persistence |
| `ErrRunNotSuspended` | The run is not in `suspended` status |
| `ErrRunAlreadyActive` | Another goroutine is already executing or resuming this run |
| `ErrWorkflowNotResumable` | The workflow uses anonymous closures that cannot be serialized |
| `ErrEngineClosed` | The engine has been shut down |

## Further reading

- [Example 07: Suspend and Resume](../../examples/07-suspend-resume/) --
  complete working example with SQLite persistence
- [Example 08: Sleep Recovery](../../examples/08-sleep-recovery/) --
  sleep/wake with crash recovery (a related persistence pattern)
- [Persistence guide](./04-persistence.md) -- how the storage layer works
- [Retry Strategies guide](./06-retry-strategies.md) -- configuring retries
  (retries and suspend are complementary: retries handle transient failures,
  suspend handles external dependencies)
