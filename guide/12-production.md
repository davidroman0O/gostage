# Production Configuration

This guide covers the engine options and operational APIs that matter when
running GoStage in production. Each section explains a single concern, what
the defaults are, and when to change them.

## Run garbage collection

Terminal runs (Completed, Failed, Bailed, Cancelled) accumulate in
persistence over time. Without cleanup, the database grows indefinitely.

### Automatic GC

`WithRunGC` starts a background goroutine that sweeps terminal runs on a
timer:

```go
engine, _ := gostage.New(
    gostage.WithSQLite("app.db"),
    gostage.WithRunGC(24*time.Hour, 5*time.Minute),
)
```

The first argument is the **TTL**: runs whose `UpdatedAt` timestamp is
older than this value are eligible for deletion. The second argument is
the **sweep interval**: how often the GC goroutine wakes up and scans.

A TTL of zero means "delete all terminal runs on every sweep, regardless
of age." This is useful in development but rarely appropriate in production,
where you may want to keep runs around for debugging or auditing.

Non-terminal runs (Running, Sleeping, Suspended) are never touched by GC,
no matter the TTL.

The GC goroutine stops automatically when the engine is closed.

### Manual purge

For more control, call `PurgeRuns` directly:

```go
purged, err := engine.PurgeRuns(ctx, 48*time.Hour)
fmt.Printf("deleted %d old runs\n", purged)
```

This does the same thing as a single GC sweep: it lists terminal runs
older than the TTL and deletes them. A TTL of zero deletes all terminal
runs. Individual deletion failures are logged and skipped; the returned
count reflects how many succeeded.

You can use `PurgeRuns` from a cron job, an admin endpoint, or a manual
maintenance script without enabling automatic GC.

## Crash recovery

When a process crashes or is killed, workflow runs may be left in
inconsistent states in persistence. `WithAutoRecover` tells the engine to
scan for and fix these runs during startup:

```go
engine, _ := gostage.New(
    gostage.WithSQLite("app.db"),
    gostage.WithAutoRecover(),
)
```

Recovery applies the following rules:

| Persisted status | Recovery action |
|---|---|
| Running | Marked as **Failed**. A running workflow found after restart indicates a crash mid-execution. There is no safe way to resume from an unknown step. |
| Sleeping (wake time in the past) | **Woken immediately.** The workflow definition is rebuilt from the persisted `WorkflowDef`, the state is restored, and execution resumes from the step after the sleep. |
| Sleeping (wake time in the future) | **Re-registered** with the timer scheduler. The workflow will wake at the originally scheduled time. |
| Sleeping (no wake time) | Marked as **Failed**. A sleeping run without a wake time is corrupted. |
| Suspended | **Left untouched.** These runs are explicitly waiting for external input via `Resume`. |

If listing runs from persistence fails during recovery, `New` returns the
error and the engine is not created. Individual run recovery failures are
logged but do not abort the overall recovery process.

You can also call `Recover` manually at any time:

```go
if err := engine.Recover(ctx); err != nil {
    log.Printf("recovery error: %v", err)
}
```

## Shutdown

`Close` performs an ordered shutdown:

1. **Signal**: closes the internal shutdown channel, preventing new runs
   from starting. Any call to `RunSync`, `Run`, or `Resume` after this
   point returns `ErrEngineClosed`.
2. **Stop scheduler**: no more sleep-wake timers fire.
3. **Cancel active runs**: all in-flight runs receive context cancellation.
4. **Drain worker pool**: waits for in-flight jobs to finish.
5. **Close spawn runner**: flushes child process state.
6. **Close persistence**: last, so in-flight runs can complete their final
   persistence writes.

`Close` is safe to call multiple times. The second and subsequent calls
return nil immediately.

### Shutdown timeout

By default, step 4 blocks indefinitely -- if a workflow is stuck, `Close`
hangs. Use `WithShutdownTimeout` to set a deadline:

```go
engine, _ := gostage.New(
    gostage.WithSQLite("app.db"),
    gostage.WithShutdownTimeout(30*time.Second),
)
```

If the worker pool does not drain within 30 seconds, `Close` proceeds with
a one-second grace period for final persistence writes and then closes the
persistence layer. Workers that are still running after the grace period
are abandoned (their goroutines will eventually exit when the process ends).
A warning is logged when this happens.

In production, set this to something reasonable for your longest-running
task. A value of 30-60 seconds is common.

## State limits

Each workflow run maintains a key-value store that grows as tasks call
`Set`. Without limits, a runaway workflow (or a bug in a loop) can consume
unbounded memory and persistence storage.

```go
engine, _ := gostage.New(
    gostage.WithSQLite("app.db"),
    gostage.WithStateLimit(500),
)
```

When the limit is reached, attempts to set a **new** key are rejected with
`ErrStateLimitExceeded`. Updates to **existing** keys always succeed,
regardless of the limit. This prevents legitimate workflows from breaking
when they update state they already own, while stopping unbounded growth.

The default is 0 (unlimited). Set a limit that reflects the maximum number
of distinct keys you expect a well-behaved workflow to create. 500-1000 is
a reasonable starting point for most workloads.

## Workflow cache

The engine keeps an LRU (least-recently-used) cache of workflow templates.
This cache serves two purposes:

1. **Sleep/wake recovery**: when a sleeping workflow wakes, the engine
   needs the original workflow template to resume execution. The cache is
   the fast path; the persisted `WorkflowDef` is the fallback.
2. **Resume**: when `Resume` is called, the engine checks the cache before
   attempting to rebuild from the persisted definition.

```go
engine, _ := gostage.New(
    gostage.WithSQLite("app.db"),
    gostage.WithCacheSize(500),
)
```

The default cache size is **1000** entries. When the cache is full and a
new workflow needs to be cached, the least-recently-used entry is evicted.

### Pinned workflows

Workflows that have sleeping runs are **pinned** in the cache and protected
from eviction. If all 1000 cache entries are pinned (because 1000 workflows
are currently sleeping), the cache grows beyond the limit rather than
evicting a sleeping workflow's template. Evicting a pinned workflow would
cause the wake to fail if the `WorkflowDef` also cannot be rebuilt.

A value of 0 means unlimited (no eviction). This is safe for processes that
run a small, fixed number of workflow types but wasteful for systems with
high workflow variety.

Size the cache based on the number of distinct workflow IDs your engine
handles concurrently. If you run 50 different workflow types, a cache size
of 100-200 gives comfortable headroom.

## Worker pool

The worker pool executes asynchronous runs (via `Run`) and timer-based wake
operations for sleeping workflows. It is a bounded pool of goroutines.

```go
engine, _ := gostage.New(
    gostage.WithSQLite("app.db"),
    gostage.WithWorkerPoolSize(16),
)
```

The default size is `2 * runtime.NumCPU()`, with a minimum of 4.

### Backpressure

When all workers are busy, new submissions block. This is intentional
backpressure: it prevents the engine from accepting more work than it can
handle, which would lead to unbounded goroutine growth and memory pressure.

In practice, `Run` will block until a worker becomes available. `RunSync`
does not use the pool (it executes on the calling goroutine) and is not
affected by pool saturation.

Size the pool based on your expected concurrency. If you run 100 async
workflows simultaneously and each takes 5 seconds, you need at least 100
workers to avoid queuing. If workflows are mostly I/O-bound (waiting on
HTTP calls, databases), a larger pool is appropriate. If they are
CPU-bound, a pool larger than `NumCPU` wastes context-switching overhead.

## Timeouts

### Global timeout

`WithTimeout` sets a per-run deadline applied to every `RunSync` call:

```go
engine, _ := gostage.New(
    gostage.WithSQLite("app.db"),
    gostage.WithTimeout(5*time.Minute),
)
```

When the timeout elapses, the run's context is cancelled and the workflow
receives `context.DeadlineExceeded`, resulting in a Cancelled status. This
timeout does **not** apply to `Run` (asynchronous) -- those inherit the
caller's context deadline.

Without this option, `RunSync` runs have no engine-imposed deadline.

### Per-task timeout

`WithTaskTimeout` sets a deadline on individual task invocations:

```go
gostage.Task("external-api", apiHandler,
    gostage.WithTaskTimeout(30*time.Second),
    gostage.WithRetry(3),
)
```

Each retry attempt gets its own timeout. If a task has a 30-second timeout
and 3 retries, the worst-case duration for that step is approximately
90 seconds (plus retry delays), not 30 seconds total.

### Interaction with retries

Task timeouts and retries interact as follows:

1. The engine starts a timer for the task timeout.
2. If the timer fires before the task returns, the task's context is
   cancelled.
3. The cancellation surfaces as a `context.DeadlineExceeded` error.
4. If retries remain, the engine waits for the retry delay and starts a
   fresh attempt with a new timeout.
5. If no retries remain, the step fails.

The global timeout is independent. If the global timeout fires in the
middle of retry attempt 2 of 3, the entire run is cancelled -- remaining
retries are not attempted.

## Deleting runs

`DeleteRun` removes a run and all its associated state from persistence.
For active runs, it has **cancel-and-wait** semantics:

```go
err := engine.DeleteRun(ctx, runID)
```

The sequence:

1. If the run is currently executing, `DeleteRun` cancels it and waits for
   all in-flight persistence writes to finish before deleting. This
   prevents deleting a run while the executor is still writing state.
2. If the provided context is cancelled while waiting, `DeleteRun` proceeds
   with deletion anyway to avoid leaving orphaned data.
3. For sleeping runs, `DeleteRun` cancels the scheduled wake timer and
   unpins the workflow from the cache.
4. The run and its state are deleted from persistence.
5. An `EventRunDeleted` event is emitted.

`DeleteRun` is safe to call for runs in any status. If the run does not
exist in persistence, the behavior depends on the persistence
implementation (the default backends return nil, not an error).

## Operational queries

### GetRun

`GetRun` returns the current persisted state of a run without blocking:

```go
run, err := engine.GetRun(ctx, runID)
if err != nil {
    // ErrRunNotFound or ErrEngineClosed
}
fmt.Println(run.Status, run.WorkflowID, run.UpdatedAt)
```

Unlike `Wait`, `GetRun` returns immediately regardless of whether the run
is still in progress. Use it for status polling, admin dashboards, or
health checks.

### ListRuns

`ListRuns` queries persistence with filter criteria:

```go
// Find all sleeping runs
runs, _ := engine.ListRuns(ctx, gostage.RunFilter{
    Status: gostage.Sleeping,
})

// Find completed runs for a specific workflow, paginated
runs, _ := engine.ListRuns(ctx, gostage.RunFilter{
    WorkflowID: "order-process",
    Status:     gostage.Completed,
    Limit:      20,
    Offset:     40,
})

// Find all runs updated before a cutoff
runs, _ := engine.ListRuns(ctx, gostage.RunFilter{
    Before: time.Now().Add(-24 * time.Hour),
})
```

Available filters:

| Field | Type | Description |
|---|---|---|
| `WorkflowID` | `string` | Filter by workflow ID. Empty = all. |
| `Status` | `Status` | Filter by run status. Empty = all. |
| `Limit` | `int` | Maximum results. 0 = no limit. |
| `Offset` | `int` | Skip first N results. For pagination. |
| `Before` | `time.Time` | Runs with `UpdatedAt` before this time. Zero = no filter. |

An empty filter returns all runs. Returns nil (not an empty slice) when no
runs match.

## Putting it together

A production configuration typically combines several of these options:

```go
engine, err := gostage.New(
    // Durable storage
    gostage.WithSQLite("/var/lib/myapp/workflows.db"),

    // Recover interrupted runs on startup
    gostage.WithAutoRecover(),

    // Clean up old terminal runs every 10 minutes
    gostage.WithRunGC(7*24*time.Hour, 10*time.Minute),

    // Prevent unbounded state growth
    gostage.WithStateLimit(1000),

    // Size the worker pool for expected concurrency
    gostage.WithWorkerPoolSize(32),

    // Global safety net
    gostage.WithTimeout(10*time.Minute),

    // Do not hang forever on shutdown
    gostage.WithShutdownTimeout(30*time.Second),

    // Right-size the workflow cache
    gostage.WithCacheSize(200),

    // Operational visibility
    gostage.WithLogger(myLogger),
)
if err != nil {
    log.Fatalf("engine init: %v", err)
}
defer engine.Close()
```

Adjust the numbers for your workload. The defaults are reasonable for
moderate traffic, but high-volume systems benefit from explicit tuning of
pool size, cache size, GC intervals, and timeouts.
