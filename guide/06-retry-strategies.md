# Retry Strategies

Network calls fail. APIs return 503. Database connections drop. A well-designed
workflow does not fall over on the first transient error -- it retries with a
sensible delay and gives the dependency time to recover.

GoStage provides a layered retry system. Tasks can declare their own retry
count and strategy. Workflows can set defaults for tasks that do not configure
retries. The engine applies the most specific configuration it finds.

## Default behavior: no retries

By default, tasks have **no retries**. If a task returns an error, the workflow
fails immediately. This is the safest default because not all errors are
transient, and blindly retrying non-idempotent operations can make things worse.

Internally, a freshly registered task has `retries = -1`, a sentinel value
meaning "not configured." When the engine resolves the retry count, `-1` is
treated as zero retries unless a workflow-level default overrides it.

```go
gostage.Task("fragile", func(ctx *gostage.Ctx) error {
    return callExternalAPI() // fails once -> workflow fails
})
```

## WithRetry and WithRetryDelay

The simplest retry configuration is a count and a fixed delay:

```go
gostage.Task("flaky-service", handler,
    gostage.WithRetry(3),                       // retry up to 3 times
    gostage.WithRetryDelay(2*time.Second),       // wait 2s between attempts
)
```

`WithRetry(n)` sets the maximum number of **retry** attempts. The task
executes once, then retries up to `n` times if it keeps failing, for a total
of `n+1` attempts.

`WithRetryDelay(d)` sets a fixed delay between attempts. Internally it creates
a `FixedDelay` strategy, so every retry waits exactly `d` before trying again.

If `WithRetry` is set without `WithRetryDelay`, retries happen immediately
with no delay.

## WithRetryStrategy

For more sophisticated backoff, use `WithRetryStrategy` with one of the
built-in strategies or your own implementation:

```go
gostage.Task("api-call", handler,
    gostage.WithRetry(5),
    gostage.WithRetryStrategy(
        gostage.ExponentialBackoffWithJitter(100*time.Millisecond, 30*time.Second),
    ),
)
```

`WithRetryStrategy` overrides `WithRetryDelay` if both are set on the same
task.

### The RetryStrategy interface

A retry strategy determines the delay before each retry attempt:

```go
type RetryStrategy interface {
    Delay(attempt int) time.Duration
}
```

The `attempt` parameter is zero-based: `0` for the delay before the first
retry, `1` for the delay before the second retry, and so on. The initial
execution is not an "attempt" from the strategy's perspective.

## Built-in strategies

### FixedDelay

Waits the same duration between every retry. This is what `WithRetryDelay`
creates under the hood.

```go
gostage.FixedDelay(2 * time.Second)
```

| Attempt | Delay |
|---------|-------|
| 0 | 2s |
| 1 | 2s |
| 2 | 2s |

Use fixed delay when the dependency has a predictable recovery time, or when
simplicity is more important than optimization.

### ExponentialBackoff

Doubles the delay on each attempt, capped at a maximum:

```go
gostage.ExponentialBackoff(100*time.Millisecond, 30*time.Second)
```

The delay formula is `base * 2^attempt`, clamped to `max`:

| Attempt | Delay |
|---------|-------|
| 0 | 100ms |
| 1 | 200ms |
| 2 | 400ms |
| 3 | 800ms |
| 4 | 1.6s |
| 5 | 3.2s |
| ... | ... |
| N | min(base * 2^N, 30s) |

If `max` is `0`, no cap is applied and the delay grows without bound.

Use exponential backoff when the dependency is likely to recover within seconds
but might take longer under heavy load.

### ExponentialBackoffWithJitter

Same exponential growth as `ExponentialBackoff`, but the actual delay is a
random value between `0` and the calculated ceiling:

```go
gostage.ExponentialBackoffWithJitter(100*time.Millisecond, 30*time.Second)
```

| Attempt | Ceiling | Actual delay |
|---------|---------|-------------|
| 0 | 100ms | random [0, 100ms) |
| 1 | 200ms | random [0, 200ms) |
| 2 | 400ms | random [0, 400ms) |

The randomization prevents thundering herds. If 50 workflows all hit the same
API, fail at the same time, and retry with deterministic backoff, they all
retry at the same instant and overwhelm the API again. Jitter spreads the
retries across the full delay window.

**Use jitter whenever multiple workflow instances might retry against the same
external service.** This is the recommended strategy for production workloads
that call rate-limited or capacity-constrained APIs.

## Custom strategies

Implement `RetryStrategy` to create your own. The interface has a single method:

```go
func (s *myStrategy) Delay(attempt int) time.Duration { ... }
```

Pass it to `WithRetryStrategy` like any built-in strategy.

## Workflow-level defaults

Instead of configuring retries on every task, set defaults on the workflow.
Tasks that do not have their own retry configuration inherit the workflow
defaults:

```go
wf, _ := gostage.NewWorkflow("resilient",
    gostage.WithDefaultRetry(3, time.Second),
).
    Step("task-a").  // inherits 3 retries, 1s delay
    Step("task-b").  // inherits 3 retries, 1s delay
    Commit()
```

For a default strategy instead of a fixed delay, add `WithDefaultRetryStrategy`:

```go
gostage.WithDefaultRetry(5, 0),
gostage.WithDefaultRetryStrategy(gostage.ExponentialBackoffWithJitter(100*time.Millisecond, 30*time.Second)),
```

## Precedence

The engine resolves retry configuration with a clear priority order:

1. **Task-level configuration wins.** If `WithRetry` is set on the task,
   that count is used. If `WithRetryStrategy` or `WithRetryDelay` is set on
   the task, that strategy is used.

2. **Workflow defaults fill the gap.** If the task has no retry configuration
   (the default `-1` sentinel), the engine checks the workflow's
   `WithDefaultRetry` count. If that is set, the workflow's default strategy
   or default delay is used.

3. **No retries.** If neither the task nor the workflow configures retries,
   the task runs once. A failure is final.

```go
gostage.Task("explicit", handler, gostage.WithRetry(2))      // 2 retries, always

gostage.Task("inherited", handler)                             // -1 sentinel

wf, _ := gostage.NewWorkflow("mixed",
    gostage.WithDefaultRetry(5, time.Second),                  // workflow default
).
    Step("explicit").     // uses its own 2 retries (task wins)
    Step("inherited").    // uses workflow default: 5 retries, 1s delay
    Commit()
```

## Non-retryable signals

The retry loop knows that certain errors are not transient and should never
be retried. These signals propagate immediately through the retry loop without
counting as an attempt:

- **Bail.** `Bail(ctx, reason)` returns a `BailError`. The engine catches it
  and terminates the workflow with status `bailed`.

- **Suspend.** `Suspend(ctx, data)` returns a `SuspendError`. The engine saves
  state and sets status to `suspended`.

- **Sleep.** Internal `sleepError` signals from persistent sleep steps. The
  engine saves state and schedules a timer wake.

- **Context cancellation.** If the parent context is cancelled or its deadline
  expires, the retry loop exits immediately with the context error.

```go
gostage.Task("guarded", func(ctx *gostage.Ctx) error {
    if shouldBail(ctx) {
        return gostage.Bail(ctx, "business rule violation")
        // NOT retried, even if WithRetry(10) is set
    }
    return callFlaky() // this error IS retried
}, gostage.WithRetry(10))
```

## Per-task timeout

Independent of retries, each task can have its own timeout. The timeout applies
to each individual attempt, not to the total retry duration:

```go
gostage.Task("slow-api", handler,
    gostage.WithRetry(3),
    gostage.WithRetryDelay(time.Second),
    gostage.WithTaskTimeout(30*time.Second),  // each attempt gets 30s
)
```

If attempt 1 runs for 30 seconds and times out, the engine waits 1 second,
then starts attempt 2 with a fresh 30-second timeout. The per-task timeout is
separate from the engine-level timeout (`WithTimeout`), which caps the entire
workflow run.

## Further reading

- [Example 09: Retry with Exponential Backoff](../../examples/09-retry-backoff/)
  -- working example with timing output
- [Suspend and Resume guide](./05-suspend-resume.md) -- for non-transient waits
  that require external input instead of automatic retries
- [Persistence guide](./04-persistence.md) -- how retry state interacts with
  durable storage
