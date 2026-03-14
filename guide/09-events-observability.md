# Events and Observability

GoStage provides two systems for observing workflow execution: **events** for
lifecycle notifications and **IPC messaging** for application-level
communication between tasks and the outside world.


## Middleware vs events

| Concern | Middleware | Events |
|---------|-----------|--------|
| Modify execution flow | Yes | No |
| Measure step/task duration | Yes | No |
| React to run lifecycle changes | No | Yes |
| External notifications (webhooks) | No | Yes |
| Short-circuit or block execution | Yes | No |
| Audit log of state transitions | No | Yes |

Middleware is **active** -- it wraps execution and can change the outcome.
Events are **passive** -- they fire after a transition has happened. You cannot
prevent a run from failing by handling `EventRunFailed`.


## The 9 event types

Events fire at run-level state transitions. Every event carries a timestamp,
run ID, workflow ID, and the resulting status.

| Event | Fires when |
|-------|------------|
| `EventRunCreated` | New run saved to persistence, before execution begins |
| `EventRunCompleted` | Run finishes successfully |
| `EventRunFailed` | Run fails after exhausting retries |
| `EventRunBailed` | Run exits early via `Bail()` |
| `EventRunSuspended` | Run suspends via `Suspend()` |
| `EventRunSleeping` | Run enters a timed sleep |
| `EventRunCancelled` | Run cancelled via `Cancel()` or context |
| `EventRunWoken` | Sleeping run's timer fires, execution resumes |
| `EventRunDeleted` | Run removed from persistence |

Timing: `EventRunCreated` fires before execution. Terminal events (`Completed`,
`Failed`, `Bailed`, `Suspended`, `Sleeping`) fire after execution ends but
before persistence is updated. `EventRunCancelled` fires from `Cancel()` after
updating persistence. `EventRunWoken` fires before the resumed execution begins.


## EngineEvent and EventHandler

```go
type EngineEvent struct {
    Type       EventType
    RunID      RunID
    WorkflowID string
    Status     Status
    Timestamp  time.Time
}

type EventHandler interface {
    OnEvent(event EngineEvent)
}
```

Register handlers during engine construction:

```go
engine, _ := gostage.New(
    gostage.WithEventHandler(&auditLogger{log: logger}),
    gostage.WithEventHandler(&metricsCollector{}),
)
```

Multiple handlers are called in registration order, synchronously, in the
goroutine that triggers the event. Keep handler logic fast. For expensive work,
dispatch to a channel:

```go
func (n *asyncNotifier) OnEvent(event gostage.EngineEvent) {
    select {
    case n.ch <- event:
    default: // drop if full
    }
}
```


## Panic safety

Handler panics are recovered and logged. A panicking handler does not crash the
engine or affect other handlers:

```go
// Inside the engine:
defer func() {
    if r := recover(); r != nil {
        e.logger.Error("event handler panicked for event type %d: %v", event.Type, r)
    }
}()
h.OnEvent(event)
```

You do not need defensive recovery in handlers, but a recovered panic means
your handler's side effects may be incomplete.


## Concurrency

Events fire from multiple goroutines (worker pool, timer scheduler, `Cancel()`,
`DeleteRun()`). Handler implementations must be safe for concurrent use. Use
mutexes, atomics, or channel-based designs.


## IPC messaging

Events cover engine lifecycle. For application-level communication, use IPC.

### Sending messages

From inside a task, call `Send`:

```go
func processOrder(ctx *gostage.Ctx) error {
    gostage.Send(ctx, "progress", gostage.Params{"pct": 75})
    return nil
}
```

Routing depends on context:
- **In a child process**: serialized and sent over gRPC to the parent engine.
- **In the parent process**: routed locally through the engine's handlers.
- **If neither is available**: silently ignored.

The payload is converted to `map[string]any`. Non-map payloads are wrapped as
`{"data": payload}`.

### Receiving messages

Register handlers with `OnMessage`:

```go
id := engine.OnMessage("order.progress", func(msgType string, payload map[string]any) {
    log.Printf("progress: %v%%", payload["pct"])
})
// later:
engine.OffMessage(id)
```

### Wildcard handlers

Register with `"*"` to receive all message types:

```go
engine.OnMessage("*", func(msgType string, payload map[string]any) {
    log.Printf("IPC: type=%s payload=%v", msgType, payload)
})
```

Wildcard handlers fire in addition to type-specific handlers, not instead of.

### Scoped handlers: OnMessageForRun

Listen for messages from a specific run only:

```go
id := engine.OnMessageForRun("progress", runID, func(msgType string, payload map[string]any) {
    updateProgressBar(payload) // only fires for this run
})
defer engine.OffMessage(id)
```

Scoped handlers filter by run ID. Engine-wide handlers still fire for all
messages. This is useful when tracking multiple concurrent runs:

```go
func monitorRun(engine *gostage.Engine, runID gostage.RunID, done chan struct{}) {
    id := engine.OnMessageForRun("status", runID, func(_ string, p map[string]any) {
        fmt.Printf("run %s: %v\n", runID, p)
    })
    <-done
    engine.OffMessage(id)
}
```

### Handler dispatch details

- Called synchronously in the sender's goroutine.
- Panics are recovered and logged.
- Multiple handlers for the same type fire in registration order.
- `OffMessage` with an unknown ID is a no-op.


## Practical patterns

### Metrics collection

```go
type metricsCollector struct{}

func (m *metricsCollector) OnEvent(event gostage.EngineEvent) {
    metrics.IncrCounter("workflow_events_total", "type", eventName(event.Type))
    switch event.Type {
    case gostage.EventRunCompleted:
        metrics.IncrCounter("workflow_completed_total", "workflow", event.WorkflowID)
    case gostage.EventRunFailed:
        metrics.IncrCounter("workflow_failed_total", "workflow", event.WorkflowID)
    }
}
```

### Audit logging

```go
func (a *auditHandler) OnEvent(event gostage.EngineEvent) {
    a.store.Append(AuditEntry{
        Timestamp:  event.Timestamp,
        RunID:      string(event.RunID),
        WorkflowID: event.WorkflowID,
        EventType:  eventName(event.Type),
        Status:     event.Status.String(),
    })
}
```

### Webhook dispatch

```go
func (n *webhookNotifier) OnEvent(event gostage.EngineEvent) {
    switch event.Type {
    case gostage.EventRunCompleted, gostage.EventRunFailed, gostage.EventRunBailed:
        select {
        case n.ch <- event:
        default: // channel full
        }
    }
}

// background drain goroutine POSTs each event as JSON
```

### Progress tracking with IPC

```go
// Task side:
func processItems(ctx *gostage.Ctx) error {
    items := gostage.Get[[]string](ctx, "items")
    for i, item := range items {
        process(item)
        gostage.Send(ctx, "progress", gostage.Params{"current": i + 1, "total": len(items)})
    }
    return nil
}

// Caller side:
id := engine.OnMessageForRun("progress", runID, func(_ string, p map[string]any) {
    fmt.Printf("%v/%v\n", p["current"], p["total"])
})
defer engine.OffMessage(id)
```
