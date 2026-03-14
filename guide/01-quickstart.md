# Getting Started

This guide walks you through your first gostage workflow: registering a task, building a workflow, running it, and reading the result.

## Install

```bash
go get github.com/davidroman0O/gostage
```

## Minimum Viable Workflow

Here is the smallest useful program you can write with gostage:

```go
package main

import (
    "context"
    "fmt"
    "log"

    gs "github.com/davidroman0O/gostage"
)

func main() {
    // 1. Register a task
    gs.Task("greet", func(ctx *gs.Ctx) error {
        name := gs.GetOr[string](ctx, "name", "World")
        return gs.Set(ctx, "greeting", fmt.Sprintf("Hello, %s!", name))
    })

    // 2. Build a workflow
    wf, err := gs.NewWorkflow("hello").Step("greet").Commit()
    if err != nil {
        log.Fatal(err)
    }

    // 3. Create an engine
    engine, err := gs.New()
    if err != nil {
        log.Fatal(err)
    }
    defer engine.Close()

    // 4. Run the workflow
    result, err := engine.RunSync(context.Background(), wf, gs.Params{"name": "Alice"})
    if err != nil {
        log.Fatal(err)
    }

    // 5. Read the result
    greeting, _ := gs.ResultGet[string](result, "greeting")
    fmt.Println(greeting) // Hello, Alice!
}
```

The rest of this guide explains why each of these five steps exists.

## The Four Core Concepts

### Tasks

A **task** is a named function that does one thing. You register it globally with `gostage.Task`:

```go
gs.Task("validate-order", func(ctx *gs.Ctx) error {
    orderID := gs.Get[string](ctx, "order_id")
    if orderID == "" {
        return gs.Bail(ctx, "missing order_id")
    }
    ctx.Log.Info("Order %s is valid", orderID)
    return nil
})
```

Tasks read and write data through the context's key-value store using `Get`, `Set`, `GetOr`, and `GetOk`. They do not receive arguments directly -- everything flows through state. See [State Management](03-state.md) for details.

You can configure tasks with options at registration time:

```go
gs.Task("call-api", handler,
    gs.WithRetry(3),
    gs.WithRetryDelay(2*time.Second),
    gs.WithTaskTimeout(30*time.Second),
)
```

Tasks are resolved by name at execution time, not at build time. You can register tasks in any order relative to building workflows.

### Workflows

A **workflow** defines the sequence and structure of steps to execute. You build one with a fluent API and finalize it with `Commit()`:

```go
wf, err := gs.NewWorkflow("process-order").
    Step("validate-order").
    Step("charge-payment").
    Step("send-confirmation").
    Commit()
```

`Step("name")` references a registered task by name. But workflows support much more than simple sequences. You can fan out with `Parallel`, branch with `Branch`, iterate with `ForEach`, loop with `DoUntil`/`DoWhile`, nest with `Sub`, transform with `Map`, pause with `Sleep`, and group with `Stage`. See [Step Kinds](02-step-kinds.md) for the full catalog.

After `Commit()`, the workflow is immutable. Create it once and reuse it across multiple runs -- the engine clones it internally so concurrent runs never interfere.

### Engine

The **engine** executes workflows. It manages persistence, worker pools, timers, and lifecycle:

```go
engine, err := gs.New()
```

That creates an in-memory engine suitable for development and testing. For durable workflows that survive process restarts, add SQLite:

```go
engine, err := gs.New(gs.WithSQLite("app.db"))
```

The engine provides three ways to run workflows:

- **`RunSync`** -- blocks until the workflow finishes and returns the result directly.
- **`Run`** -- submits the workflow to a worker pool and returns a `RunID` immediately. Call `Wait` to get the result later.
- **`Resume`** -- resumes a previously suspended workflow with new data.

Always close the engine when you are done:

```go
defer engine.Close()
```

`Close` drains in-flight runs, stops timers, and flushes persistence. See `go doc gostage.New` for the full list of engine options.

### Results

Every run produces a `Result` containing the terminal status, any error, and a snapshot of the state store:

```go
result, err := engine.RunSync(ctx, wf, gs.Params{"order_id": "ORD-123"})
if err != nil {
    log.Fatal(err) // engine-level error (closed, nil workflow)
}

switch result.Status {
case gs.Completed:
    total, ok := gs.ResultGet[int](result, "order_total")
    // use total
case gs.Failed:
    fmt.Println("Error:", result.Error)
case gs.Bailed:
    fmt.Println("Bailed:", result.BailReason)
case gs.Suspended:
    fmt.Println("Waiting for:", result.SuspendData)
}
```

Use `ResultGet[T]` to extract typed values from the result store. It applies the same type coercion as `Get[T]` inside tasks, so numeric types round-trip correctly through JSON persistence.

The possible statuses are: `Completed`, `Failed`, `Bailed`, `Cancelled`, `Suspended`, and `Sleeping`.

## The Lifecycle

Every workflow execution follows this sequence:

```
Register tasks  -->  Build workflow  -->  Commit  -->  Run  -->  Read result
     |                    |                 |           |            |
  gostage.Task()    NewWorkflow()      .Commit()    RunSync()   result.Status
                    .Step(...)                      or Run()    ResultGet[T]()
                    .Parallel(...)
```

1. **Register** -- `gostage.Task("name", fn)` adds your functions to the global registry.
2. **Build** -- `NewWorkflow("id").Step(...).Commit()` creates an immutable workflow definition.
3. **Run** -- `engine.RunSync(ctx, wf, params)` clones the workflow, injects params into state, and executes.
4. **Read** -- The returned `Result` has the status and a snapshot of all state keys your tasks wrote.

## What Next

- [Step Kinds](02-step-kinds.md) -- the 10 step types and when to use each one
- [State Management](03-state.md) -- how the key-value store works, type fidelity, and concurrency
- See the [01-hello-world example](../../examples/01-hello-world/main.go) for the runnable version of the code above
