# Step Kinds

Gostage workflows are built from 10 step kinds. Each one controls how the engine executes that step -- sequentially, concurrently, conditionally, in a loop, or as a nested sub-workflow. This guide explains all 10, shows when to use each, and covers the rules for composing them.

## Overview

| Step Kind | Builder Method | Purpose |
|-----------|---------------|---------|
| Single | `.Step("name")` | Execute one registered task |
| Parallel | `.Parallel(refs...)` | Fan out, wait for all |
| Branch | `.Branch(cases...)` | Conditional routing |
| ForEach | `.ForEach(key, ref)` | Iterate over a collection |
| Map | `.Map(fn)` | Inline data transformation |
| DoUntil | `.DoUntil(ref, cond)` | Post-condition loop |
| DoWhile | `.DoWhile(ref, cond)` | Pre-condition loop |
| Sub | `.Sub(wf)` | Nested sub-workflow |
| Sleep | `.Sleep(d)` | Timed delay |
| Stage | `.Stage("name", refs...)` | Named group of sequential steps |

## Single

The most common step. It executes a single registered task by name.

```go
wf, _ := gs.NewWorkflow("pipeline").
    Step("validate").
    Step("process").
    Step("notify").
    Commit()
```

Steps execute in the order you add them. The task does not need to be registered when you build the workflow -- it is resolved at execution time.

You can attach tags to individual steps for runtime queries and mutations:

```go
wf.Step("charge", gs.WithStepTags("billing", "critical"))
```

**When to use:** Any time you need to run a single task. This is your default building block.

## Parallel

Runs multiple steps concurrently in separate goroutines. The workflow waits for all of them to finish before proceeding. If any step fails, the entire Parallel step fails.

```go
wf, _ := gs.NewWorkflow("extract").
    Parallel(
        gs.Step("extract-users"),
        gs.Step("extract-orders"),
        gs.Step("extract-products"),
    ).
    Step("merge-results").
    Commit()
```

Since parallel steps share the same state store, each step must write to unique keys. If `extract-users` and `extract-orders` both write to `"count"`, you have a data race. Write to `"user_count"` and `"order_count"` instead.

You can nest `Sub` references inside Parallel to run sub-workflows concurrently:

```go
wf.Parallel(gs.Step("fast-task"), gs.Sub(heavyWorkflow))
```

**When to use:** When you have independent work that can run at the same time -- fetching from multiple APIs, running independent validations, or processing unrelated data.

## Branch

Conditional routing. Cases are evaluated in order; the first match executes and the rest are skipped. You can add a `Default` case that runs when nothing else matches.

```go
wf, _ := gs.NewWorkflow("route").
    Branch(
        gs.When(func(ctx *gs.Ctx) bool {
            return gs.Get[string](ctx, "priority") == "high"
        }).Step("urgent-process"),
        gs.When(func(ctx *gs.Ctx) bool {
            return gs.Get[string](ctx, "priority") == "medium"
        }).Step("normal-process"),
        gs.Default().Step("low-priority-process"),
    ).
    Commit()
```

Each case can route to a task (`.Step("name")`) or a sub-workflow (`.Sub(wf)`):

```go
gs.When(isLargeOrder).Sub(largeOrderWorkflow)
```

If no condition matches and no `Default` is provided, the Branch step is a no-op -- the workflow continues to the next step.

**When to use:** When the next action depends on runtime data. Order routing, feature flags, error classification.

### Named conditions for persistence

Anonymous closures (as above) work fine for in-memory workflows. But if your workflow uses `Sleep` or `Suspend` and needs to survive a process restart, you must use named conditions so the workflow definition is serializable:

```go
gs.Condition("is-high-priority", func(ctx *gs.Ctx) bool {
    return gs.Get[string](ctx, "priority") == "high"
})

wf.Branch(gs.WhenNamed("is-high-priority").Step("urgent"))
```

This applies to `Branch`, `DoUntil`, and `DoWhile`. See the [Named vs Anonymous](#named-vs-anonymous-variants) section below.

## ForEach

Iterates over a collection stored in state. The collection must be a slice. For each element, the referenced task (or sub-workflow) executes with the current item accessible via `Item[T]` and the index via `ItemIndex`.

```go
gs.Task("download", func(ctx *gs.Ctx) error {
    url := gs.Item[string](ctx)
    index := gs.ItemIndex(ctx)
    // download url...
    return gs.Set(ctx, fmt.Sprintf("result_%d", index), data)
})

wf, _ := gs.NewWorkflow("batch").
    ForEach("urls", gs.Step("download"), gs.WithConcurrency(4)).
    Step("summarize").
    Commit()
```

By default, iterations run sequentially (concurrency 1). `WithConcurrency(n)` allows up to `n` iterations to run simultaneously. Since concurrent iterations share the same state store, write to unique keys (e.g., using the index as a suffix).

You can iterate over a sub-workflow instead of a single task:

```go
wf.ForEach("items", gs.Sub(itemProcessingWorkflow))
```

`WithSpawn()` runs each iteration in an isolated child process instead of a goroutine, giving you process-level isolation at the cost of IPC overhead:

```go
wf.ForEach("items", gs.Step("process"), gs.WithSpawn())
```

**When to use:** Processing a list of items -- downloading files, sending emails, transforming records.

## Map

An inline data transformation step. Unlike task steps, Map functions are not registered by name -- they run directly in-process.

```go
wf, _ := gs.NewWorkflow("transform").
    Step("fetch-raw-data").
    Map(func(ctx *gs.Ctx) error {
        raw := gs.Get[[]byte](ctx, "raw")
        gs.Set(ctx, "records", parseCSV(raw))
        return nil
    }).
    Step("process-records").
    Commit()
```

Map is ideal for lightweight data reshaping between steps -- parsing, filtering, formatting. It keeps the logic close to where it is used, avoiding the overhead of registering a named task for trivial transformations.

For serializable workflows, use `MapNamed` with a registered function:

```go
gs.MapFn("parse-csv", func(ctx *gs.Ctx) error {
    raw := gs.Get[[]byte](ctx, "raw")
    return gs.Set(ctx, "records", parseCSV(raw))
})

wf.MapNamed("parse-csv")
```

**When to use:** Glue logic between steps. Data format conversion, filtering, enrichment. If the logic is substantial, register it as a task instead.

## DoUntil

A post-condition loop: the body executes first, then the condition is checked. This guarantees at least one execution. The loop repeats until the condition returns `true`.

```go
wf, _ := gs.NewWorkflow("poll").
    DoUntil(gs.Step("check-status"), func(ctx *gs.Ctx) bool {
        return gs.Get[string](ctx, "status") == "ready"
    }).
    Step("proceed").
    Commit()
```

The body can be a task reference or a sub-workflow. For serializable workflows, use `DoUntilNamed`:

```go
gs.Condition("is-ready", func(ctx *gs.Ctx) bool {
    return gs.Get[string](ctx, "status") == "ready"
})

wf.DoUntilNamed(gs.Step("check-status"), "is-ready")
```

**When to use:** Polling for a condition, retrying until success, processing until a queue is empty.

## DoWhile

A pre-condition loop: the condition is checked before each iteration. If the condition is `false` on the first check, the body never executes.

```go
wf, _ := gs.NewWorkflow("paginate").
    Step("fetch-first-page").
    DoWhile(gs.Step("fetch-next-page"), func(ctx *gs.Ctx) bool {
        return gs.Get[bool](ctx, "has_more")
    }).
    Commit()
```

For serializable workflows, use `DoWhileNamed`:

```go
gs.Condition("has-more-pages", func(ctx *gs.Ctx) bool {
    return gs.Get[bool](ctx, "has_more")
})

wf.DoWhileNamed(gs.Step("fetch-next-page"), "has-more-pages")
```

**When to use:** Pagination, conditional repetition where you might not need any iterations.

## Sub

Embeds a committed sub-workflow inside the parent workflow. The sub-workflow's steps execute sequentially within the parent, sharing the same state store.

```go
paymentWf, _ := gs.NewWorkflow("payment").
    Step("validate-card").
    Step("charge").
    Step("record-transaction").
    Commit()

orderWf, _ := gs.NewWorkflow("order").
    Step("validate-order").
    Sub(paymentWf).
    Step("send-receipt").
    Commit()
```

Sub-workflows must be committed before being used. They are a composition primitive -- you build reusable workflow fragments and plug them into larger workflows.

You can use `gs.Sub(wf)` as a `StepRef` inside `Parallel`, `ForEach`, `Branch`, `DoUntil`, `DoWhile`, and `Stage`:

```go
wf.Parallel(gs.Sub(workflowA), gs.Sub(workflowB))
wf.ForEach("orders", gs.Sub(orderProcessingWf))
gs.When(isSpecialCase).Sub(specialCaseWf)
```

**When to use:** Reusing workflow fragments across multiple parent workflows, organizing complex workflows into logical units.

## Sleep

Pauses the workflow for a duration. Behavior depends on the engine's persistence:

- **With persistence (SQLite):** The run is saved with `Sleeping` status and no goroutine blocks. The engine's timer scheduler wakes the workflow when the duration elapses, even across process restarts.
- **Without persistence (in-memory):** `time.Sleep` is called directly, blocking the goroutine.

```go
wf, _ := gs.NewWorkflow("delayed").
    Step("send-notification").
    Sleep(24 * time.Hour).
    Step("send-followup").
    Commit()
```

**When to use:** Rate limiting, scheduled follow-ups, cool-down periods. With persistence, sleep survives restarts -- ideal for long delays (hours, days).

## Stage

A named group of sequential steps. Stages provide organizational structure without adding concurrency -- the contained steps execute in order, one after another.

```go
wf, _ := gs.NewWorkflow("pipeline").
    Stage("validation",
        gs.Step("validate-input"),
        gs.Step("validate-rules"),
        gs.Step("validate-permissions"),
    ).
    Stage("processing",
        gs.Step("transform"),
        gs.Step("enrich"),
        gs.Step("store"),
    ).
    Commit()
```

**When to use:** Grouping related steps for readability and organization. Stages make large workflows easier to understand at a glance.

## Named vs Anonymous Variants

Four step kinds accept closures: `Branch` (via `When`), `Map`, `DoUntil`, and `DoWhile`. Each has a named variant:

| Anonymous | Named | Registry |
|-----------|-------|----------|
| `When(fn)` | `WhenNamed("name")` | `Condition("name", fn)` |
| `Map(fn)` | `MapNamed("name")` | `MapFn("name", fn)` |
| `DoUntil(ref, fn)` | `DoUntilNamed(ref, "name")` | `Condition("name", fn)` |
| `DoWhile(ref, fn)` | `DoWhileNamed(ref, "name")` | `Condition("name", fn)` |

**Use anonymous variants** when:
- You are building in-memory-only workflows (no persistence)
- The logic is trivial and does not benefit from a name
- You are prototyping

**Use named variants** when:
- Your workflow uses `Sleep` or `Suspend` (it must survive process restarts)
- You want the workflow definition to be serializable
- You want to reuse conditions across multiple workflows

If a workflow with anonymous closures tries to suspend or sleep with persistence, the engine returns `ErrWorkflowNotResumable`.

## Composition Rules

Steps compose freely with two mechanisms: the builder chain and `StepRef`.

The **builder chain** is the top-level sequence:

```go
wf.Step("a").Parallel(...).Branch(...).Step("b")
```

Each builder method appends a step to the sequence. Steps execute left to right.

A **`StepRef`** is a reference to either a task or a sub-workflow, used inside composite steps:

```go
gs.Step("task-name")   // references a registered task
gs.Sub(committed_wf)   // references a committed sub-workflow
```

Here is what can contain what:

| Container | Accepts StepRef? | Notes |
|-----------|-----------------|-------|
| Parallel | Yes, variadic | All refs execute concurrently |
| ForEach | Yes, single ref | Ref executes per item |
| Branch (When/Default) | Yes, via `.Step()` or `.Sub()` | One ref per case |
| DoUntil | Yes, single ref | Ref is the loop body |
| DoWhile | Yes, single ref | Ref is the loop body |
| Stage | Yes, variadic | Refs execute sequentially |
| Sub (builder method) | No, takes `*Workflow` | Embeds the sub-workflow directly |

This means you can nest arbitrarily:

```go
wf, _ := gs.NewWorkflow("complex").
    Stage("init",
        gs.Step("setup"),
        gs.Sub(configWorkflow),
    ).
    ForEach("batches", gs.Sub(batchWorkflow), gs.WithConcurrency(4)).
    Branch(
        gs.When(hasErrors).Sub(errorHandlingWorkflow),
        gs.Default().Step("finalize"),
    ).
    Commit()
```

## Step Capabilities Summary

| Step Kind | Serializable? | Concurrent? | Accepts Sub-Workflows? |
|-----------|--------------|-------------|----------------------|
| Single | Yes | No | No |
| Parallel | Yes | Yes (all refs) | Yes (via `gs.Sub`) |
| Branch | With Named | No | Yes (via `.Sub()`) |
| ForEach | Yes | Optional (`WithConcurrency`) | Yes (via `gs.Sub`) |
| Map | With Named | No | No |
| DoUntil | With Named | No | Yes (via `gs.Sub`) |
| DoWhile | With Named | No | Yes (via `gs.Sub`) |
| Sub | Yes | No | It *is* a sub-workflow |
| Sleep | Yes | No | No |
| Stage | Yes | No | Yes (via `gs.Sub`) |

"With Named" means the step is serializable only when using the named variant (`WhenNamed`, `MapNamed`, `DoUntilNamed`, `DoWhileNamed`). Anonymous closure variants are not serializable.

## Decision Guide

Choosing the right step kind:

- **"Run one thing"** -- `Step`
- **"Run things at the same time"** -- `Parallel`
- **"Pick one path based on data"** -- `Branch`
- **"Do this for every item in a list"** -- `ForEach`
- **"Transform data between steps"** -- `Map`
- **"Keep going until a condition is met"** -- `DoUntil`
- **"Keep going while a condition holds"** -- `DoWhile`
- **"Reuse a workflow inside another"** -- `Sub`
- **"Wait for a period of time"** -- `Sleep`
- **"Group steps for readability"** -- `Stage`

## What Next

- [State Management](03-state.md) -- how tasks read and write data through the key-value store
- [Getting Started](01-quickstart.md) -- if you have not set up your first workflow yet
