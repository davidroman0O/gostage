# Workflow Serialization

GoStage workflows are built in Go code using the fluent builder API, but
they can be **serialized to JSON** and rebuilt later. Serialization is the
foundation for durable sleep, suspend/resume, crash recovery, and child
process spawning. Understanding what crosses the serialization boundary --
and what does not -- is essential for building workflows that survive
restarts.

## Named vs. anonymous: why it matters

The builder offers two variants for conditions, map functions, and loop
predicates:

| Step type | Anonymous (closure) | Named (registry reference) |
|---|---|---|
| Branch | `When(func(...) bool { ... })` | `WhenNamed("is-paid")` |
| Map | `Map(func(...) error { ... })` | `MapNamed("normalize")` |
| Do-until | `DoUntil(ref, func(...) bool { ... })` | `DoUntilNamed(ref, "is-ready")` |
| Do-while | `DoWhile(ref, func(...) bool { ... })` | `DoWhileNamed(ref, "has-more")` |

Anonymous closures work fine for in-memory, fire-and-forget workflows.
But they cannot be serialized: a Go function pointer has no portable
representation. If you attempt to serialize a workflow that contains an
anonymous closure, `WorkflowToDefinition` returns an error with a message
telling you which step is the problem and which Named variant to use.

**Rule of thumb**: if the workflow will ever sleep, suspend, resume, or run
in a child process, use the Named variants everywhere.

## Extracting a definition

`WorkflowToDefinition` converts a compiled `*Workflow` into a
`*WorkflowDef`, a plain struct with only JSON-safe fields:

```go
wf, _ := gostage.NewWorkflow("order").
    Step("validate").
    MapNamed("normalize").
    Step("charge").
    Commit()

def, err := gostage.WorkflowToDefinition(wf)
if err != nil {
    // Workflow contains an anonymous closure that cannot be serialized
    log.Fatal(err)
}
```

The returned `WorkflowDef` contains the workflow ID, name, and a slice of
`StepDef` entries -- one per step. Each `StepDef` records the step kind
(single, parallel, branch, forEach, map, doUntil, doWhile, sub, sleep,
stage), the step name, and the names of any referenced tasks, conditions,
or map functions. Sub-workflows and branch cases are nested recursively.

## JSON round-trip

Marshal and unmarshal with the provided helpers:

```go
data, err := gostage.MarshalWorkflowDefinition(def)
// data is []byte of JSON

restored, err := gostage.UnmarshalWorkflowDefinition(data)
// restored is *WorkflowDef
```

These are thin wrappers around `json.Marshal` and `json.Unmarshal`. The
definition types use standard `json` struct tags, so you can also use any
JSON-compatible encoder if you prefer.

## Rebuilding a workflow from a definition

To execute a definition, rebuild it into a live `*Workflow`:

```go
wf, err := gostage.NewWorkflowFromDef(def)
```

This resolves every task name, condition name, and map function name against
the **default registry**. If any name is not registered, the call returns an
error. This is the moment where lazy resolution meets reality: all
referenced functions must exist.

To resolve against a custom registry instead:

```go
wf, err := gostage.NewWorkflowFromDefWithRegistry(def, myRegistry)
```

Sub-workflows are rebuilt recursively. The maximum nesting depth is 64
levels; exceeding this returns an error.

## What IS serialized

The definition captures the **structural skeleton** of the workflow:

- Step kinds (single, parallel, branch, forEach, map, doUntil, doWhile,
  sub, sleep, stage)
- Step names and tags
- Task names (the string, not the function)
- Condition names (for WhenNamed, DoUntilNamed, DoWhileNamed)
- Map function names (for MapNamed)
- Sleep durations (as parseable Go duration strings like `"1h30m"`)
- ForEach configuration (collection key, concurrency, use-spawn flag)
- Sub-workflow definitions (nested recursively)
- Branch case structure (condition name, default flag, target ref)

## What is NOT serialized

The definition intentionally omits everything that is runtime-specific or
cannot be portably represented:

- **Anonymous closures**: `When()`, `Map()`, `DoUntil()`, `DoWhile()` with
  inline functions. These cause `WorkflowToDefinition` to fail.
- **Workflow callbacks**: `OnStepComplete`, `OnError`, middleware chains.
  These are set on the workflow config, not the definition.
- **Runtime state**: the key-value store, mutation queue, dynamic step
  counter. State is persisted separately by the engine.
- **Task function bodies**: only the name is stored. The function must be
  registered in the target registry at rebuild time.
- **Retry configuration**: task-level retry counts, delays, and strategies
  are part of the task definition in the registry, not the workflow
  definition.
- **Engine options**: timeouts, pool size, middleware -- these belong to the
  engine, not the workflow.

## How persistence uses serialization

When the engine persists a workflow run (for sleep, suspend, or crash
recovery), it stores the `WorkflowDef` inside the `RunState` record. This
happens automatically in `executeRun`: the engine calls
`WorkflowToDefinition` and attaches the result to the persisted run. If
serialization fails (anonymous closures), the `WorkflowDef` field is nil,
and the engine relies on the in-memory workflow cache instead.

### Sleep recovery

When a workflow hits a `Sleep` step with durable persistence:

1. The engine serializes the workflow definition into the run record.
2. The run is saved with status `Sleeping` and a `WakeAt` timestamp.
3. The goroutine exits -- no thread blocks during the sleep.
4. The timer scheduler fires at `WakeAt`.
5. The wake handler loads the run from persistence.
6. It rebuilds the workflow from the persisted `WorkflowDef` using
   `NewWorkflowFromDefWithRegistry`.
7. It restores state from persistence, replays mutations, and resumes
   execution from the step after the sleep.

This works across process restarts. When the engine starts with
`WithAutoRecover`, it scans for sleeping runs and re-registers their wake
timers (or wakes them immediately if the time has already passed).

### Suspend and resume

Suspended workflows follow a similar path. The engine persists the
definition and state when the workflow suspends. When `Resume` is called
later (possibly by a different process instance), the engine:

1. Loads the run and checks that its status is `Suspended`.
2. Looks up the workflow in the in-memory cache first.
3. Falls back to rebuilding from the persisted `WorkflowDef`.
4. Restores state, replays mutations, injects resume data, and re-executes.

If neither the cache nor the definition is available (because the workflow
used anonymous closures and the cache was evicted), Resume returns
`ErrWorkflowNotResumable`.

### The cache-first strategy

The engine maintains an LRU cache of workflow templates. For sleeping and
suspended workflows, the cache is the fast path: no deserialization needed.
The persisted `WorkflowDef` is the fallback for when the cache entry has
been evicted or the process has restarted. Sleeping workflows are pinned
in the cache to prevent eviction while they are waiting to wake.

## How spawn uses serialization

When a `ForEach` step runs with `WithSpawn()`, each iteration executes in
an isolated child process. The engine must transfer the workflow definition
to the child. It does this by serializing the `WorkflowDef` to JSON and
sending it over IPC. The child process deserializes the definition,
rebuilds the workflow against its own registry, and executes the relevant
portion.

This means spawn requires all referenced functions to be registered in the
child's registry. Since spawn uses `os/exec` to start child processes, the
child binary must import and register the same tasks. The definition is the
contract between parent and child: both sides agree on the names, and each
side provides its own function implementations.

## Practical guidelines

**Always use Named variants** for workflows that need durability:

```go
// Register once at startup
gostage.Task("validate", validateFn)
gostage.Task("charge", chargeFn)
gostage.Condition("is-paid", isPaidFn)
gostage.MapFn("enrich", enrichFn)

// Build with Named variants
wf, _ := gostage.NewWorkflow("order").
    Step("validate").
    Branch(
        gostage.WhenNamed("is-paid").Step("ship"),
        gostage.Default().Step("charge"),
    ).
    MapNamed("enrich").
    Sleep(time.Hour).
    Step("finalize").
    Commit()
```

**Anonymous closures are fine** for ephemeral workflows that run
synchronously and never suspend or sleep:

```go
wf, _ := gostage.NewWorkflow("quick-check").
    Step("fetch").
    Map(func(ctx *gostage.Ctx) error {
        // inline transform -- no serialization needed
        raw := gostage.Get[string](ctx, "data")
        gostage.Set(ctx, "result", strings.ToUpper(raw))
        return nil
    }).
    Commit()

result, _ := engine.RunSync(ctx, wf, nil)
```

**Test serialization explicitly** if durability matters:

```go
func TestWorkflowIsSerializable(t *testing.T) {
    wf, _ := gostage.NewWorkflow("order").
        Step("validate").
        Step("charge").
        Commit()

    def, err := gostage.WorkflowToDefinition(wf)
    if err != nil {
        t.Fatalf("workflow is not serializable: %v", err)
    }

    data, _ := gostage.MarshalWorkflowDefinition(def)
    restored, _ := gostage.UnmarshalWorkflowDefinition(data)

    rebuilt, err := gostage.NewWorkflowFromDef(restored)
    if err != nil {
        t.Fatalf("cannot rebuild workflow: %v", err)
    }
    _ = rebuilt
}
```
