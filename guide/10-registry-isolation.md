# Registry and Engine Isolation

GoStage uses a **registry** to map names to executable functions at runtime.
Every task, condition, and map function is registered by name; workflow
definitions only store the names, not the code. The engine resolves those
names when a step actually runs, not when the workflow is built. This
separation is what makes workflows serializable, resumable, and portable
across processes.

## The default registry

The quickest way to register functions is with the package-level helpers.
They write into a single, process-wide default registry:

```go
gostage.Task("charge", chargeHandler, gostage.WithRetry(3))
gostage.Condition("is-paid", isPaidFn)
gostage.MapFn("normalize", normalizeFn)
```

Every engine that you create without an explicit `WithRegistry` option shares
this default registry. For small services running a single workflow domain,
this is all you need.

## Custom registries

When a single process hosts workflows from different domains -- or when you
need test isolation -- create a dedicated registry:

```go
reg := gostage.NewRegistry()
reg.RegisterTask("ingest", ingestHandler)
reg.RegisterCondition("has-data", hasDataFn)
reg.RegisterMapFn("transform", transformFn)
```

`RegisterTask`, `RegisterCondition`, and `RegisterMapFn` mirror the
package-level `Task()`, `Condition()`, and `MapFn()` functions, but write
into the specific registry instance instead of the global one.

Duplicate names panic immediately. This is intentional: a name collision is
a programming error, and surfacing it at startup is safer than returning a
silent no-op at runtime.

## Binding a registry to an engine

Pass `WithRegistry` when creating the engine to scope it to your custom
registry:

```go
engine, err := gostage.New(
    gostage.WithSQLite("app.db"),
    gostage.WithRegistry(reg),
)
```

From this point, every workflow executed by this engine resolves task names,
condition names, and map function names against `reg` -- not the default
registry. Two engines in the same process can use completely different
registries, running different sets of tasks with no interference.

## Lazy resolution

A critical detail: **the builder does not resolve names at build time.** When
you call `wf.Step("charge")`, the builder stores the string `"charge"` and
moves on. It does not check whether a task named `"charge"` exists in any
registry. Resolution happens when the engine executes the step.

This means you can build workflows before tasks are registered:

```go
// Build the workflow first
wf, _ := gostage.NewWorkflow("order").
    Step("validate").
    Step("charge").
    Commit()

// Register tasks later (perhaps loaded from a plugin)
gostage.Task("validate", validateHandler)
gostage.Task("charge", chargeHandler)

// Run -- tasks are resolved now
result, _ := engine.RunSync(ctx, wf, nil)
```

If a name cannot be resolved at execution time, the engine returns an error
for that step. The workflow does not silently skip it.

This lazy approach supports dynamic loading patterns where tasks are
discovered or compiled at startup from configuration, plugin directories,
or remote registries.

## Use cases

### Multi-tenant isolation

A platform that runs workflows on behalf of different tenants can give each
tenant its own registry. Tenant A's `"process"` task can be a completely
different function from tenant B's:

```go
tenantA := gostage.NewRegistry()
tenantA.RegisterTask("process", tenantAHandler)

tenantB := gostage.NewRegistry()
tenantB.RegisterTask("process", tenantBHandler)

engineA, _ := gostage.New(gostage.WithRegistry(tenantA))
engineB, _ := gostage.New(gostage.WithRegistry(tenantB))
```

Both engines can run a workflow with a step named `"process"`, but each
resolves it to a different function.

### Test isolation

Tests that register tasks into the default registry can leak state between
cases. Use `ResetTaskRegistry()` in your test setup to clear the default
registry, including its serializability cache:

```go
func TestOrderWorkflow(t *testing.T) {
    gostage.ResetTaskRegistry()

    gostage.Task("charge", mockChargeHandler)
    gostage.Condition("is-paid", func(ctx *gostage.Ctx) bool {
        return true
    })

    // ... build, run, assert
}
```

Alternatively, create a fresh registry per test and pass it via
`WithRegistry`. This avoids touching global state entirely:

```go
func TestChargeTask(t *testing.T) {
    reg := gostage.NewRegistry()
    reg.RegisterTask("charge", mockChargeHandler)

    engine, _ := gostage.New(gostage.WithRegistry(reg))
    defer engine.Close()

    // ... run workflow against this isolated engine
}
```

### Dynamic task loading

In systems where task implementations are discovered at runtime -- loaded
from a plugin directory, fetched from a remote service, or generated from
configuration -- you can populate the registry after the process starts
and before the engine begins executing workflows:

```go
reg := gostage.NewRegistry()

for _, plugin := range discoverPlugins("/etc/app/plugins") {
    reg.RegisterTask(plugin.Name, plugin.Handler)
}

engine, _ := gostage.New(gostage.WithRegistry(reg))
```

Because workflow definitions reference tasks by name, the same workflow
JSON can be deployed across environments where different plugins provide
the actual implementations.

## Tags and discovery

Tasks support tags for organizational queries:

```go
gostage.Task("send.email", emailHandler, gostage.WithTags("notification", "async"))
gostage.Task("send.sms", smsHandler, gostage.WithTags("notification", "async"))
gostage.Task("charge", chargeHandler, gostage.WithTags("billing"))
```

Use `ListTasksByTag` to discover tasks by tag:

```go
names := gostage.ListTasksByTag("notification")
// ["send.email", "send.sms"]
```

This is useful for building admin interfaces, generating documentation, or
dynamically constructing workflows from tagged task groups. Tags are also
available at the step level via `WithStepTags` for runtime mutations like
`DisableByTag` and `EnableByTag`.

## Summary

| Concept | Function | Scope |
|---|---|---|
| Default registry | `Task()`, `Condition()`, `MapFn()` | Process-wide |
| Custom registry | `NewRegistry()`, `RegisterTask()`, etc. | Instance |
| Engine binding | `WithRegistry(reg)` | Per engine |
| Test reset | `ResetTaskRegistry()` | Default registry |
| Tag queries | `ListTasksByTag(tag)` | Default registry |
| Resolution timing | At step execution | Lazy |
