# State Management

Every workflow run has its own isolated key-value store. Tasks communicate exclusively through this store -- there are no function arguments or return values. This guide covers how to read and write state, what types are supported, and how to handle concurrency.

## The Key-Value Model

When you call `engine.RunSync(ctx, wf, params)`, the engine creates a fresh state store for that run and seeds it with `params`. Every task in the workflow reads from and writes to this same store:

```go
// The caller seeds state with params
result, _ := engine.RunSync(ctx, wf, gs.Params{
    "order_id": "ORD-123",
    "amount":   99,
})

// Inside a task, you read and write state
gs.Task("process", func(ctx *gs.Ctx) error {
    orderID := gs.Get[string](ctx, "order_id")   // "ORD-123"
    amount := gs.Get[int](ctx, "amount")          // 99
    return gs.Set(ctx, "total", amount * 100)      // write new key
})

// After the workflow finishes, read from the result
total, ok := gs.ResultGet[int](result, "total")   // 9900
```

State is scoped to a single run. Two concurrent runs of the same workflow have completely independent stores.

## Reading State

There are four functions for reading state, all generic:

### Get

Returns the value or the zero value of `T` if the key is missing:

```go
name := gs.Get[string](ctx, "name")     // "" if missing
count := gs.Get[int](ctx, "count")      // 0 if missing
items := gs.Get[[]string](ctx, "items") // nil if missing
```

You cannot distinguish "key not found" from "key is set to the zero value" with `Get`. Use `GetOk` when that matters.

### GetOr

Returns the value or a caller-provided default if the key is missing:

```go
name := gs.GetOr[string](ctx, "name", "World")
pageSize := gs.GetOr[int](ctx, "page_size", 25)
retries := gs.GetOr[int](ctx, "max_retries", 3)
```

This is the most common read function. Prefer it over `Get` when you have a sensible default.

### GetOk

Returns the value and a boolean indicating whether the key exists and was successfully coerced to `T`:

```go
val, ok := gs.GetOk[string](ctx, "optional_field")
if !ok {
    // key does not exist or stored value is not a string
}
```

Use `GetOk` when you need to distinguish "missing" from "present but zero" -- for example, when `false` is a meaningful value rather than just the zero value of `bool`.

### ResultGet

After a workflow completes, read from the result store:

```go
result, _ := engine.RunSync(ctx, wf, params)
total, ok := gs.ResultGet[int](result, "order_total")
```

`ResultGet` applies the same type coercion rules as `Get`. See `go doc gostage.ResultGet` for full details.

## Writing State

### Set

Stores a value under a key:

```go
gs.Set(ctx, "user_name", "Alice")
gs.Set(ctx, "item_count", 42)
gs.Set(ctx, "results", []string{"a", "b", "c"})
```

`Set` returns an error if the value is not JSON-serializable or if the state limit is exceeded:

```go
if err := gs.Set(ctx, "key", value); err != nil {
    return fmt.Errorf("failed to store key: %w", err)
}
```

In practice, most callers return the error directly:

```go
return gs.Set(ctx, "greeting", fmt.Sprintf("Hello, %s!", name))
```

### Delete

Removes a key from the store. The deletion is durable -- it is recorded as a tombstone and flushed to the persistence layer. After a crash and resume, the key does not reappear:

```go
gs.Delete(ctx, "temp_data")
```

Deleting a key that does not exist is a no-op.

## Type Fidelity

JSON is the wire format for persistence and child process communication. JSON decodes all numbers as `float64`, which creates a problem: if you store an `int`, persist it, and read it back, you get `float64`.

Gostage solves this automatically. Every stored value carries its Go type name alongside the JSON payload. When you read a value, the coercion layer:

1. Tries a direct type assertion (fast path, works for in-memory values).
2. Falls back to numeric conversion if `T` is a numeric type (handles `float64` to `int`, `uint`, etc.).
3. Falls back to JSON re-marshal for composite types (handles `map[string]interface{}` to your struct type).

This means you can write `int`, persist it, resume the workflow, and `Get[int]` still works:

```go
gs.Set(ctx, "count", 42)
// ... persist, crash, resume ...
count := gs.Get[int](ctx, "count") // 42, not float64(42)
```

The coercion layer also guards against lossy conversions. Converting `float64(3.14)` to `int` fails (returns the zero value) because truncating the fractional part would lose information.

Struct types round-trip through persistence correctly as long as they are JSON-serializable:

```go
type Order struct {
    ID    string `json:"id"`
    Total int    `json:"total"`
}

gs.Set(ctx, "order", Order{ID: "ORD-1", Total: 100})
// ... after persistence round-trip ...
order := gs.Get[Order](ctx, "order") // Order{ID: "ORD-1", Total: 100}
```

## Types That Are Not Allowed

`Set` rejects values that cannot survive a JSON round-trip. The following types return an error:

- **Channels** (`chan T`)
- **Functions** (`func(...)`)
- **Complex numbers** (`complex64`, `complex128`)
- **Unsafe pointers** (`unsafe.Pointer`)
- **Maps with non-string keys** (`map[int]string` fails; `map[string]int` works)

The check walks the entire type tree, so a struct containing a `chan` field is also rejected.

Unexported struct fields and fields tagged `json:"-"` are ignored during the check (and during marshaling).

## Dirty Tracking

State writes are buffered in memory during step execution. Only keys that were actually written (marked "dirty") are flushed to persistence at step boundaries. This means:

- If a step reads 50 keys but writes 2, only those 2 are persisted.
- Updating the same key multiple times within a step results in a single persistence write.
- Reading a key never triggers a persistence write.

This design keeps persistence I/O proportional to the amount of data your tasks actually change, not the total state size. You do not need to think about this in normal usage -- it is an implementation detail that affects performance, not correctness.

## State Limits

You can cap the number of entries per run to prevent runaway workflows from consuming unbounded memory or storage:

```go
engine, _ := gs.New(gs.WithStateLimit(1000))
```

When the limit is reached, `Set` returns `ErrStateLimitExceeded` for new keys. Updates to existing keys always succeed regardless of the limit. This protects against bugs like a ForEach loop that creates a new key per iteration without bound.

Without `WithStateLimit`, there is no cap (the default is unlimited).

## Concurrent Access

The state store is protected by a read-write mutex, so individual `Get` and `Set` calls are safe from multiple goroutines. However, shared state in `Parallel` and `ForEach` steps requires discipline.

### The Rule

**Write to unique keys.** If two goroutines write to the same key, you have a logical race -- the last writer wins, and the result is nondeterministic.

In `ForEach` with concurrency, use the item index to create unique keys:

```go
gs.Task("process-item", func(ctx *gs.Ctx) error {
    item := gs.Item[string](ctx)
    index := gs.ItemIndex(ctx)
    result := transform(item)
    return gs.Set(ctx, fmt.Sprintf("result_%d", index), result)
})
```

In `Parallel`, use distinct key names for each parallel step:

```go
// Good: each step writes to its own key
gs.Task("fetch-users", func(ctx *gs.Ctx) error {
    return gs.Set(ctx, "user_count", 150)
})
gs.Task("fetch-orders", func(ctx *gs.Ctx) error {
    return gs.Set(ctx, "order_count", 340)
})
```

### Values Are Stored by Reference

`Set` stores values by reference, not by deep copy. If you store a slice, then modify the slice outside the store, the stored value changes too. In concurrent steps, this can cause data races even if you write to different keys.

The safe approach: do not mutate a value after storing it. If you need to build a value incrementally, build it fully, then store:

```go
// Good
results := make([]string, 0)
for _, item := range items {
    results = append(results, process(item))
}
gs.Set(ctx, "results", results)

// Risky in concurrent code
gs.Set(ctx, "results", mySlice)
mySlice = append(mySlice, "another") // mutates stored value
```

## Internal Keys

The engine reserves keys prefixed with `__` (double underscore) for internal bookkeeping:

- `__resuming` -- set to `true` during a resumed execution
- `__resume:*` -- resume data passed to `Engine.Resume`
- `__foreach_item` / `__foreach_index` -- current ForEach iteration state

These keys are stripped from `Result.Store` before the result is returned to you. Do not write to keys starting with `__` -- they may be overwritten or deleted by the engine.

## What Next

- [Getting Started](01-quickstart.md) -- if you have not run your first workflow yet
- [Step Kinds](02-step-kinds.md) -- the 10 step types and how they interact with state
