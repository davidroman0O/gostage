# Persistence

GoStage workflows run in memory by default. Every value a task writes to the
store, every step completion marker, and the run record itself live in a Go map
that vanishes when the process exits. For short-lived scripts and tests that is
fine. For anything that matters -- approval workflows, multi-hour ETL jobs,
sleep-and-retry patterns -- you need durable persistence so the engine can
recover state after a crash.

This guide explains what gets persisted, how the flush cycle works, and how to
plug in your own storage backend.

## Why persistence matters

Three engine capabilities depend on durable storage:

1. **Crash recovery.** If the process dies mid-workflow, the engine can detect
   interrupted runs on restart and either mark them failed or resume them.
   Without persistence the run is simply gone.

2. **Suspend and resume.** A workflow that calls `Suspend` saves its state and
   exits. Hours or days later, `Resume` loads that state and picks up where it
   left off. The state must survive in between.

3. **Sleep and wake.** Persistent sleep steps save the workflow, release the
   goroutine, and schedule a timer. When the timer fires -- even after a restart
   -- the engine reloads the workflow and continues. Without persistence the
   goroutine blocks on `time.Sleep`, and a restart loses the run.

## SQLite setup

The built-in durable backend is SQLite, accessed through a pure-Go driver
(`modernc.org/sqlite`). No CGo, no C compiler, no shared libraries. Cross-compile
to any `GOOS`/`GOARCH` target and it works.

```go
engine, err := gostage.New(gostage.WithSQLite("app.db"))
if err != nil {
    log.Fatal(err)
}
defer engine.Close()
```

`WithSQLite` accepts a file path. The database is created if it does not exist.
Schema migrations run automatically the first time the engine opens the file.

WAL (Write-Ahead Logging) mode is enabled on every connection. WAL lets readers
and writers proceed concurrently, which matters when one goroutine is flushing
state while another is listing runs. The engine enforces a single connection
(`SetMaxOpenConns(1)`) to avoid SQLite's file-level write lock causing
`SQLITE_BUSY` errors.

For tests you can pass `":memory:"` to get an in-memory SQLite database. It
behaves like the real thing (migrations, queries, transactions) but disappears
when the engine closes:

```go
engine, err := gostage.New(gostage.WithSQLite(":memory:"))
```

## What gets persisted

### Run records

Every call to `RunSync`, `Run`, or `Resume` creates a row in the `runs` table.
The record tracks the run ID, workflow ID, current status, the currently
executing step, bail reason, suspend data, wake time, dynamic mutations, and
the serialized workflow definition. The engine updates this record at key
lifecycle points: when a run starts, when it suspends or sleeps, when it
completes or fails, and during crash recovery.

### Step statuses

Each step completion is recorded as a separate row in the `step_statuses`
table, keyed by `(run_id, step_id)`. This is how resume works: when a workflow
re-executes after a crash, suspend, or sleep, the executor loads the step
statuses and skips any step already marked `Completed`.

Earlier schema versions stored step statuses as a JSON blob on the run record.
The current schema (v3) uses a dedicated table so that marking a step complete
is a single `INSERT ... ON CONFLICT` upsert instead of a read-modify-write
JSON cycle.

### State entries

The workflow's key-value store is persisted per-key in the `run_state` table.
Each row holds the run ID, a key string, the JSON-encoded value, and the Go
type name (e.g. `"int"`, `"string"`, `"MyStruct"`). The type name is stored so
that numeric types survive the JSON round trip -- JSON decodes all numbers as
`float64`, and the engine uses the stored type name to convert back to the
original Go type on load.

State entries are persisted at step boundaries, not on every `Set` call. This
is the flush model described next.

## Flush semantics

Writes to the store during a task are buffered in memory. Each entry carries a
`dirty` flag. When a step completes, the executor flushes all dirty entries to
persistence in a single transaction, then marks the step as `Completed`:

```
task runs -> writes to store (in-memory, dirty) -> step finishes
          -> Flush: write dirty entries to run_state
          -> mark step Completed in step_statuses
```

The order matters. State is flushed **before** the step is marked complete. If
the process crashes between the flush and the completion marker, the step
re-executes on resume but the state written by the previous attempt is already
persisted. If the process crashes before the flush, the step re-executes and
the incomplete state writes are discarded -- the task runs from a clean slate.

Deleted keys are handled the same way. Calling `Delete` records a tombstone in
memory. On the next flush, the engine issues `DELETE` statements for each
tombstone, then clears the tombstone set.

A final flush runs after the workflow finishes (or suspends, sleeps, bails, or
fails) to catch any state changes from the last step. This final flush uses a
background context so it succeeds even if the workflow's context was cancelled.

### Terminal cleanup

When a run reaches a terminal status (`Completed`, `Failed`, `Bailed`, or
`Cancelled`), the engine deletes all state entries from `run_state` for that
run. The final state snapshot is already captured in `Result.Store`, so the
persisted entries are no longer needed. This keeps the database from growing
without bound as runs accumulate.

## In-memory mode

When no persistence option is configured, the engine creates a
`memoryPersistence` backend. It implements the same `Persistence` interface
with Go maps protected by a `sync.RWMutex`. Functionally identical during a
single process lifetime, but state does not survive restarts.

The engine uses a marker interface, `InMemoryPersistence`, to detect this
backend. The marker changes one behavior: **sleep steps block the goroutine**
instead of returning a `sleepError` signal. Since there is no durable store to
save the workflow, the engine cannot release the goroutine and schedule a timer
wake. It falls back to `time.Sleep` so the workflow still works -- it just ties
up a goroutine for the duration.

```go
// In-memory: blocks the goroutine
engine, err := gostage.New() // no WithSQLite
// ...
wf.Sleep(10 * time.Second)  // goroutine sleeps for 10s

// SQLite: saves state, releases goroutine, wakes via timer
engine, err := gostage.New(gostage.WithSQLite("app.db"))
// ...
wf.Sleep(10 * time.Second)  // goroutine returns immediately
```

## Custom persistence

To integrate with PostgreSQL, DynamoDB, Redis, or any other backend, implement
the `Persistence` interface:

```go
type Persistence interface {
    SaveRun(ctx context.Context, run *RunState) error
    LoadRun(ctx context.Context, runID RunID) (*RunState, error)
    UpdateStepStatus(ctx context.Context, runID RunID, stepID string, status Status) error
    SaveState(ctx context.Context, runID RunID, entries map[string]StateEntry) error
    LoadState(ctx context.Context, runID RunID) (map[string]StateEntry, error)
    DeleteState(ctx context.Context, runID RunID) error
    DeleteStateKey(ctx context.Context, runID RunID, key string) error
    DeleteRun(ctx context.Context, runID RunID) error
    UpdateCurrentStep(ctx context.Context, runID RunID, stepID string) error
    ListRuns(ctx context.Context, filter RunFilter) ([]*RunState, error)
    Close() error
}
```

Then pass it to the engine:

```go
engine, err := gostage.New(gostage.WithPersistence(myPostgresBackend))
```

A few implementation notes:

- **Thread safety.** The engine calls persistence methods from multiple
  goroutines (the worker pool, the timer scheduler, `RunSync` callers). Your
  implementation must handle concurrent access.

- **`SaveState` atomicity.** The engine passes a batch of dirty entries in a
  single call. Write them atomically (e.g. in a transaction) so a crash does
  not leave partial state.

- **`RunNotFoundError`.** Return a `*RunNotFoundError` from `LoadRun` when the
  run does not exist. The engine uses `errors.Is(err, ErrRunNotFound)` to
  distinguish "not found" from other failures.

- **`Close`.** The engine calls `Close` on the persistence layer during
  shutdown, after all in-flight runs have finished. Release connections, flush
  buffers, and clean up resources here.

- **In-memory marker.** If your implementation does not survive restarts,
  embed the `InMemoryPersistence` marker interface so the engine knows to
  fall back to blocking sleep.

## Schema versioning

The SQLite backend uses an automatic migration system. A `schema_version` table
tracks the current version number. On startup, the engine compares the stored
version against the migration list and applies any pending migrations in order,
each within its own transaction.

The current schema has three versions:

| Version | Changes |
|---------|---------|
| v1 | `runs` and `run_state` tables, indexes on `workflow_id` and `status` |
| v2 | `dyn_counter` column on `runs` for dynamic step mutation tracking |
| v3 | `step_statuses` table for O(1) per-step updates; drops unused `checkpoints` table |

Migrations are append-only. Existing migrations are never modified. When you
upgrade GoStage to a version with new migrations, the engine applies them
automatically the first time it opens the database. No manual `ALTER TABLE`
statements or migration tools are needed.

If you implement a custom `Persistence` backend, you manage your own schema.
The migration system is specific to the SQLite implementation.
