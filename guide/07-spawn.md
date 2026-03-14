# Child Process Spawning

GoStage can run ForEach iterations in isolated child processes instead of
goroutines. Each child is a fresh copy of your binary, connected to the parent
over gRPC. If one child crashes, the parent stays up. If a child leaks memory,
the leak dies with the process.


## The model

When a ForEach step has spawning enabled, the engine re-executes the **same
binary** with a hidden `--gostage-child` flag. The child connects back to the
parent over a local gRPC socket, receives its work assignment (task name, item,
state), executes the task, and sends the results back before exiting.

```
Parent process
  |-- starts gRPC server on localhost (random port)
  |-- for each item:
  |       exec(os.Executable(), "--gostage-child", "--grpc-addr=...", "--job-id=...")
  |       child connects, pulls work, executes, returns dirty state
  |       parent merges results into the shared store
  |-- shuts down gRPC server
```

There is no long-lived child pool. A new process is spawned per item, bounded
by the concurrency limit on the ForEach step. When all items are done (or the
first one fails), the gRPC server shuts down.


## The IsChild / HandleChild pattern

Because the child is the same binary, your `main()` must detect whether it was
launched as a child and hand control to the spawn subsystem immediately. This
must be the first thing in main, before flags are parsed or servers started:

```go
func main() {
    if spawn.IsChild() {
        registerTasks()          // same tasks the parent uses
        spawn.HandleChild()      // never returns -- calls os.Exit
        return
    }
    // normal parent setup continues here
    engine, _ := gostage.New(spawn.WithSpawn())
}
```

`IsChild()` checks `os.Args` for `--gostage-child`. `HandleChild()` runs the
child lifecycle -- connect, receive assignment, execute, return results, exit --
and calls `os.Exit`. It never returns. You can pass engine options if the child
needs configuration:

```go
spawn.HandleChild(gostage.WithLogger(myLogger), gostage.WithTaskMiddleware(myMW))
```

The child engine skips the worker pool and timer scheduler since children
execute a single task and exit.


## Task registration: both sides must agree

The parent and child are separate processes. When the parent tells a child "run
task `process.image`", the child must have that task in its own registry.

The simplest pattern is a shared registration function:

```go
func registerTasks() {
    gostage.Task("process.image", processImage)
    gostage.Task("validate.input", validateInput)
}

func main() {
    if spawn.IsChild() {
        registerTasks()
        spawn.HandleChild()
        return
    }
    registerTasks()
    engine, _ := gostage.New(spawn.WithSpawn())
}
```

If a child receives a task name it does not recognize, it exits with an error
and the parent treats that item as failed.


## State transfer

State flows between parent and child through serialization:

1. **Parent to child.** The parent serializes the workflow's key-value store
   (plus the ForEach item and index) into a byte map. The child receives this
   as its initial state.

2. **Child execution.** The child runs the task with a fresh `runState`. Parent
   values are marked clean; values the child writes are marked dirty.

3. **Child to parent.** On completion, the child serializes only dirty keys and
   sends them back over gRPC as a `FINAL_STORE` message.

4. **Parent merge.** The parent deserializes the dirty state and merges it into
   the workflow store. Internal keys (prefixed with `__`) are filtered out.

A child that reads 100 keys but writes 2 sends only those 2 keys back.


## gRPC communication

The parent starts a gRPC server on `localhost:0` (random port) for each
ForEach-with-spawn execution.

### Authentication

Every gRPC call is authenticated with two layers:

- **Shared secret.** A UUID generated per ForEach execution, passed via
  `GOSTAGE_SECRET` and validated through the `x-gostage-secret` metadata header
  using constant-time comparison.

- **Per-job token.** A second UUID unique to each child, passed via
  `GOSTAGE_JOB_TOKEN` as `x-gostage-job-token`. This prevents one child from
  impersonating another.

### Pull model

The child initiates communication. After connecting, it calls
`RequestWorkflowDefinition` to pull its work assignment. The parent never pushes
to a child -- all connections are outbound from child to a known localhost port.

### Message types

| Type | Direction | Purpose |
|------|-----------|---------|
| `FINAL_STORE` | child to parent | Child's dirty state |
| `WORKFLOW_RESULT` | child to parent | Task failure with error message |
| `BAIL` | child to parent | Intentional early exit via `Bail()` |
| `IPC` | child to parent | Application-level messages via `Send()` |

Both sides use gRPC keepalive (10s ping, 5s timeout) for faster dead connection
detection.


## Orphan detection

If the parent crashes, children must not run forever. Three mechanisms work
together:

### Lifeline pipe

The parent creates an OS pipe before spawning. The child inherits the read end
as fd 3; the parent holds the write end. If the parent dies, the OS closes the
write end and the child's blocking `Read()` returns EOF, triggering
cancellation. This is the fastest mechanism.

### PID polling

The child records its parent PID at startup. A goroutine polls every 2 seconds.
On Unix, a dead parent causes re-parenting to PID 1. If PPID changes, the child
cancels itself. This is the fallback when the pipe is unavailable.

### pdeathsig (Linux only)

On Linux, the child is started with `Pdeathsig = SIGKILL`. The kernel sends
SIGKILL when the parent thread dies. The calling goroutine is locked to its OS
thread because `PR_SET_PDEATHSIG` is per-thread. On non-Linux platforms this is
a no-op.


## Spawn depth limits

Children can spawn grandchildren (HandleChild includes `WithSpawn()` by
default). Depth is tracked through environment variables:

- `GOSTAGE_SPAWN_DEPTH` -- incremented per generation (root=0, child=1, ...).
- `GOSTAGE_MAX_SPAWN_DEPTH` -- maximum allowed depth. Default: 5.

A child exceeding the limit exits immediately with an error. Override:

```bash
export GOSTAGE_MAX_SPAWN_DEPTH=2   # parent -> child only, no grandchildren
```


## Child environment and timeout

Children receive a minimal, filtered environment: `PATH`, `HOME`, `USER`,
`LOGNAME`, `SHELL`, temp dirs, `LANG`/`LC_*`, and the two spawn-specific
variables (`GOSTAGE_CHILD_TIMEOUT`, `GOSTAGE_MAX_SPAWN_DEPTH`). Credentials
and API keys are **not** forwarded. Pass secrets through the workflow store.

Children have a default 5-minute timeout. Override with:

```bash
export GOSTAGE_CHILD_TIMEOUT=10m
```


## Optional dependency

The `spawn` package is a separate import. The core `gostage` package has no
dependency on gRPC. Import spawn only when needed:

```go
import "github.com/davidroman0O/gostage/spawn"
engine, _ := gostage.New(spawn.WithSpawn())
```

If a ForEach step uses spawning but no `SpawnRunner` is installed, the engine
returns an error at runtime.


## Sub-workflow spawning

ForEach can spawn children that run entire sub-workflows. The parent serializes
the workflow definition to JSON and sends it alongside the state. The child
rebuilds and executes all stages, returning dirty state at the end. Tasks
referenced by the sub-workflow must still be registered in the child's registry.
