# 02 - IPC with gRPC

For more demanding use cases, `gostage` provides a high-performance Inter-Process Communication (IPC) transport based on **gRPC**. This transport uses Protocol Buffers (Protobuf) for serialization, which is more efficient and strictly typed than JSON.

## Why Use gRPC?

-   **Performance**: gRPC's binary Protobuf format is significantly faster and smaller than text-based JSON, making it ideal for high-frequency messaging.
-   **Type Safety**: The communication contract is defined in a `.proto` file, providing compile-time checks and reducing the chance of runtime errors due to malformed messages.
-   **Streaming**: gRPC has built-in support for bidirectional streaming, which can be leveraged for more complex communication patterns.
-   **Cleanliness**: It avoids the need to manage `stdin`/`stdout` pipes directly and instead uses a standard network socket, which can be easier to reason about and debug.

## How It Works

The overall parent/child pattern remains the same as with the JSON transport, but the underlying communication mechanism changes.

1.  **Parent Starts Server**: The parent process's `Runner` starts a gRPC server on a TCP port. If you specify port `0`, the operating system will automatically assign an available port, preventing port conflicts.
2.  **Parent Spawns Child**: The parent uses `runner.Spawn()` as before. `gostage` automatically includes the gRPC server's address and assigned port in the `SubWorkflowDef` that is sent to the child.
3.  **Child Connects as Client**: The child process reads the `SubWorkflowDef`, sees the gRPC configuration, and automatically connects to the parent's gRPC server as a client.
4.  **Communication**: All subsequent communication from the child back to the parent happens via type-safe gRPC calls, not over `stdout`.

## Configuring gRPC Transport

Switching to gRPC is designed to be as seamless as possible. You simply configure the parent's `Runner` to use the gRPC transport.

```go
// --- In the Parent Process ---

// Create a runner and configure it to use gRPC.
// By providing port 0, we let the system pick an available port automatically.
parentRunner := gostage.NewRunner(gostage.WithGRPCTransport("localhost", 0))

// The rest of the parent logic (registering handlers, defining the
// workflow, and calling runner.Spawn()) remains exactly the same as with
// the JSON transport.
// ...
```

The child process (`childMain`) requires **no changes**. The `gostage.NewChildRunner()` function will automatically detect the gRPC configuration in the `SubWorkflowDef` and set up the client connection accordingly.

This clean separation of concerns means you can switch between IPC transports by changing only a single line of configuration in the parent process, without altering any of the core workflow or child process logic.

---

This completes the section on Spawning Sub-Workflows. The next chapter will cover [**Advanced Topics**](../06-advanced-topics/README.md). 