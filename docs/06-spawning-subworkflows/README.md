# Spawning Sub-Workflows

For greater isolation and stability, `gostage` allows you to execute a workflow, or a "sub-workflow," in a completely separate child process. This is a powerful feature for building robust systems where a failure in one part of a process should not bring down the entire application.

When you spawn a sub-workflow, `gostage` handles the complex parts of process management and inter-process communication (IPC) for you.

## The Parent/Child Execution Flow

The core of the spawning pattern is a single Go binary that can be executed in one of two modes: "parent" (the main application) or "child" (the spawned worker). The mode is typically determined by a command-line flag or an environment variable that the application checks at startup.

Here is a more detailed breakdown of the execution flow:

1.  **Parent Defines Work**: The parent process constructs a `SubWorkflowDef`. This is a serializable struct that describes the workflow to be run, including the unique IDs of the actions involved (as registered in the Action Registry).
2.  **Parent Spawns Itself**: The parent executes its own binary, adding a special flag like `--gostage-child` to the command. This new process is the "child".
3.  **Parent Sends Definition**: The parent serializes the `SubWorkflowDef` into a JSON string and writes it to the child's `stdin` pipe.
4.  **Child Starts in Worker Mode**: The child process starts, sees the `--gostage-child` flag, and knows it must act as a worker. Its first task is to read the workflow definition from its `stdin`.
5.  **Child Rebuilds Workflow**: The child uses the action IDs from the definition to look up the corresponding action factories in its own Action Registry and reconstructs the entire workflow in memory.
6.  **Child Executes**: The child creates a `Runner` and executes the reconstructed workflow.
7.  **Child Communicates**: As the child's actions run, they can use `ctx.Send()` to send `Message` objects (like logs or status updates) back to the parent. The child's `RunnerBroker` automatically marshals these messages to JSON and writes them to its `stdout`.
8.  **Parent Listens for Results**: The parent process continuously reads the `stdout` of the child process, deserializing the JSON messages and routing them to registered handlers. This allows the parent to react in real-time to the child's progress.

## The Application Entry Point

Your application's `main` function needs to handle a startup argument to differentiate between parent and child modes.

```go
// In your application's main.go
func main() {
    // A simple flag check to determine the execution mode.
    if len(os.Args) > 1 && os.Args[1] == "--gostage-child" {
        // If the flag is present, run the logic for the child worker.
        childMain()
        return
    } 
    
    // Otherwise, run the main parent application logic.
}
```
This clear division of responsibilities makes the spawning pattern robust. The parent orchestrates, and the child executes its assigned task in isolation.

## Why Spawn a Sub-Workflow?

-   **Isolation**: The sub-workflow runs in its own memory space. A crash or a panic in the child process will not affect the parent process.
-   **Resource Management**: You can manage the resources of the child process separately (e.g., memory, CPU limits).
-   **Parallelism**: While the `Runner` executes workflows sequentially, you can use standard Go concurrency patterns (goroutines) to spawn and manage multiple sub-workflows in parallel.
-   **Security**: The child process can potentially run with different permissions than the parent process.

## How It Works

1.  **Define the Work**: The parent process creates a `SubWorkflowDef`, which is a serializable, JSON-based representation of a workflow. This definition describes the stages and actions the child process should execute, referencing them by their registered IDs.
2.  **Spawn a Child Process**: The parent process uses `runner.Spawn()` to launch a new instance of itself as a child process.
3.  **Parent-Child Communication**: The parent passes the `SubWorkflowDef` to the child. The two processes then communicate over an IPC channel.
4.  **Child Executes**: The child process receives the definition, reconstructs the workflow using the Action Registry, and executes it. As it runs, it can send messages (logs, status updates, results) back to the parent.
5.  **Parent Listens**: The parent process listens for these messages and can react to them in real-time, for example, by updating its own state store.

This pattern requires that any action used in a sub-workflow be registered with the **Action Registry**, which is covered in the previous section.

## IPC Transports

`gostage` supports two primary mechanisms for Inter-Process Communication:

-   **JSON over Stdio**: The default method. Simple, human-readable, and requires no extra setup. The child process writes JSON messages to its standard output, and the parent reads them.
-   **gRPC**: A more advanced, high-performance option. It uses a binary protocol (Protobuf) over a TCP connection, which is more efficient and type-safe for high-frequency communication.

---

### In This Section

-   [**01 - IPC with JSON**](./01-ipc-json.md): Learn how to use the default JSON-based communication for spawned workflows.
-   [**02 - IPC with gRPC**](./02-ipc-grpc.md): Learn how to configure and use the high-performance gRPC transport. 