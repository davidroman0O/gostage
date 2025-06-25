# Spawning Sub-Workflows

For greater isolation and stability, `gostage` allows you to execute a workflow, or a "sub-workflow," in a completely separate child process. This is a powerful feature for building robust systems where a failure in one part of a process should not bring down the entire application.

When you spawn a sub-workflow, `gostage` handles the complex parts of process management and inter-process communication (IPC) for you using **high-performance gRPC**.

## The Parent/Child Execution Flow

The core of the spawning pattern is a single Go binary that can be executed in one of two modes: "parent" (the main application) or "child" (the spawned worker). The mode is determined by the `--gostage-child` command-line flag that the application checks at startup.

Here is a detailed breakdown of the execution flow:

1.  **Parent Defines Work**: The parent process constructs a `SubWorkflowDef`. This is a serializable struct that describes the workflow to be run, including the unique IDs of the actions involved (as registered in the Action Registry).
2.  **Parent Sets Up gRPC Server**: The parent automatically starts a gRPC server on an available port to handle communication with child processes.
3.  **Parent Spawns Child**: The parent executes its own binary, adding the `--gostage-child` flag along with gRPC connection parameters (`--grpc-address` and `--grpc-port`).
4.  **Child Connects via gRPC**: The child process starts, parses the gRPC connection arguments, and connects to the parent's gRPC server.
5.  **Child Requests Workflow**: The child signals readiness and requests the workflow definition from the parent via a gRPC call.
6.  **Child Rebuilds Workflow**: The child uses the action IDs from the definition to look up the corresponding action factories in its own Action Registry and reconstructs the entire workflow in memory.
7.  **Child Executes**: The child creates a `Runner` and executes the reconstructed workflow.
8.  **Real-time Communication**: As the child's actions run, they can use `ctx.Send()` to send type-safe protobuf messages back to the parent via gRPC. This includes logs, status updates, and data.
9.  **Parent Receives Messages**: The parent process receives these gRPC messages in real-time and routes them to registered handlers, allowing immediate reaction to the child's progress.
10. **Final Store Transfer**: When the workflow completes, the child sends its final store state back to the parent via gRPC.

## The Application Entry Point

Your application's `main` function needs to handle the `--gostage-child` argument to differentiate between parent and child modes.

```go
// In your application's main.go
func main() {
    // Check for child process mode
    for _, arg := range os.Args[1:] {
        if arg == "--gostage-child" {
            // If the flag is present, run the logic for the child worker.
            childMain()
            return
        }
    }
    
    // Otherwise, run the main parent application logic.
    parentMain()
}
```

This clear division of responsibilities makes the spawning pattern robust. The parent orchestrates, and the child executes its assigned task in isolation.

## Why Spawn a Sub-Workflow?

-   **Isolation**: The sub-workflow runs in its own memory space. A crash or a panic in the child process will not affect the parent process.
-   **Resource Management**: You can manage the resources of the child process separately (e.g., memory, CPU limits).
-   **Parallelism**: While the `Runner` executes workflows sequentially, you can use standard Go concurrency patterns (goroutines) to spawn and manage multiple sub-workflows in parallel.
-   **Security**: The child process can potentially run with different permissions than the parent process.
-   **Type Safety**: All communication uses strongly-typed protobuf messages, preventing serialization errors.
-   **Performance**: gRPC's binary protocol is efficient for high-frequency communication.

## How It Works

1.  **Define the Work**: The parent process creates a `SubWorkflowDef`, which is a serializable representation of a workflow. This definition describes the stages and actions the child process should execute, referencing them by their registered IDs.
2.  **Spawn a Child Process**: The parent process uses `runner.Spawn()` to launch a new instance of itself as a child process. gRPC transport is automatically configured.
3.  **gRPC Communication**: The parent and child communicate over a gRPC connection using type-safe protobuf messages.
4.  **Child Executes**: The child process receives the definition via gRPC, reconstructs the workflow using the Action Registry, and executes it. As it runs, it sends messages (logs, status updates, results) back to the parent via gRPC.
5.  **Parent Listens**: The parent process listens for these gRPC messages and can react to them in real-time, for example, by updating its own state store.

This pattern requires that any action used in a sub-workflow be registered with the **Action Registry**, which is covered in the previous section.

## Pure gRPC Transport

`gostage` uses **pure gRPC** for all Inter-Process Communication, providing:

-   **High Performance**: Binary protobuf format is faster and more compact than text-based formats
-   **Type Safety**: Communication contract is defined in protobuf, providing compile-time safety
-   **Automatic Setup**: No configuration needed - gRPC server and client are set up automatically
-   **Port Management**: Automatic port assignment prevents conflicts in multi-process scenarios
-   **Streaming Support**: Built-in support for bidirectional streaming for complex communication patterns

## Example Usage

```go
// Parent process - no configuration needed!
parentRunner := gostage.NewRunner() // gRPC is automatic

// Register message handlers
parentRunner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
    // Handle child logs
    return nil
})

// Define and spawn workflow
workflowDef := gostage.SubWorkflowDef{
    ID: "my-workflow",
    Stages: []gostage.StageDef{{
        ID: "my-stage",
        Actions: []gostage.ActionDef{{ID: "my-action"}},
    }},
}

err := parentRunner.Spawn(context.Background(), workflowDef)
```

```go
// Child process - automatic gRPC setup
func childMain() {
    // Parse gRPC connection from command line (handled automatically)
    var grpcAddress string = "localhost"
    var grpcPort int = 50051
    
    for _, arg := range os.Args {
        if strings.HasPrefix(arg, "--grpc-address=") {
            grpcAddress = strings.TrimPrefix(arg, "--grpc-address=")
        } else if strings.HasPrefix(arg, "--grpc-port=") {
            if port, err := strconv.Atoi(strings.TrimPrefix(arg, "--grpc-port=")); err == nil {
                grpcPort = port
            }
        }
    }
    
    // Create child runner and connect via gRPC
    childRunner, err := gostage.NewChildRunner(grpcAddress, grpcPort)
    if err != nil {
        log.Fatal(err)
    }
    
    // Request workflow definition from parent
    childId := fmt.Sprintf("child-%d", os.Getpid())
    grpcTransport := childRunner.Broker.GetTransport()
    workflowDef, err := grpcTransport.RequestWorkflowDefinitionFromParent(context.Background(), childId)
    if err != nil {
        log.Fatal(err)
    }
    
    // Execute workflow...
}
```

---

### In This Section

-   [**gRPC Spawning**](./02-ipc-grpc.md): Learn how to use the high-performance gRPC transport for spawned workflows. 