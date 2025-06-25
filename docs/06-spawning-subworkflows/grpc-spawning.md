# gRPC Spawning

`gostage` uses **pure gRPC** for all Inter-Process Communication (IPC) between parent and child processes. This provides high-performance, type-safe communication using Protocol Buffers (Protobuf) for serialization.

## Why gRPC?

-   **Performance**: gRPC's binary Protobuf format is significantly faster and smaller than text-based formats, making it ideal for high-frequency messaging.
-   **Type Safety**: The communication contract is defined in a `.proto` file, providing compile-time checks and reducing the chance of runtime errors due to malformed messages.
-   **Streaming**: gRPC has built-in support for bidirectional streaming, which can be leveraged for more complex communication patterns.
-   **Automatic Setup**: No configuration needed - the gRPC server and client connections are established automatically.
-   **Port Management**: Automatic port assignment prevents conflicts when running multiple workflows.

## How It Works

The parent/child spawning pattern uses gRPC for all communication:

1.  **Parent Starts Server**: The parent process's `Runner` automatically starts a gRPC server on an available TCP port. The operating system assigns an available port automatically, preventing port conflicts.
2.  **Parent Spawns Child**: The parent uses `runner.Spawn()` and automatically passes the gRPC server's address and port to the child process via command-line arguments.
3.  **Child Connects as Client**: The child process parses the gRPC connection parameters from the command line and automatically connects to the parent's gRPC server.
4.  **Type-Safe Communication**: All subsequent communication from the child back to the parent happens via type-safe gRPC calls using protobuf messages.

## Automatic gRPC Configuration

Using gRPC is completely automatic - no configuration needed:

```go
// --- In the Parent Process ---

// Create a runner - gRPC is automatic!
parentRunner := gostage.NewRunner()

// Register handlers to receive messages from child processes
parentRunner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
    var logData struct {
        Level   string `json:"level"`
        Message string `json:"message"`
    }
    json.Unmarshal(payload, &logData)
    fmt.Printf("[CHILD-%s] %s\n", logData.Level, logData.Message)
    return nil
})

parentRunner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
    var data struct {
        Key   string      `json:"key"`
        Value interface{} `json:"value"`
    }
    json.Unmarshal(payload, &data)
    fmt.Printf("Child updated: %s = %v\n", data.Key, data.Value)
    return nil
})

// Define and spawn workflow - gRPC communication is automatic
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
// --- In the Child Process ---

func childMain() {
    // Parse gRPC connection from command line (automatic)
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
    
    // Register actions that the child will need
    gostage.RegisterAction("my-action", func() gostage.Action {
        return &MyAction{BaseAction: gostage.NewBaseAction("my-action", "My action")}
    })
    
    // Create child runner and connect via gRPC (automatic)
    childRunner, err := gostage.NewChildRunner(grpcAddress, grpcPort)
    if err != nil {
        log.Fatal(err)
    }
    
    // Request workflow definition from parent (automatic gRPC call)
    childId := fmt.Sprintf("child-%d", os.Getpid())
    grpcTransport := childRunner.Broker.GetTransport()
    workflowDef, err := grpcTransport.RequestWorkflowDefinitionFromParent(context.Background(), childId)
    if err != nil {
        log.Fatal(err)
    }
    
    // Create workflow from definition
    workflow, err := gostage.NewWorkflowFromDef(workflowDef)
    if err != nil {
        log.Fatal(err)
    }
    
    // Execute workflow - all communication is via gRPC
    logger := &ChildLogger{broker: childRunner.Broker}
    if err := childRunner.Execute(context.Background(), workflow, logger); err != nil {
        log.Fatal(err)
    }
    
    // Send final store to parent via gRPC
    if workflow.Store != nil {
        finalStore := workflow.Store.ExportAll()
        childRunner.Broker.Send(gostage.MessageTypeFinalStore, finalStore)
    }
    
    childRunner.Broker.Close()
}

// ChildLogger sends all logs via gRPC
type ChildLogger struct {
    broker *gostage.RunnerBroker
}

func (l *ChildLogger) Info(format string, args ...interface{}) {
    l.broker.Send(gostage.MessageTypeLog, map[string]string{
        "level":   "INFO",
        "message": fmt.Sprintf(format, args...),
    })
}
// ... implement other log levels
```

## Key Benefits

-   **Zero Configuration**: No transport setup required - everything is automatic
-   **Type Safety**: All messages use protobuf for type-safe communication
-   **High Performance**: Binary protocol is efficient for high-frequency messaging
-   **Automatic Port Management**: No port conflicts when running multiple workflows
-   **Real-time Communication**: Parent receives child messages immediately via gRPC streaming
-   **Clean Process Isolation**: Child processes are completely isolated but can communicate efficiently

The gRPC transport handles all the complexity of process communication, allowing you to focus on your workflow logic while ensuring reliable, high-performance inter-process communication.

---

This completes the documentation for spawning sub-workflows. For more advanced topics, see the [**Advanced Topics**](../07-advanced-topics/README.md) section. 