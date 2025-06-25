# 03 - Testing Spawned Workflows

Testing a workflow that runs in a separate process presents a unique challenge: your test needs to act as the parent process, spawn the child, and then verify the results communicated back from the child via gRPC.

A powerful and self-contained way to write these kinds of integration tests in Go is to use the `TestMain` function to create a single test binary that can act as both the parent *and* the child.

## The `TestMain` Pattern for gRPC Spawning

The `go test` command compiles your code and test files into a single executable. We can leverage this to create a "dual-mode" binary that supports both parent and child modes.

The pattern relies on the `--gostage-child` command-line flag to signal which mode the binary should run in.

1.  **The Parent (Test Function)**: Your `Test...` function will set up the parent `Runner` with automatic gRPC transport, define the `SubWorkflowDef`, and then execute the test binary *again*, but this time with the `--gostage-child` flag.
2.  **The Child (`TestMain`)**: When the new process starts, the `TestMain` function is the first thing to run. It checks for the `--gostage-child` flag. If it's present, `TestMain` bypasses the standard test execution and instead calls a `childMain` function, which contains the logic for the child worker.

Here's how you structure the `TestMain` function in your test file (e.g., `spawn_test.go`):

```go
func TestMain(m *testing.M) {
	// 1. Check for the --gostage-child flag
	for _, arg := range os.Args[1:] {
		if arg == "--gostage-child" {
			// 2. If it's present, we are the child. Run the child logic and exit.
			childMain()
			return
		}
	}

	// 3. If it's not present, we are the parent. Run the tests normally.
	os.Exit(m.Run())
}
```

### Parent Test Logic

Your actual test function acts as the parent. It sets up gRPC message handlers, defines the sub-workflow, and then spawns the child process. The gRPC transport is automatically configured.

```go
func TestSpawn(t *testing.T) {
    // Parent logic with automatic gRPC transport
    parentRunner := gostage.NewRunner() // gRPC is automatic!
    
    // Register handlers for gRPC messages from child
    parentRunner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
        var logData struct {
            Level   string `json:"level"`
            Message string `json:"message"`
        }
        json.Unmarshal(payload, &logData)
        t.Logf("[CHILD-%s] %s", logData.Level, logData.Message)
        return nil
    })
    
    // Handler for real-time updates
    parentRunner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
        var data struct {
            Key   string      `json:"key"`
            Value interface{} `json:"value"`
        }
        json.Unmarshal(payload, &data)
        t.Logf("Child update: %s = %v", data.Key, data.Value)
        return nil
    })
    
    // Handler for final store
    var finalStore map[string]interface{}
    parentRunner.Broker.RegisterHandler(gostage.MessageTypeFinalStore, func(msgType gostage.MessageType, payload json.RawMessage) error {
        return json.Unmarshal(payload, &finalStore)
    })

    subWorkflowDef := gostage.SubWorkflowDef{
        ID: "test-workflow",
        Stages: []gostage.StageDef{{
            ID: "test-stage",
            Actions: []gostage.ActionDef{{ID: "test-action"}},
        }},
    }

    // Spawn child with automatic gRPC transport
    err := parentRunner.Spawn(context.Background(), subWorkflowDef)
    assert.NoError(t, err)

    // Assert results received from the child via gRPC
    assert.NotNil(t, finalStore)
}
```

### Child Worker Logic

The `childMain` function contains all the logic the child needs: it parses gRPC connection info from command line, connects to the parent, requests the workflow definition, and executes it.

```go
func childMain() {
    // Parse gRPC connection arguments from command line
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

    // Register actions that the child process will need
    registerActions() // Critical: child must know how to build actions.

    // Create child runner with gRPC connection
    childRunner, err := gostage.NewChildRunner(grpcAddress, grpcPort)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to initialize child runner: %v\n", err)
        os.Exit(1)
    }

    // Request workflow definition from parent via gRPC
    childId := fmt.Sprintf("child-%d", os.Getpid())
    grpcTransport := childRunner.Broker.GetTransport()
    workflowDef, err := grpcTransport.RequestWorkflowDefinitionFromParent(context.Background(), childId)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to request workflow definition: %v\n", err)
        os.Exit(1)
    }

    // Create and execute workflow
    workflow, err := gostage.NewWorkflowFromDef(workflowDef)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to create workflow: %v\n", err)
        os.Exit(1)
    }

    // Logger that sends messages via gRPC
    logger := &ChildLogger{broker: childRunner.Broker}

    // Execute workflow
    if err := childRunner.Execute(context.Background(), workflow, logger); err != nil {
        fmt.Fprintf(os.Stderr, "Workflow execution failed: %v\n", err)
        os.Exit(1)
    }

    // Send final store to parent via gRPC
    if workflow.Store != nil {
        childRunner.Broker.Send(gostage.MessageTypeFinalStore, workflow.Store.ExportAll())
    }

    childRunner.Broker.Close()
    os.Exit(0)
}

// ChildLogger implements gostage.Logger and sends all logs via gRPC
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

## Key Benefits of gRPC Testing

- **Type Safety**: All communication uses protobuf, catching serialization errors at compile time
- **High Performance**: Binary protocol is efficient for test scenarios with lots of data
- **Automatic Setup**: No transport configuration needed - gRPC server and client are set up automatically
- **Real-time Communication**: Parent receives child messages immediately during test execution
- **Clean Process Isolation**: Tests run in completely separate processes while maintaining reliable communication

This pattern creates a fully self-contained integration test for your spawned workflows using high-performance gRPC communication, without needing separate binary builds or complex test scripts. 