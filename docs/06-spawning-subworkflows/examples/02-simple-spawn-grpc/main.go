package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// This TestMain function is the key to testing the spawn functionality.
// It allows the test binary to be re-executed in a "child" mode.
func TestMain(m *testing.M) {
	if os.Getenv("GOSTAGE_EXEC_CHILD") == "1" {
		childMain()
		return
	}
	fmt.Println("--- Running Parent Process (gRPC) ---")
	main()
}

// --- Action Definition ---

const (
	GrpcTestActionID = "grpc-test-action"
)

// GrpcTestAction is a basic action that sends messages back to the parent via gRPC.
type GrpcTestAction struct {
	gostage.BaseAction
}

func (a *GrpcTestAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("gRPC child action is executing in process %d.", os.Getpid())
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "child.status.grpc", "value": "running"})
	time.Sleep(100 * time.Millisecond) // Simulate work
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "child.status.grpc", "value": "completed"})
	ctx.Logger.Info("gRPC child action has finished.")
	return nil
}

// ChildLogger sends log messages over the communication broker.
type ChildLogger struct {
	broker *gostage.RunnerBroker
}

func (l *ChildLogger) send(level, format string, args ...interface{}) {
	payload := map[string]string{"level": level, "message": fmt.Sprintf(format, args...)}
	l.broker.Send(gostage.MessageTypeLog, payload)
}
func (l *ChildLogger) Debug(format string, args ...interface{}) { l.send("DEBUG", format, args...) }
func (l *ChildLogger) Info(format string, args ...interface{})  { l.send("INFO", format, args...) }
func (l *ChildLogger) Warn(format string, args ...interface{})  { l.send("WARN", format, args...) }
func (l *ChildLogger) Error(format string, args ...interface{}) { l.send("ERROR", format, args...) }

// --- Child Process Logic ---

func childMain() {
	workflowDef, err := gostage.ReadWorkflowDefinitionFromStdin()
	if err != nil {
		fmt.Fprintf(os.Stderr, "child-grpc: failed to read workflow definition: %v\n", err)
		os.Exit(1)
	}

	// NewChildRunner automatically detects gRPC config and connects as a client.
	childRunner, err := gostage.NewChildRunner(*workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child-grpc: failed to initialize runner: %v\n", err)
		os.Exit(1)
	}

	// Register the action.
	gostage.RegisterAction(GrpcTestActionID, func() gostage.Action {
		return &GrpcTestAction{BaseAction: gostage.NewBaseAction(GrpcTestActionID, "A test action for gRPC spawning.")}
	})

	wf, err := gostage.NewWorkflowFromDef(workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child-grpc: failed to create workflow from def: %v\n", err)
		os.Exit(1)
	}

	brokerLogger := &ChildLogger{broker: childRunner.Broker}

	if err := childRunner.Execute(context.Background(), wf, brokerLogger); err != nil {
		fmt.Fprintf(os.Stderr, "child-grpc: workflow execution failed: %v\n", err)
		os.Exit(1)
	}

	if wf.Store != nil {
		childRunner.Broker.Send(gostage.MessageTypeFinalStore, wf.Store.ExportAll())
	}
	childRunner.Broker.Close() // Important for gRPC to close the connection.
	os.Exit(0)
}

// --- Parent Process Logic ---

func main() {
	// Register the action in the parent.
	gostage.RegisterAction(GrpcTestActionID, func() gostage.Action {
		return &GrpcTestAction{BaseAction: gostage.NewBaseAction(GrpcTestActionID, "A test action for gRPC spawning.")}
	})

	// 1. Create a runner and configure it to use gRPC.
	//    Using port 0 lets the OS pick an available port automatically.
	parentRunner := gostage.NewRunner(gostage.WithGRPCTransport("localhost", 0))

	parentStore := store.NewKVStore()
	var finalStoreFromChild map[string]interface{}

	// 2. Register handlers. The logic is identical to the JSON example.
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var data struct {
			Key   string      `json:"key"`
			Value interface{} `json:"value"`
		}
		if err := json.Unmarshal(payload, &data); err != nil {
			return err
		}
		fmt.Printf("Parent received gRPC message: %s = %v\n", data.Key, data.Value)
		return parentStore.Put(data.Key, data.Value)
	})
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var logData struct{ Message string }
		json.Unmarshal(payload, &logData)
		fmt.Printf("[CHILD LOG/gRPC] %s\n", logData.Message)
		return nil
	})
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeFinalStore, func(msgType gostage.MessageType, payload json.RawMessage) error {
		json.Unmarshal(payload, &finalStoreFromChild)
		fmt.Printf("Parent received final store from gRPC child with %d keys.\n", len(finalStoreFromChild))
		return nil
	})

	// 3. Define the sub-workflow.
	subWorkflowDef := gostage.SubWorkflowDef{
		ID: "child-workflow-grpc",
		Stages: []gostage.StageDef{{
			ID:      "child-stage-grpc",
			Actions: []gostage.ActionDef{{ID: GrpcTestActionID}},
		}},
	}

	// 4. Spawn the child process.
	//    The runner will automatically handle starting the gRPC server and
	//    passing the connection info to the child.
	fmt.Println("Spawning child process with gRPC transport...")
	errCh := make(chan error, 1)
	go func() {
		errCh <- parentRunner.Spawn(context.Background(), subWorkflowDef)
	}()

	// Wait for spawn to complete or timeout
	select {
	case err := <-errCh:
		if err != nil {
			fmt.Printf("Error spawning gRPC child process: %v\n", err)
		}
	case <-time.After(5 * time.Second):
		fmt.Println("Spawn operation timed out.")
	}

	parentRunner.Broker.Close() // Clean up the server.
	fmt.Println("Parent process finished.")

	// 5. Verify results.
	status, _ := store.Get[string](parentStore, "child.status.grpc")
	fmt.Printf("\nFinal status received from gRPC child: %s\n", status)
}
