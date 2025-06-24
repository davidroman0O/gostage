package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// This TestMain function is the key to testing the spawn functionality.
// It allows the test binary to be re-executed in a "child" mode.
func TestMain(m *testing.M) {
	// If this env var is set, we run the special child process main func.
	if os.Getenv("GOSTAGE_EXEC_CHILD") == "1" {
		childMain()
		return
	}
	// Otherwise, run the tests as normal (though we have no other tests in this file).
	fmt.Println("--- Running Parent Process ---")
	main()
}

// --- Action Definition ---

const (
	SimpleTestActionID = "simple-test-action"
)

// SimpleTestAction is a basic action that sends messages back to the parent.
type SimpleTestAction struct {
	gostage.BaseAction
}

func (a *SimpleTestAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("Child action is executing in process %d.", os.Getpid())
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "child.status", "value": "running"})
	time.Sleep(100 * time.Millisecond) // Simulate work
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "child.status", "value": "completed"})
	ctx.Logger.Info("Child action has finished.")
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

// childMain is the entrypoint for the spawned child process.
func childMain() {
	// Read workflow definition from stdin.
	workflowDef, err := gostage.ReadWorkflowDefinitionFromStdin()
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: failed to read workflow definition: %v\n", err)
		os.Exit(1)
	}

	// Create a child runner, which is automatically configured for JSON IPC.
	childRunner, err := gostage.NewChildRunner(*workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: failed to initialize runner: %v\n", err)
		os.Exit(1)
	}

	// The child process must have the same actions registered as the parent.
	gostage.RegisterAction(SimpleTestActionID, func() gostage.Action {
		return &SimpleTestAction{BaseAction: gostage.NewBaseAction(SimpleTestActionID, "A simple test action for spawning.")}
	})

	// Reconstruct the workflow from the definition.
	wf, err := gostage.NewWorkflowFromDef(workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: failed to create workflow from def: %v\n", err)
		os.Exit(1)
	}

	// Create a logger that sends all logs back to the parent.
	brokerLogger := &ChildLogger{broker: childRunner.Broker}

	// Execute the reconstructed workflow.
	if err := childRunner.Execute(context.Background(), wf, brokerLogger); err != nil {
		fmt.Fprintf(os.Stderr, "child: workflow execution failed: %v\n", err)
		os.Exit(1)
	}

	// Send the final store state back to the parent.
	if wf.Store != nil {
		childRunner.Broker.Send(gostage.MessageTypeFinalStore, wf.Store.ExportAll())
	}
	os.Exit(0)
}

// --- Parent Process Logic ---

func main() {
	// The parent must also register the action so it can be added to the definition.
	gostage.RegisterAction(SimpleTestActionID, func() gostage.Action {
		return &SimpleTestAction{BaseAction: gostage.NewBaseAction(SimpleTestActionID, "A simple test action for spawning.")}
	})

	// 1. Set up the parent's runner and a store to hold results from the child.
	parentRunner := gostage.NewRunner()
	parentStore := store.NewKVStore()
	var finalStoreFromChild map[string]interface{}
	var wg sync.WaitGroup

	// 2. Register handlers on the parent's broker to process messages.
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var data struct {
			Key   string      `json:"key"`
			Value interface{} `json:"value"`
		}
		if err := json.Unmarshal(payload, &data); err != nil {
			return err
		}
		fmt.Printf("Parent received IPC message: %s = %v\n", data.Key, data.Value)
		return parentStore.Put(data.Key, data.Value)
	})
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var logData struct{ Message string }
		json.Unmarshal(payload, &logData)
		fmt.Printf("[CHILD LOG] %s\n", logData.Message)
		return nil
	})
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeFinalStore, func(msgType gostage.MessageType, payload json.RawMessage) error {
		json.Unmarshal(payload, &finalStoreFromChild)
		fmt.Printf("Parent received final store from child with %d keys.\n", len(finalStoreFromChild))
		return nil
	})

	// 3. Define the sub-workflow for the child to execute.
	subWorkflowDef := gostage.SubWorkflowDef{
		ID: "child-workflow-json",
		Stages: []gostage.StageDef{{
			ID:      "child-stage",
			Actions: []gostage.ActionDef{{ID: SimpleTestActionID}},
		}},
		InitialStore: map[string]interface{}{"parent.message": "Hello from parent!"},
	}

	// 4. Spawn the child process.
	fmt.Println("Spawning child process...")
	err := spawnTestProcess(context.Background(), parentRunner, subWorkflowDef, &wg)
	if err != nil {
		fmt.Printf("Error spawning child process: %v\n", err)
	}

	// Wait for the message listener to finish.
	wg.Wait()
	fmt.Println("Parent process finished.")

	// 5. Verify results.
	status, _ := store.Get[string](parentStore, "child.status")
	fmt.Printf("\nFinal status received from child: %s\n", status)
	if finalStoreFromChild != nil {
		fmt.Printf("Message from parent in final store: %v\n", finalStoreFromChild["parent.message"])
	}
}

// spawnTestProcess is a helper that mimics runner.Spawn for this example.
func spawnTestProcess(ctx context.Context, r *gostage.Runner, def gostage.SubWorkflowDef, wg *sync.WaitGroup) error {
	// Set the transport type in the definition for the child.
	def.Transport = &gostage.TransportConfig{Type: gostage.TransportJSON}
	defBytes, err := json.Marshal(def)
	if err != nil {
		return err
	}

	// Get the path to the current running test binary.
	exePath, err := os.Executable()
	if err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, exePath)
	cmd.Env = append(os.Environ(), "GOSTAGE_EXEC_CHILD=1")
	childStdout, _ := cmd.StdoutPipe()
	childStdin, _ := cmd.StdinPipe()
	cmd.Stderr = os.Stderr // Show child's errors in parent's console.

	if err := cmd.Start(); err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.Broker.Listen(childStdout); err != nil && err != io.EOF {
			fmt.Printf("Parent error listening to child: %v\n", err)
		}
	}()

	if _, err := childStdin.Write(defBytes); err != nil {
		return err
	}
	childStdin.Close()

	return cmd.Wait()
}
