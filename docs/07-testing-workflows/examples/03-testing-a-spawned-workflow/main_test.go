package examples

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/stretchr/testify/assert"
)

// TestMain acts as the entry point for the test binary. It allows the
// same binary to act as both the parent test runner and the spawned child process.
func TestMain(m *testing.M) {
	// Check for the --gostage-child flag instead of environment variable
	for _, arg := range os.Args[1:] {
		if arg == "--gostage-child" {
			// If the flag is present, run the child logic and exit
			childMain()
			return
		}
	}
	// If the flag is not present, run the tests in this file as normal
	os.Exit(m.Run())
}

// --- Action to be executed by the child ---

const TestActionID = "test-action"

type TestAction struct {
	gostage.BaseAction
}

func (a *TestAction) Execute(ctx *gostage.ActionContext) error {
	// Send real-time messages back to the parent via gRPC
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "child.status",
		"value": "running",
	})

	// Simulate some work
	time.Sleep(100 * time.Millisecond)

	// Store data in the workflow store
	ctx.Workflow.Store.Put("child_result", map[string]interface{}{
		"executed_by_pid": os.Getpid(),
		"execution_time":  time.Now().Format("2006-01-02 15:04:05"),
		"transport":       "grpc",
	})

	// Send completion message
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "child.status",
		"value": "completed",
	})

	return nil
}

// Both parent and child must register the action.
func registerTestAction() {
	gostage.RegisterAction(TestActionID, func() gostage.Action {
		return &TestAction{BaseAction: gostage.NewBaseAction(TestActionID, "Test action for spawned workflow")}
	})
}

// ChildLogger implements gostage.Logger and sends all logs via gRPC broker
type ChildLogger struct {
	broker *gostage.RunnerBroker
}

func (l *ChildLogger) Debug(format string, args ...interface{}) { l.send("DEBUG", format, args...) }
func (l *ChildLogger) Info(format string, args ...interface{})  { l.send("INFO", format, args...) }
func (l *ChildLogger) Warn(format string, args ...interface{})  { l.send("WARN", format, args...) }
func (l *ChildLogger) Error(format string, args ...interface{}) { l.send("ERROR", format, args...) }

func (l *ChildLogger) send(level, format string, args ...interface{}) {
	payload := map[string]string{
		"level":   level,
		"message": fmt.Sprintf(format, args...),
	}
	l.broker.Send(gostage.MessageTypeLog, payload)
}

// --- Child Process Logic ---

// childMain contains the code that will be executed only by the child process.
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
	registerTestAction()

	// Create child runner with gRPC connection (NEW PURE GRPC API)
	childRunner, err := gostage.NewChildRunner(grpcAddress, grpcPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process failed to initialize: %v\n", err)
		os.Exit(1)
	}

	// Request workflow definition from parent (this also signals that we're ready)
	childId := fmt.Sprintf("child-%d", os.Getpid())
	grpcTransport := childRunner.Broker.GetTransport()
	workflowDef, err := grpcTransport.RequestWorkflowDefinitionFromParent(context.Background(), childId)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to request workflow definition: %v\n", err)
		os.Exit(1)
	}

	// Create workflow from definition
	workflow, err := gostage.NewWorkflowFromDef(workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process failed to create workflow: %v\n", err)
		os.Exit(1)
	}

	// Create logger that sends all messages to parent via gRPC
	logger := &ChildLogger{broker: childRunner.Broker}

	// Execute workflow
	if err := childRunner.Execute(context.Background(), workflow, logger); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process workflow execution failed: %v\n", err)
		os.Exit(1)
	}

	// Send final store state to parent via gRPC
	if workflow.Store != nil {
		finalStore := workflow.Store.ExportAll()
		if err := childRunner.Broker.Send(gostage.MessageTypeFinalStore, finalStore); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️  Child process failed to send final store: %v\n", err)
		}
	}

	// Close broker to clean up gRPC connections
	childRunner.Broker.Close()
	os.Exit(0)
}

// --- Parent Process Test ---

// TestSpawnedWorkflow acts as the parent process. It defines and spawns the
// sub-workflow using pure gRPC, then waits for and verifies the result.
func TestSpawnedWorkflow(t *testing.T) {
	registerTestAction()

	// 1. Set up the parent's runner with automatic gRPC transport
	parentRunner := gostage.NewRunner() // gRPC is automatic!

	// Variables to track test results
	var childStatus string
	var finalStoreFromChild map[string]interface{}
	var logMessages []string

	// 2. Register handlers to process gRPC messages from the child
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var logData struct {
			Level   string `json:"level"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(payload, &logData); err != nil {
			return err
		}

		logMessage := fmt.Sprintf("[CHILD-%s] %s", logData.Level, logData.Message)
		logMessages = append(logMessages, logMessage)
		t.Logf("%s", logMessage)
		return nil
	})

	parentRunner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var data struct {
			Key   string      `json:"key"`
			Value interface{} `json:"value"`
		}
		if err := json.Unmarshal(payload, &data); err != nil {
			return err
		}

		// Track child status updates
		if data.Key == "child.status" {
			if status, ok := data.Value.(string); ok {
				childStatus = status
				t.Logf("Child status update: %s", status)
			}
		}

		return nil
	})

	parentRunner.Broker.RegisterHandler(gostage.MessageTypeFinalStore, func(msgType gostage.MessageType, payload json.RawMessage) error {
		if err := json.Unmarshal(payload, &finalStoreFromChild); err != nil {
			return fmt.Errorf("failed to unmarshal final store: %w", err)
		}
		t.Logf("Received final store with %d keys", len(finalStoreFromChild))
		return nil
	})

	// 3. Define the workflow for the child to run
	subWorkflowDef := gostage.SubWorkflowDef{
		ID:          "test-child-workflow",
		Name:        "Test Child Workflow",
		Description: "Tests spawned workflow execution via pure gRPC",
		Stages: []gostage.StageDef{{
			ID:   "test-stage",
			Name: "Test Stage",
			Actions: []gostage.ActionDef{{
				ID: TestActionID,
			}},
		}},
	}

	// 4. Spawn the child process with automatic gRPC transport
	t.Log("Spawning child process with pure gRPC transport...")

	err := parentRunner.Spawn(context.Background(), subWorkflowDef)
	assert.NoError(t, err, "Spawning the child process should not produce an error")

	// 5. Clean up gRPC server
	defer parentRunner.Broker.Close()

	// 6. Assert test results
	assert.Equal(t, "completed", childStatus, "Child should have completed successfully")
	assert.NotNil(t, finalStoreFromChild, "Should have received final store from child")

	// Verify the child result data
	if assert.Contains(t, finalStoreFromChild, "child_result", "Final store should contain child_result") {
		childResult, ok := finalStoreFromChild["child_result"].(map[string]interface{})
		assert.True(t, ok, "child_result should be a map")
		if ok {
			assert.Contains(t, childResult, "executed_by_pid", "Should contain child PID")
			assert.Contains(t, childResult, "execution_time", "Should contain execution time")
			assert.Equal(t, "grpc", childResult["transport"], "Should confirm gRPC transport")

			// Verify the child ran in a different process
			childPID := int(childResult["executed_by_pid"].(float64))
			parentPID := os.Getpid()
			assert.NotEqual(t, parentPID, childPID, "Child should run in different process than parent")

			t.Logf("✅ Child executed in process %d (parent is %d)", childPID, parentPID)
			t.Logf("✅ Child execution time: %s", childResult["execution_time"])
		}
	}

	// Verify we received log messages
	assert.Greater(t, len(logMessages), 0, "Should have received log messages from child")

	t.Log("🎉 Pure gRPC spawned workflow test completed successfully!")
}
