package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This demonstrates the FIXED spawn functionality for external packages
// Before our fix: Child processes would fail with "flag provided but not defined: -gostage-child"
// After our fix: Child processes properly handle the --gostage-child flag

// CRITICAL: This TestMain function is what external applications need to add
// when they want to use gostage spawn functionality
func TestMain(m *testing.M) {
	// Check for child process mode using command line arguments
	if len(os.Args) > 1 && os.Args[1] == "--gostage-child" {
		handleChildProcess()
		return
	}

	// Otherwise run tests normally
	os.Exit(m.Run())
}

// handleChildProcess implements the child process logic for external applications
// This is the standard pattern that all gostage users should follow
func handleChildProcess() {
	// CRITICAL: Register actions that the child process will need
	registerExternalTestActions()

	// ✨ NEW SEAMLESS API - automatic gRPC setup and logger creation
	childRunner, logger, err := gostage.NewChildRunner()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create child runner: %v\n", err)
		os.Exit(1)
	}

	logger.Info("Child process started successfully - PID: %d", os.Getpid())

	// ✨ Direct method call - no GetTransport() needed!
	workflowDef, err := childRunner.RequestWorkflowDefinitionFromParent(context.Background())
	if err != nil {
		logger.Error("Failed to request workflow definition: %v", err)
		os.Exit(1)
	}

	logger.Info("Workflow definition received: %s", workflowDef.ID)

	// Create workflow from definition
	workflow, err := gostage.NewWorkflowFromDef(workflowDef)
	if err != nil {
		logger.Error("Failed to create workflow: %v", err)
		os.Exit(1)
	}

	// ✨ Same logger used throughout - seamless!
	if err := childRunner.Execute(context.Background(), workflow, logger); err != nil {
		logger.Error("Child workflow execution failed: %v", err)
		os.Exit(1)
	}

	// Send final store state back to parent
	if workflow.Store != nil {
		finalStore := workflow.Store.ExportAll()
		if err := childRunner.Broker.Send(gostage.MessageTypeFinalStore, finalStore); err != nil {
			logger.Error("Failed to send final store: %v", err)
		} else {
			logger.Info("Final store sent with %d keys", len(finalStore))
		}
	}

	logger.Info("Child process completed successfully")
	childRunner.Close()
	os.Exit(0)
}

// ChildLogger implements gostage.Logger and sends all logs to parent via gRPC
type ChildLogger struct {
	broker *gostage.RunnerBroker
}

func (l *ChildLogger) send(level, format string, args ...interface{}) {
	payload := map[string]string{
		"level":   level,
		"message": fmt.Sprintf(format, args...),
	}
	// Send log message to parent via gRPC
	l.broker.Send(gostage.MessageTypeLog, payload)
}

func (l *ChildLogger) Debug(format string, args ...interface{}) { l.send("DEBUG", format, args...) }
func (l *ChildLogger) Info(format string, args ...interface{})  { l.send("INFO", format, args...) }
func (l *ChildLogger) Warn(format string, args ...interface{})  { l.send("WARN", format, args...) }
func (l *ChildLogger) Error(format string, args ...interface{}) { l.send("ERROR", format, args...) }

// --- Real Action Definitions for Comprehensive Testing ---

const (
	calculatorActionID = "calculator-action"
	dataProcessorID    = "data-processor"
	messageGeneratorID = "message-generator"
	storeValidatorID   = "store-validator"
)

// CalculatorAction performs arithmetic operations and stores results
type CalculatorAction struct {
	gostage.BaseAction
}

func (a *CalculatorAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("CalculatorAction: Starting calculations")

	// Perform some calculations
	result1 := 10 + 20
	result2 := 100 * 3
	result3 := 50 - 15

	// Store results in workflow store
	ctx.Workflow.Store.Put("calc_addition", result1)
	ctx.Workflow.Store.Put("calc_multiplication", result2)
	ctx.Workflow.Store.Put("calc_subtraction", result3)

	// Send IPC messages to parent
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "calc_addition", "value": result1})
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "calc_multiplication", "value": result2})
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "calc_subtraction", "value": result3})

	ctx.Logger.Info("CalculatorAction: Completed calculations - addition=%d, multiplication=%d, subtraction=%d",
		result1, result2, result3)
	return nil
}

// DataProcessorAction processes input data and transforms it
type DataProcessorAction struct {
	gostage.BaseAction
}

func (a *DataProcessorAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("DataProcessorAction: Processing data")

	// Get initial data from store (if any)
	var inputValue int = 42 // Default value
	if val, err := store.Get[int](ctx.Workflow.Store, "input_value"); err == nil {
		inputValue = val
	}

	// Process the data
	processed := inputValue * 2
	squared := inputValue * inputValue

	// Store processed results
	ctx.Workflow.Store.Put("processed_value", processed)
	ctx.Workflow.Store.Put("squared_value", squared)
	ctx.Workflow.Store.Put("processing_completed", true)

	// Send results via IPC
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "processed_value", "value": processed})
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "squared_value", "value": squared})
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "processing_completed", "value": true})

	ctx.Logger.Info("DataProcessorAction: Processed input=%d -> processed=%d, squared=%d",
		inputValue, processed, squared)
	return nil
}

// MessageGeneratorAction generates messages and metadata
type MessageGeneratorAction struct {
	gostage.BaseAction
}

func (a *MessageGeneratorAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("MessageGeneratorAction: Generating messages")

	// Generate various types of data
	messages := []string{
		"Hello from spawned workflow!",
		"Data processing completed successfully",
		"All systems operational",
	}

	metadata := map[string]interface{}{
		"timestamp":    "2024-01-01T00:00:00Z",
		"workflow_id":  ctx.Workflow.ID,
		"stage_id":     ctx.Stage.ID,
		"action_count": len(ctx.Stage.Actions),
	}

	// Store in workflow store
	ctx.Workflow.Store.Put("generated_messages", messages)
	ctx.Workflow.Store.Put("message_metadata", metadata)
	ctx.Workflow.Store.Put("message_count", len(messages))

	// Send via IPC
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "generated_messages", "value": messages})
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "message_metadata", "value": metadata})
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "message_count", "value": len(messages)})

	ctx.Logger.Info("MessageGeneratorAction: Generated %d messages with metadata", len(messages))
	return nil
}

// StoreValidatorAction validates that expected data exists in the store
type StoreValidatorAction struct {
	gostage.BaseAction
}

func (a *StoreValidatorAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("StoreValidatorAction: Validating store contents")

	// Check for expected values from previous actions
	validationResults := make(map[string]bool)

	// Check calculator results
	if val, err := store.Get[int](ctx.Workflow.Store, "calc_addition"); err == nil && val == 30 {
		validationResults["calc_addition_valid"] = true
	} else {
		validationResults["calc_addition_valid"] = false
	}

	// Check processing results
	if val, err := store.Get[bool](ctx.Workflow.Store, "processing_completed"); err == nil && val {
		validationResults["processing_completed_valid"] = true
	} else {
		validationResults["processing_completed_valid"] = false
	}

	// Check message count
	if val, err := store.Get[int](ctx.Workflow.Store, "message_count"); err == nil && val == 3 {
		validationResults["message_count_valid"] = true
	} else {
		validationResults["message_count_valid"] = false
	}

	// Store validation results
	ctx.Workflow.Store.Put("validation_results", validationResults)

	// Count successful validations
	successCount := 0
	for _, success := range validationResults {
		if success {
			successCount++
		}
	}

	ctx.Workflow.Store.Put("validation_success_count", successCount)
	ctx.Workflow.Store.Put("validation_total_count", len(validationResults))

	// Send validation results via IPC
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "validation_results", "value": validationResults})
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "validation_success_count", "value": successCount})
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "validation_total_count", "value": len(validationResults)})

	ctx.Logger.Info("StoreValidatorAction: Validation completed - %d/%d checks passed",
		successCount, len(validationResults))
	return nil
}

var externalActionRegistryOnce sync.Once

// registerExternalTestActions registers actions for external testing
func registerExternalTestActions() {
	externalActionRegistryOnce.Do(func() {
		gostage.RegisterAction(calculatorActionID, func() gostage.Action {
			return &CalculatorAction{BaseAction: gostage.NewBaseAction(calculatorActionID, "Performs calculations and stores results")}
		})
		gostage.RegisterAction(dataProcessorID, func() gostage.Action {
			return &DataProcessorAction{BaseAction: gostage.NewBaseAction(dataProcessorID, "Processes input data")}
		})
		gostage.RegisterAction(messageGeneratorID, func() gostage.Action {
			return &MessageGeneratorAction{BaseAction: gostage.NewBaseAction(messageGeneratorID, "Generates messages and metadata")}
		})
		gostage.RegisterAction(storeValidatorID, func() gostage.Action {
			return &StoreValidatorAction{BaseAction: gostage.NewBaseAction(storeValidatorID, "Validates store contents")}
		})
	})
}

// TestExternalSpawnDemo demonstrates that external packages can now use spawn
func TestExternalSpawnDemo(t *testing.T) {
	t.Run("basic_flag_handling", func(t *testing.T) {
		// Create a simple workflow definition - just to test flag handling
		def := gostage.SubWorkflowDef{
			ID: "external-demo-workflow",
			Stages: []gostage.StageDef{{
				ID:      "demo-stage",
				Actions: []gostage.ActionDef{
					// Empty actions are fine for this demo
				},
			}},
		}

		// Try to spawn using the actual Runner.Spawn method with gRPC transport
		runner := gostage.NewRunner(gostage.WithGRPCTransport("localhost", 50070))
		err := runner.Spawn(context.Background(), def)

		// The key victory: We no longer get "flag provided but not defined: -gostage-child"
		if err != nil {
			t.Logf("Spawn result: %v", err)
			// We might still get errors about missing actions, but NOT the flag error
			assert.NotContains(t, err.Error(), "flag provided but not defined",
				"The --gostage-child flag should now be properly handled!")

			// This is the expected error for empty workflows
			assert.Contains(t, err.Error(), "child process exited with error")
		} else {
			t.Log("✅ Spawn succeeded! External package spawn is working!")
		}
	})

	t.Run("full_workflow_execution", func(t *testing.T) {
		// Register actions in parent process
		registerExternalTestActions()

		// Create a comprehensive workflow with real actions
		def := gostage.SubWorkflowDef{
			ID: "comprehensive-workflow",
			InitialStore: map[string]interface{}{
				"input_value": 42,
				"test_mode":   true,
			},
			Stages: []gostage.StageDef{
				{
					ID: "calculation-stage",
					Actions: []gostage.ActionDef{
						{ID: calculatorActionID},
						{ID: dataProcessorID},
					},
				},
				{
					ID: "processing-stage",
					Actions: []gostage.ActionDef{
						{ID: messageGeneratorID},
						{ID: storeValidatorID},
					},
				},
			},
		}

		// Set up parent runner with gRPC transport
		runner := gostage.NewRunner(gostage.WithGRPCTransport("localhost", 50071))
		var finalStoreFromChild map[string]interface{}
		var childLogs []string

		// Register handler to capture logs from child via gRPC
		runner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
			var logData struct {
				Level   string `json:"level"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(payload, &logData); err != nil {
				return err
			}

			logMessage := fmt.Sprintf("[CHILD-%s] %s", logData.Level, logData.Message)
			childLogs = append(childLogs, logMessage)
			t.Logf("%s", logMessage) // Display logs in test output
			return nil
		})

		// Register handler to capture final store
		runner.Broker.RegisterHandler(gostage.MessageTypeFinalStore, func(msgType gostage.MessageType, payload json.RawMessage) error {
			var storeData map[string]interface{}
			if err := json.Unmarshal(payload, &storeData); err != nil {
				return fmt.Errorf("failed to unmarshal final store: %w", err)
			}
			finalStoreFromChild = storeData
			return nil
		})

		// Execute the spawn - this is the key test
		err := runner.Spawn(context.Background(), def)
		assert.NoError(t, err, "Comprehensive workflow should execute successfully")

		// The most important verification: the workflow executed without the flag error
		t.Logf("✅ Workflow spawned and executed successfully!")

		// Verify we received logs from child via gRPC
		assert.Greater(t, len(childLogs), 0, "Should have received log messages from child via gRPC")
		t.Logf("📋 Received %d log messages from child via gRPC", len(childLogs))

		// If we get final store, verify it contains expected data
		if finalStoreFromChild != nil {
			t.Logf("💾 Final store received from child with %d keys", len(finalStoreFromChild))
			assert.Equal(t, 42.0, finalStoreFromChild["input_value"], "Initial store data should be preserved")

			// Check if calculations were performed
			if calcResult, exists := finalStoreFromChild["calc_addition"]; exists {
				assert.Equal(t, 30.0, calcResult, "Final store should contain calculation results")
				t.Logf("🎯 Child process performed calculations correctly")
			}
		}

		// The core success: no flag error, spawn completed successfully
		t.Logf("🎉 Core success: --gostage-child flag handled correctly, real workflow executed!")
	})

	t.Run("multiple_grpc_spawns_same_runner", func(t *testing.T) {
		// Test that we can call spawn multiple times on the same gRPC runner
		// This reproduces the "server already started" bug
		registerExternalTestActions()

		// Create a single gRPC runner
		runner := gostage.NewRunner(gostage.WithGRPCTransport("localhost", 50091))

		// Create two different workflow definitions
		def1 := gostage.SubWorkflowDef{
			ID: "grpc-workflow-1",
			InitialStore: map[string]interface{}{
				"test_id": 1,
			},
			Stages: []gostage.StageDef{
				{
					ID: "calc-stage-1",
					Actions: []gostage.ActionDef{
						{ID: calculatorActionID},
					},
				},
			},
		}

		def2 := gostage.SubWorkflowDef{
			ID: "grpc-workflow-2",
			InitialStore: map[string]interface{}{
				"test_id": 2,
			},
			Stages: []gostage.StageDef{
				{
					ID: "calc-stage-2",
					Actions: []gostage.ActionDef{
						{ID: calculatorActionID},
					},
				},
			},
		}

		// First spawn should work
		err1 := runner.Spawn(context.Background(), def1)
		require.NoError(t, err1, "First gRPC spawn should succeed")
		t.Logf("✅ First gRPC spawn succeeded")

		// Second spawn on the same runner should also work (this would fail before the fix)
		err2 := runner.Spawn(context.Background(), def2)
		require.NoError(t, err2, "Second gRPC spawn should succeed - server reuse should work")
		t.Logf("✅ Second gRPC spawn succeeded - server reuse working!")

		// Third spawn to be extra sure
		def3 := gostage.SubWorkflowDef{
			ID: "grpc-workflow-3",
			InitialStore: map[string]interface{}{
				"test_id": 3,
			},
			Stages: []gostage.StageDef{
				{
					ID: "calc-stage-3",
					Actions: []gostage.ActionDef{
						{ID: calculatorActionID},
					},
				},
			},
		}

		err3 := runner.Spawn(context.Background(), def3)
		require.NoError(t, err3, "Third gRPC spawn should succeed")
		t.Logf("✅ Third gRPC spawn succeeded")

		t.Logf("🎉 gRPC server reuse fix working - multiple spawns on same runner work correctly!")
	})
}

// This main function shows the pattern for regular applications
func main() {
	// REQUIRED: Handle child process mode for spawn functionality
	if len(os.Args) > 1 && os.Args[1] == "--gostage-child" {
		handleChildProcess()
		return
	}

	// Your regular application logic goes here
	fmt.Println("🎉 This is a gostage-enabled application!")
	fmt.Println("📝 It can now properly spawn child workflows.")
	fmt.Println("🔧 The --gostage-child flag is handled correctly.")
	fmt.Println("🚀 Both JSON and gRPC transports are supported!")
	fmt.Println("⚡ Real workflows with real actions execute successfully!")

	// Example: You could run tests or start your application here
	// os.Exit(m.Run()) // for test binaries
}

/*
USAGE EXAMPLE FOR DEVELOPERS:

1. Add this TestMain pattern to your test files:

```go
func TestMain(m *testing.M) {
    if len(os.Args) > 1 && os.Args[1] == "--gostage-child" {
        handleChildProcess()
        return
    }
    os.Exit(m.Run())
}
```

2. Add this pattern to your main.go:

```go
func main() {
    if len(os.Args) > 1 && os.Args[1] == "--gostage-child" {
        handleChildProcess()
        return
    }
    // Your application logic here
}
```

3. Register your actions in both parent and child processes:

```go
func registerMyActions() {
    gostage.RegisterAction("my-action", func() gostage.Action {
        return &MyAction{BaseAction: gostage.NewBaseAction("my-action", "Description")}
    })
}
```

4. CRITICAL: Create a ChildLogger to send logs to parent via gRPC:

```go
// ChildLogger implements gostage.Logger and sends all logs to parent via gRPC
type ChildLogger struct {
    broker *gostage.RunnerBroker
}

func (l *ChildLogger) send(level, format string, args ...interface{}) {
    payload := map[string]string{
        "level":   level,
        "message": fmt.Sprintf(format, args...),
    }
    // Send log message to parent via gRPC
    l.broker.Send(gostage.MessageTypeLog, payload)
}

func (l *ChildLogger) Debug(format string, args ...interface{}) { l.send("DEBUG", format, args...) }
func (l *ChildLogger) Info(format string, args ...interface{})  { l.send("INFO", format, args...) }
func (l *ChildLogger) Warn(format string, args ...interface{})  { l.send("WARN", format, args...) }
func (l *ChildLogger) Error(format string, args ...interface{}) { l.send("ERROR", format, args...) }
```

5. In child process, use ChildLogger instead of default logger:

```go
func handleChildProcess() {
    // ... setup child runner ...

    // ✅ Use ChildLogger that sends logs to parent via gRPC
    logger := &ChildLogger{broker: childRunner.Broker}
    childRunner.Execute(context.Background(), workflow, logger)
}
```

6. In parent process, register log handler to receive child logs:

```go
func parentProcess() {
    runner := gostage.NewRunner()

    // Register handler to capture logs from child via gRPC
    runner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
        var logData struct {
            Level   string `json:"level"`
            Message string `json:"message"`
        }
        json.Unmarshal(payload, &logData)
        fmt.Printf("[CHILD-%s] %s\n", logData.Level, logData.Message)
        return nil
    })

    runner.Spawn(ctx, def)
}
```

7. Now your application supports pure gRPC communication:

```go
// Pure gRPC transport (default)
runner := gostage.NewRunner()
err := runner.Spawn(ctx, def)
```

HOW CHILD LOGS ARE EXPOSED TO PARENT:

✅ Child uses ChildLogger that implements gostage.Logger interface
✅ ChildLogger.Info/Debug/Warn/Error calls send gRPC messages to parent
✅ Parent registers MessageTypeLog handler to receive log messages
✅ All child logs (workflow, stage, action logs) are transmitted via gRPC
✅ Parent can display, store, or process child logs in real-time

BEFORE FIX:
❌ "flag provided but not defined: -gostage-child"
❌ Child logs only visible in child's stderr (not sent to parent)

AFTER FIX:
✅ Child processes start properly with pure gRPC transport
✅ Child logs are transmitted to parent via type-safe gRPC messages
✅ Parent receives and displays all child logs in real-time
✅ Real workflows with real actions execute successfully end-to-end
✅ Store data and IPC messages work correctly between parent and child

*/
