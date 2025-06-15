package gostage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/store"
	"github.com/stretchr/testify/assert"
)

// BrokerLogger is a gostage.Logger implementation that sends log messages
// over the communication broker instead of writing to the console.
type BrokerLogger struct {
	broker *RunnerBroker
}

// NewBrokerLogger creates a logger that sends messages via the given broker.
func NewBrokerLogger(broker *RunnerBroker) *BrokerLogger {
	return &BrokerLogger{broker: broker}
}

func (l *BrokerLogger) send(level, format string, args ...interface{}) {
	payload := map[string]string{
		"level":   level,
		"message": fmt.Sprintf(format, args...),
	}
	// We ignore the error here for simplicity in a test logger.
	// A production implementation should handle this more robustly.
	_ = l.broker.Send(MessageTypeLog, payload)
}

func (l *BrokerLogger) Debug(format string, args ...interface{}) { l.send("debug", format, args...) }
func (l *BrokerLogger) Info(format string, args ...interface{})  { l.send("info", format, args...) }
func (l *BrokerLogger) Warn(format string, args ...interface{})  { l.send("warn", format, args...) }
func (l *BrokerLogger) Error(format string, args ...interface{}) { l.send("error", format, args...) }

// This TestMain function is the key to testing the spawn functionality.
// It allows the test binary to be re-executed in a "child" mode.
func TestMain(m *testing.M) {
	// If this env var is set, we run the special child process main func.
	if os.Getenv("GOSTAGE_EXEC_CHILD") == "1" {
		childMain()
		return
	}
	// Otherwise, run the tests as normal.
	os.Exit(m.Run())
}

// childMain is the entrypoint for the spawned child process.
// It sets up a runner, reads a workflow definition from stdin, executes it,
// and communicates results back to the parent via stdout.
func childMain() {
	// 1. Create a broker that writes to standard output.
	broker := NewRunnerBroker(os.Stdout)
	// 2. Create a logger that sends all logs through this broker.
	brokerLogger := NewBrokerLogger(broker)
	// 3. Create a runner with the broker using the convenience constructor.
	childRunner := NewRunnerWithBroker(broker)

	// The child process must have the same actions registered as the parent
	// so it can instantiate them from the definition.
	registerSpawnTestActions()

	// Read the serialized workflow definition from stdin.
	defBytes, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child process failed to read from stdin: %v\n", err)
		os.Exit(1)
	}

	var def SubWorkflowDef
	if err := json.Unmarshal(defBytes, &def); err != nil {
		fmt.Fprintf(os.Stderr, "child process failed to unmarshal workflow def: %v\n", err)
		os.Exit(1)
	}

	// Reconstruct the workflow from the definition.
	wf, err := NewWorkflowFromDef(&def)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child process failed to create workflow from def: %v\n", err)
		os.Exit(1)
	}

	// Execute the reconstructed workflow.
	// Passing brokerLogger ensures the runner's default logger is never used.
	if err := childRunner.Execute(context.Background(), wf, brokerLogger); err != nil {
		// Errors here will be caught by the parent's cmd.Wait().
		// We could also send an explicit error message.
		fmt.Fprintf(os.Stderr, "child workflow execution failed: %v\n", err)
		os.Exit(1)
	}

	// Send the final store state back to the parent
	if wf.Store != nil {
		finalStore := wf.Store.ExportAll()
		if err := broker.Send(MessageTypeFinalStore, finalStore); err != nil {
			fmt.Fprintf(os.Stderr, "child process failed to send final store: %v\n", err)
			// Don't exit on this error as the workflow execution was successful
		}
	}

	// Exit successfully.
	os.Exit(0)
}

// --- Action Definitions for Testing ---

const (
	spawnTestActionID     = "spawn-test-action"
	errorTestActionID     = "error-test-action"
	storeModifierActionID = "store-modifier-action"
	panicTestActionID     = "panic-test-action"
	slowTestActionID      = "slow-test-action"
)

// SpawnTestAction is a simple action that sends messages back to the parent.
type SpawnTestAction struct{ BaseAction }

func (a *SpawnTestAction) Execute(ctx *ActionContext) error {
	ctx.Logger.Info("SpawnTestAction is executing.")
	// Send multiple store updates to prove the channel stays open.
	ctx.Send(MessageTypeStorePut, map[string]interface{}{"key": "item1", "value": "value1"})
	ctx.Send(MessageTypeStorePut, map[string]interface{}{"key": "item2", "value": 42})
	ctx.Logger.Info("SpawnTestAction has finished.")
	return nil
}

// StoreModifierAction modifies the workflow store directly and also sends IPC messages
type StoreModifierAction struct{ BaseAction }

func (a *StoreModifierAction) Execute(ctx *ActionContext) error {
	ctx.Logger.Info("StoreModifierAction is executing.")

	// Put data directly into the workflow store (this will be in the final store)
	ctx.Workflow.Store.Put("item1", "value1")
	ctx.Workflow.Store.Put("item2", 42)

	// Also send IPC messages like the original action
	ctx.Send(MessageTypeStorePut, map[string]interface{}{"key": "item1", "value": "value1"})
	ctx.Send(MessageTypeStorePut, map[string]interface{}{"key": "item2", "value": 42})

	ctx.Logger.Info("StoreModifierAction has finished.")
	return nil
}

// ErrorTestAction is an action that always returns an error to test failure propagation.
type ErrorTestAction struct{ BaseAction }

func (a *ErrorTestAction) Execute(ctx *ActionContext) error {
	ctx.Logger.Error("This action is designed to fail.")
	return fmt.Errorf("intentional action failure")
}

// PanicTestAction is an action that panics to test panic recovery.
type PanicTestAction struct{ BaseAction }

func (a *PanicTestAction) Execute(ctx *ActionContext) error {
	ctx.Logger.Info("PanicTestAction is about to panic.")
	panic("intentional panic for testing")
}

// SlowTestAction is an action that takes a long time to test timeout scenarios.
type SlowTestAction struct{ BaseAction }

func (a *SlowTestAction) Execute(ctx *ActionContext) error {
	ctx.Logger.Info("SlowTestAction is starting long operation.")
	// Sleep for a long time to test timeout handling
	select {
	case <-ctx.GoContext.Done():
		ctx.Logger.Info("SlowTestAction was cancelled.")
		return ctx.GoContext.Err()
	case <-time.After(10 * time.Second):
		ctx.Logger.Info("SlowTestAction completed.")
		return nil
	}
}

var registerOnce sync.Once

// registerSpawnTestActions registers the actions used in the test.
func registerSpawnTestActions() {
	registerOnce.Do(func() {
		RegisterAction(spawnTestActionID, func() Action {
			return &SpawnTestAction{BaseAction: NewBaseAction(spawnTestActionID, "A test action for spawning.")}
		})
		RegisterAction(errorTestActionID, func() Action {
			return &ErrorTestAction{BaseAction: NewBaseAction(errorTestActionID, "An action that fails.")}
		})
		RegisterAction(storeModifierActionID, func() Action {
			return &StoreModifierAction{BaseAction: NewBaseAction(storeModifierActionID, "An action that modifies the store.")}
		})
		RegisterAction(panicTestActionID, func() Action {
			return &PanicTestAction{BaseAction: NewBaseAction(panicTestActionID, "An action that panics.")}
		})
		RegisterAction(slowTestActionID, func() Action {
			return &SlowTestAction{BaseAction: NewBaseAction(slowTestActionID, "An action that takes a long time.")}
		})
	})
}

// --- Parent Process Tests ---

// TestSpawnWorkflow_Success tests the end-to-end process of spawning a child,
// executing a workflow, and receiving multiple messages back.
func TestSpawnWorkflow_Success(t *testing.T) {
	// Register the action that the child process will need to create.
	registerSpawnTestActions()

	// 1. Set up the parent's runner, a store for results, and slices for messages.
	parentRunner := NewRunner()
	parentStore := store.NewKVStore()
	var actionLogs []string // Only logs from our test action
	var allLogs []string    // All log messages for verification

	// 2. Register handlers on the parent's broker to process messages.
	parentRunner.Broker.RegisterHandler(MessageTypeStorePut, func(msgType MessageType, payload json.RawMessage) error {
		var data struct {
			Key   string      `json:"key"`
			Value interface{} `json:"value"`
		}
		if err := json.Unmarshal(payload, &data); err != nil {
			return err
		}
		return parentStore.Put(data.Key, data.Value)
	})
	parentRunner.Broker.RegisterHandler(MessageTypeLog, func(msgType MessageType, payload json.RawMessage) error {
		var logData struct{ Message string }
		json.Unmarshal(payload, &logData)
		allLogs = append(allLogs, logData.Message)

		// Filter for logs from our specific action
		if strings.Contains(logData.Message, "SpawnTestAction") {
			actionLogs = append(actionLogs, logData.Message)
		}
		return nil
	})

	// 3. Define the sub-workflow for the child to execute.
	subWorkflowDef := SubWorkflowDef{
		ID: "child-workflow",
		Stages: []StageDef{{
			ID: "child-stage-1",
			Actions: []ActionDef{{
				ID: spawnTestActionID, // This ID must be registered.
			}},
		}},
	}

	// 4. Spawn the child process.
	err := spawnTestProcess(context.Background(), parentRunner, subWorkflowDef)
	assert.NoError(t, err, "Spawning child process should succeed")

	// 5. Verify the parent's store was updated by all messages from the child.
	val1, _ := store.Get[string](parentStore, "item1")
	assert.Equal(t, "value1", val1, "Should receive first store update")

	val2, _ := store.Get[float64](parentStore, "item2") // JSON unmarshals numbers as float64
	assert.Equal(t, 42.0, val2, "Should receive second store update")

	// 6. Verify we received the expected number of total logs (runner generates many internal logs)
	assert.GreaterOrEqual(t, len(allLogs), 10, "Should have received multiple log messages from runner execution")

	// 7. Verify we received the specific logs from our action
	assert.Len(t, actionLogs, 2, "Should have received exactly 2 log messages from SpawnTestAction")
	assert.Contains(t, actionLogs[0], "SpawnTestAction is executing.")
	assert.Contains(t, actionLogs[1], "SpawnTestAction has finished.")
}

// TestSpawnWorkflow_WithError tests that errors in the child process are propagated to the parent.
func TestSpawnWorkflow_WithError(t *testing.T) {
	registerSpawnTestActions()

	parentRunner := NewRunner()
	var errorActionLogs []string

	// We still want to see logs from the failing child.
	parentRunner.Broker.RegisterHandler(MessageTypeLog, func(msgType MessageType, payload json.RawMessage) error {
		var logData struct{ Message string }
		json.Unmarshal(payload, &logData)

		// Filter for logs from our specific error action
		if strings.Contains(logData.Message, "This action is designed to fail") {
			errorActionLogs = append(errorActionLogs, logData.Message)
		}
		return nil
	})

	subWorkflowDef := SubWorkflowDef{
		ID: "failing-child-workflow",
		Stages: []StageDef{{
			ID: "failing-stage",
			Actions: []ActionDef{{
				ID: errorTestActionID, // This action is designed to fail.
			}},
		}},
	}

	// Spawn the child process and expect an error.
	err := spawnTestProcess(context.Background(), parentRunner, subWorkflowDef)
	assert.Error(t, err, "Spawning a failing workflow should return an error")
	assert.Contains(t, err.Error(), "child process exited with error", "Error message should indicate child process failure")

	// Verify we still received the log message that occurred before the error.
	assert.Len(t, errorActionLogs, 1, "Should have received one log message from the failing action")
	assert.Contains(t, errorActionLogs[0], "This action is designed to fail.")
}

// TestSpawnWorkflow_WithStoreHandling tests passing initial store and receiving final store
func TestSpawnWorkflow_WithStoreHandling(t *testing.T) {
	registerSpawnTestActions()

	parentRunner := NewRunner()
	var finalStoreFromChild map[string]interface{}

	// Handler to capture the final store from child
	parentRunner.Broker.RegisterHandler(MessageTypeFinalStore, func(msgType MessageType, payload json.RawMessage) error {
		var storeData map[string]interface{}
		if err := json.Unmarshal(payload, &storeData); err != nil {
			return fmt.Errorf("failed to unmarshal final store: %w", err)
		}
		finalStoreFromChild = storeData
		return nil
	})

	// Define initial store data to pass to child
	initialStore := map[string]interface{}{
		"parent_message": "Hello from parent",
		"initial_count":  100,
		"shared_data":    map[string]interface{}{"x": 1, "y": 2},
	}

	// Define sub-workflow that will use and modify the store
	subWorkflowDef := SubWorkflowDef{
		ID:           "store-test-workflow",
		InitialStore: initialStore, // Pass the initial store directly in the definition
		Stages: []StageDef{{
			ID: "store-stage",
			Actions: []ActionDef{{
				ID: storeModifierActionID, // Use the action that actually modifies the store
			}},
		}},
	}

	// Use the same spawnTestProcess function as other tests
	err := spawnTestProcess(context.Background(), parentRunner, subWorkflowDef)

	// Verify spawn was successful
	assert.NoError(t, err, "Spawn should succeed")

	// Verify we received the final store via the handler
	assert.NotNil(t, finalStoreFromChild, "Should capture final store via handler")

	// Verify initial data was passed through
	assert.Equal(t, "Hello from parent", finalStoreFromChild["parent_message"])
	assert.Equal(t, 100.0, finalStoreFromChild["initial_count"]) // JSON unmarshals numbers as float64

	// Verify the action added new data to the store
	assert.Equal(t, "value1", finalStoreFromChild["item1"])
	assert.Equal(t, 42.0, finalStoreFromChild["item2"])

	// The shared_data should still be there
	sharedData, ok := finalStoreFromChild["shared_data"].(map[string]interface{})
	assert.True(t, ok, "shared_data should be preserved")
	assert.Equal(t, 1.0, sharedData["x"])
	assert.Equal(t, 2.0, sharedData["y"])
}

// spawnTestProcess is a helper that mimics runner.Spawn but sets the
// environment variable needed to trigger the child logic in TestMain.
func spawnTestProcess(ctx context.Context, r *Runner, def SubWorkflowDef) error {
	defBytes, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("failed to serialize sub-workflow definition: %w", err)
	}

	// Get the path to the current running test binary.
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to find executable path: %w", err)
	}

	// Create the command to re-run the test binary.
	cmd := exec.CommandContext(ctx, exePath)
	// Set the special environment variable to trigger the child code path in TestMain.
	cmd.Env = append(os.Environ(), "GOSTAGE_EXEC_CHILD=1")

	// The rest of this function is identical to runner.Spawn.
	childStdin, _ := cmd.StdinPipe()
	childStdout, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start child test process: %w", err)
	}

	// Listen for messages from the child's stdout.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.Broker.Listen(childStdout); err != nil {
			// In tests, it's okay if this errors when the pipe closes, so we don't log it.
		}
	}()

	// Write the definition to the child's stdin.
	_, err = childStdin.Write(defBytes)
	if err != nil {
		// If writing fails, the child process may already be dead, so we check for an early exit.
		if cmd.ProcessState != nil {
			return fmt.Errorf("child process exited early: %s", cmd.ProcessState.String())
		}
		return fmt.Errorf("failed to write workflow definition to child: %w", err)
	}
	childStdin.Close() // Close stdin to signal EOF to the child's reader.

	// Wait for the child process to finish.
	err = cmd.Wait()

	// Wait for the listening goroutine to finish processing all messages.
	wg.Wait()

	if err != nil {
		return fmt.Errorf("child process exited with error: %w", err)
	}

	return nil
}

// TestSpawnWorkflow_WithPanic tests that panics in child processes are handled gracefully
func TestSpawnWorkflow_WithPanic(t *testing.T) {
	registerSpawnTestActions()

	parentRunner := NewRunner()
	var panicLogs []string

	// Capture logs to verify the panic was logged
	parentRunner.Broker.RegisterHandler(MessageTypeLog, func(msgType MessageType, payload json.RawMessage) error {
		var logData struct{ Message string }
		json.Unmarshal(payload, &logData)

		if strings.Contains(logData.Message, "PanicTestAction") {
			panicLogs = append(panicLogs, logData.Message)
		}
		return nil
	})

	subWorkflowDef := SubWorkflowDef{
		ID: "panicking-child-workflow",
		Stages: []StageDef{{
			ID: "panic-stage",
			Actions: []ActionDef{{
				ID: panicTestActionID,
			}},
		}},
	}

	// Spawn the child process and expect an error due to panic
	err := spawnTestProcess(context.Background(), parentRunner, subWorkflowDef)
	assert.Error(t, err, "Spawning a panicking workflow should return an error")
	assert.Contains(t, err.Error(), "child process exited with error", "Error message should indicate child process failure")

	// Verify we received the log message before the panic
	assert.GreaterOrEqual(t, len(panicLogs), 1, "Should have received at least one log message from the panicking action")
	assert.Contains(t, panicLogs[0], "PanicTestAction is about to panic.")
}

// TestSpawnWorkflow_WithTimeout tests timeout handling for slow child processes
func TestSpawnWorkflow_WithTimeout(t *testing.T) {
	registerSpawnTestActions()

	parentRunner := NewRunner()
	var slowLogs []string

	// Capture logs to verify the action started
	parentRunner.Broker.RegisterHandler(MessageTypeLog, func(msgType MessageType, payload json.RawMessage) error {
		var logData struct{ Message string }
		json.Unmarshal(payload, &logData)

		if strings.Contains(logData.Message, "SlowTestAction") {
			slowLogs = append(slowLogs, logData.Message)
		}
		return nil
	})

	subWorkflowDef := SubWorkflowDef{
		ID: "slow-child-workflow",
		Stages: []StageDef{{
			ID: "slow-stage",
			Actions: []ActionDef{{
				ID: slowTestActionID,
			}},
		}},
	}

	// Create a context with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Spawn the child process with timeout
	err := spawnTestProcess(ctx, parentRunner, subWorkflowDef)
	assert.Error(t, err, "Spawning a slow workflow with timeout should return an error")

	// The error could be either a timeout or the child process being killed
	// Both are acceptable outcomes for this test
	assert.True(t,
		strings.Contains(err.Error(), "context deadline exceeded") ||
			strings.Contains(err.Error(), "child process exited with error") ||
			strings.Contains(err.Error(), "signal: killed"),
		"Error should indicate timeout or process termination")

	// Verify the action at least started
	assert.GreaterOrEqual(t, len(slowLogs), 1, "Should have received at least one log message from the slow action")
	assert.Contains(t, slowLogs[0], "SlowTestAction is starting long operation.")
}

// TestSpawnWorkflow_WithMalformedDefinition tests handling of invalid workflow definitions
func TestSpawnWorkflow_WithMalformedDefinition(t *testing.T) {
	registerSpawnTestActions()

	parentRunner := NewRunner()

	// Create a workflow definition with a non-existent action
	subWorkflowDef := SubWorkflowDef{
		ID: "malformed-workflow",
		Stages: []StageDef{{
			ID: "malformed-stage",
			Actions: []ActionDef{{
				ID: "non-existent-action-id", // This action doesn't exist
			}},
		}},
	}

	// Spawn the child process and expect an error
	err := spawnTestProcess(context.Background(), parentRunner, subWorkflowDef)
	assert.Error(t, err, "Spawning with malformed definition should return an error")
	assert.Contains(t, err.Error(), "child process exited with error", "Error should indicate child process failure")
}

// TestSpawnWorkflow_WithEmptyWorkflow tests handling of empty workflow definitions
func TestSpawnWorkflow_WithEmptyWorkflow(t *testing.T) {
	registerSpawnTestActions()

	parentRunner := NewRunner()

	// Create an empty workflow definition
	subWorkflowDef := SubWorkflowDef{
		ID:     "empty-workflow",
		Stages: []StageDef{}, // No stages
	}

	// Spawn the child process - this should fail since workflows need at least one stage
	err := spawnTestProcess(context.Background(), parentRunner, subWorkflowDef)
	assert.Error(t, err, "Spawning an empty workflow should return an error")
	assert.Contains(t, err.Error(), "child process exited with error", "Error should indicate child process failure")
}

// TestSpawnWorkflow_WithMultipleFailures tests a workflow with multiple failing actions
func TestSpawnWorkflow_WithMultipleFailures(t *testing.T) {
	registerSpawnTestActions()

	parentRunner := NewRunner()
	var errorLogs []string

	// Capture error logs
	parentRunner.Broker.RegisterHandler(MessageTypeLog, func(msgType MessageType, payload json.RawMessage) error {
		var logData struct{ Message string }
		json.Unmarshal(payload, &logData)

		if strings.Contains(logData.Message, "designed to fail") || strings.Contains(logData.Message, "about to panic") {
			errorLogs = append(errorLogs, logData.Message)
		}
		return nil
	})

	subWorkflowDef := SubWorkflowDef{
		ID: "multi-failure-workflow",
		Stages: []StageDef{{
			ID: "failure-stage",
			Actions: []ActionDef{
				{ID: errorTestActionID}, // This will fail first
				{ID: panicTestActionID}, // This won't be reached due to first failure
			},
		}},
	}

	// Spawn the child process and expect an error from the first failing action
	err := spawnTestProcess(context.Background(), parentRunner, subWorkflowDef)
	assert.Error(t, err, "Spawning a workflow with multiple failures should return an error")
	assert.Contains(t, err.Error(), "child process exited with error", "Error should indicate child process failure")

	// Should only see the first error, not the panic (since execution stops at first failure)
	assert.GreaterOrEqual(t, len(errorLogs), 1, "Should have received at least one error log")
	assert.Contains(t, errorLogs[0], "designed to fail")

	// Should NOT see the panic message since execution should stop at first failure
	panicFound := false
	for _, log := range errorLogs {
		if strings.Contains(log, "about to panic") {
			panicFound = true
			break
		}
	}
	assert.False(t, panicFound, "Should not reach the panic action due to earlier failure")
}
