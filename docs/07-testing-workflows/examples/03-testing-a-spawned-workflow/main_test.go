package examples

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/davidroman0O/gostage"
	"github.com/stretchr/testify/assert"
)

// TestMain acts as the entry point for the test binary. It allows the
// same binary to act as both the parent test runner and the spawned child process.
func TestMain(m *testing.M) {
	// Check for a specific environment variable.
	// The parent process (the test function) will set this variable when it
	// re-executes its own test binary to run the child logic.
	if os.Getenv("GOSTAGE_EXEC_CHILD") == "1" {
		// If the variable is set, run the dedicated child logic and exit.
		childMain()
		return
	}
	// If the variable is not set, run the tests in this file as normal.
	os.Exit(m.Run())
}

// --- Action to be executed by the child ---

const TestActionID = "test-action"

type TestAction struct {
	gostage.BaseAction
}

func (a *TestAction) Execute(ctx *gostage.ActionContext) error {
	// The child process sends a message back to the parent.
	return ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "child.executed",
		"value": true,
	})
}

// Both parent and child must register the action.
func registerTestAction() {
	gostage.RegisterAction(TestActionID, func() gostage.Action {
		return &TestAction{BaseAction: gostage.NewBaseAction(TestActionID, "")}
	})
}

// --- Child Process Logic ---

// childMain contains the code that will be executed only by the child process.
func childMain() {
	registerTestAction()

	workflowDef, err := gostage.ReadWorkflowDefinitionFromStdin()
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: failed to read workflow definition: %v\n", err)
		os.Exit(1)
	}

	childRunner, err := gostage.NewChildRunner(*workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: failed to initialize runner: %v\n", err)
		os.Exit(1)
	}

	wf, err := gostage.NewWorkflowFromDef(workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: failed to create workflow from def: %v\n", err)
		os.Exit(1)
	}

	// In a real test, you would use a broker logger to send logs back to the parent.
	// For this example, we use a nil logger for simplicity.
	if err := childRunner.Execute(context.Background(), wf, nil); err != nil {
		fmt.Fprintf(os.Stderr, "child: workflow execution failed: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// --- Parent Process Test ---

// TestSpawnedWorkflow acts as the parent process. It defines and spawns the
// sub-workflow, then waits for and verifies the result.
func TestSpawnedWorkflow(t *testing.T) {
	registerTestAction()

	// 1. Set up the parent's runner and a flag to verify child execution.
	parentRunner := gostage.NewRunner()
	var childDidExecute bool
	var wg sync.WaitGroup

	// 2. Register a handler to process messages from the child.
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var data struct {
			Key   string `json:"key"`
			Value bool   `json:"value"`
		}

		if json.Unmarshal(payload, &data) == nil && data.Key == "child.executed" {
			childDidExecute = data.Value
		}
		return nil
	})

	// 3. Define the workflow for the child to run.
	subWorkflowDef := gostage.SubWorkflowDef{
		ID: "child-wf",
		Stages: []gostage.StageDef{
			{ID: "child-stage", Actions: []gostage.ActionDef{{ID: TestActionID}}},
		},
	}

	// 4. Spawn the child process using the helper.
	err := spawnTestProcess(context.Background(), parentRunner, subWorkflowDef, &wg)
	assert.NoError(t, err, "Spawning the child process should not produce an error.")

	// 5. Wait for the listener to finish processing messages.
	wg.Wait()

	// 6. Assert that we received the confirmation message from the child.
	assert.True(t, childDidExecute, "The parent should have received a message confirming the child action ran.")
}

// spawnTestProcess is a helper that executes the test binary as a child process.
func spawnTestProcess(ctx context.Context, r *gostage.Runner, def gostage.SubWorkflowDef, wg *sync.WaitGroup) error {
	def.Transport = &gostage.TransportConfig{Type: gostage.TransportJSON}
	defBytes, err := json.Marshal(def)
	if err != nil {
		return err
	}

	exePath, err := os.Executable() // Path to the currently running test binary.
	if err != nil {
		return err
	}

	// Create a command to execute the test binary again.
	cmd := exec.CommandContext(ctx, exePath)
	// Set the environment variable that TestMain will check.
	cmd.Env = append(os.Environ(), "GOSTAGE_EXEC_CHILD=1")

	// Set up pipes for communication.
	childStdout, _ := cmd.StdoutPipe()
	childStdin, _ := cmd.StdinPipe()
	cmd.Stderr = os.Stderr // Pipe child's errors to the parent's console.

	if err := cmd.Start(); err != nil {
		return err
	}

	// Start a goroutine for the parent to listen to the child's stdout.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.Broker.Listen(childStdout); err != nil && err != io.EOF {
			fmt.Printf("Parent listener error: %v\n", err)
		}
	}()

	// Write the workflow definition to the child's stdin.
	if _, err := childStdin.Write(defBytes); err != nil {
		return err
	}
	childStdin.Close() // Close stdin to signal that we are done writing.

	return cmd.Wait() // Wait for the child process to exit.
}
