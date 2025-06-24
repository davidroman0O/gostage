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

	"github.com/davidroman0O/gostage"
)

// This TestMain function enables the self-spawning pattern.
func TestMain(m *testing.M) {
	if os.Getenv("GOSTAGE_EXEC_CHILD") == "1" {
		childMain()
		return
	}
	fmt.Println("--- Running Parent Process (Action Registry Example) ---")
	main()
}

// --- Action Definition and Registration ---

const (
	RegisteredActionID = "my-registered-action"
)

// RegisteredAction is a custom action we will register.
type RegisteredAction struct {
	gostage.BaseAction
}

func (a *RegisteredAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("  -> The registered action is executing successfully in the child process!")
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{"key": "registry.worked", "value": true})
	return nil
}

// registerActions contains all action registrations.
// It's crucial that this is called by both parent and child processes.
func registerActions() {
	gostage.RegisterAction(RegisteredActionID, func() gostage.Action {
		return &RegisteredAction{
			BaseAction: gostage.NewBaseAction(RegisteredActionID, "An action created from the registry."),
		}
	})
}

// --- Child Process Logic ---

// ChildLogger sends log messages over the communication broker.
type ChildLogger struct {
	broker *gostage.RunnerBroker
}

func (l *ChildLogger) send(level, format string, args ...interface{}) {
	payload := map[string]string{"level": level, "message": fmt.Sprintf(format, args...)}
	l.broker.Send(gostage.MessageTypeLog, payload)
}
func (l *ChildLogger) Info(format string, args ...interface{})  { l.send("INFO", format, args...) }
func (l *ChildLogger) Debug(format string, args ...interface{}) {}
func (l *ChildLogger) Warn(format string, args ...interface{})  {}
func (l *ChildLogger) Error(format string, args ...interface{}) {}

func childMain() {
	// 1. Register the actions. The child needs to know how to build the action from its ID.
	registerActions()

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

	// 2. Reconstruct the workflow. NewWorkflowFromDef will use the action registry.
	wf, err := gostage.NewWorkflowFromDef(workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: failed to create workflow from def: %v\n", err)
		os.Exit(1)
	}

	brokerLogger := &ChildLogger{broker: childRunner.Broker}

	// 3. Execute the workflow.
	if err := childRunner.Execute(context.Background(), wf, brokerLogger); err != nil {
		fmt.Fprintf(os.Stderr, "child: workflow execution failed: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// --- Parent Process Logic ---

func main() {
	// 1. Register the actions in the parent process as well.
	registerActions()

	parentRunner := gostage.NewRunner()
	registryWorked := false
	var wg sync.WaitGroup

	// 2. Set up a handler to listen for messages from the child.
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var data struct {
			Key   string `json:"key"`
			Value bool   `json:"value"`
		}
		if err := json.Unmarshal(payload, &data); err == nil {
			if data.Key == "registry.worked" {
				registryWorked = data.Value
			}
		}
		return nil
	})
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var logData struct{ Message string }
		json.Unmarshal(payload, &logData)
		fmt.Printf("[CHILD LOG] %s\n", logData.Message)
		return nil
	})

	// 3. Define the sub-workflow using the registered action's ID.
	//    We are not passing an instance of the action, only its identifier.
	subWorkflowDef := gostage.SubWorkflowDef{
		ID: "registry-test-workflow",
		Stages: []gostage.StageDef{{
			ID:      "main-stage",
			Actions: []gostage.ActionDef{{ID: RegisteredActionID}},
		}},
	}

	// 4. Spawn the child process.
	fmt.Println("Spawning child to execute workflow via registered action...")
	err := spawnTestProcess(context.Background(), parentRunner, subWorkflowDef, &wg)
	if err != nil {
		fmt.Printf("Error spawning child process: %v\n", err)
	}

	wg.Wait()
	fmt.Println("Parent process finished.")

	// 5. Verify the result.
	if registryWorked {
		fmt.Println("\n✅ SUCCESS: Child process confirmed that the registered action was executed.")
	} else {
		fmt.Println("\n❌ FAILED: Did not receive confirmation from the child process.")
	}
}

// spawnTestProcess is a helper that mimics runner.Spawn for this example.
func spawnTestProcess(ctx context.Context, r *gostage.Runner, def gostage.SubWorkflowDef, wg *sync.WaitGroup) error {
	def.Transport = &gostage.TransportConfig{Type: gostage.TransportJSON}
	defBytes, err := json.Marshal(def)
	if err != nil {
		return err
	}

	exePath, err := os.Executable()
	if err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, exePath)
	cmd.Env = append(os.Environ(), "GOSTAGE_EXEC_CHILD=1")
	childStdout, _ := cmd.StdoutPipe()
	childStdin, _ := cmd.StdinPipe()
	cmd.Stderr = os.Stderr

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
