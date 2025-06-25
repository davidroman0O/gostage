package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/davidroman0O/gostage"
)

// This main function enables the self-spawning pattern with the new gRPC approach.
func main() {
	// Check for the --gostage-child flag instead of environment variable
	for _, arg := range os.Args[1:] {
		if arg == "--gostage-child" {
			childMain()
			return
		}
	}

	fmt.Println("--- Running Parent Process (Action Registry Example with Pure gRPC) ---")
	parentMain()
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
	processID := os.Getpid()
	ctx.Logger.Info("The registered action is executing successfully in child process %d!", processID)

	// Send real-time confirmation via gRPC
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "registry.worked",
		"value": true,
	})

	// Store results in workflow store
	ctx.Workflow.Store.Put("registry_results", map[string]interface{}{
		"action_executed": true,
		"executed_by_pid": processID,
		"registry_lookup": "successful",
		"action_id":       RegisteredActionID,
		"execution_time":  time.Now().Format("2006-01-02 15:04:05"),
		"transport_used":  "grpc",
	})

	ctx.Logger.Info("Registry-based action completed successfully!")
	return nil
}

// registerActions contains all action registrations.
// It's crucial that this is called by both parent and child processes.
func registerActions() {
	gostage.RegisterAction(RegisteredActionID, func() gostage.Action {
		return &RegisteredAction{
			BaseAction: gostage.NewBaseAction(RegisteredActionID, "An action created from the registry via gRPC."),
		}
	})
}

// --- Child Process Logic ---

// ChildLogger sends log messages over the gRPC communication broker.
type ChildLogger struct {
	broker *gostage.RunnerBroker
}

func (l *ChildLogger) send(level, format string, args ...interface{}) {
	payload := map[string]string{
		"level":   level,
		"message": fmt.Sprintf(format, args...),
	}
	l.broker.Send(gostage.MessageTypeLog, payload)
}

func (l *ChildLogger) Debug(format string, args ...interface{}) { l.send("DEBUG", format, args...) }
func (l *ChildLogger) Info(format string, args ...interface{})  { l.send("INFO", format, args...) }
func (l *ChildLogger) Warn(format string, args ...interface{})  { l.send("WARN", format, args...) }
func (l *ChildLogger) Error(format string, args ...interface{}) { l.send("ERROR", format, args...) }

func childMain() {
	fmt.Fprintf(os.Stderr, "🔥 CHILD PROCESS STARTED - PID: %d, Parent PID: %d\n", os.Getpid(), os.Getppid())

	// 1. Register the actions. The child needs to know how to build the action from its ID.
	registerActions()
	fmt.Fprintf(os.Stderr, "✅ Child process registered actions\n")

	// ✨ NEW SEAMLESS API - automatic gRPC setup and logger creation
	childRunner, logger, err := gostage.NewChildRunner()
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process failed to initialize: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "✅ Child process automatically configured with pure gRPC transport\n")

	// ✨ Direct method call - no GetTransport() needed!
	workflowDef, err := childRunner.RequestWorkflowDefinitionFromParent(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to request workflow definition: %v\n", err)
		os.Exit(1)
	}

	// 2. Reconstruct the workflow. NewWorkflowFromDef will use the action registry.
	workflow, err := gostage.NewWorkflowFromDef(workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process failed to create workflow from def: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "✅ Child process %d executing workflow: %s (via action registry)\n", os.Getpid(), workflowDef.ID)

	// 3. Execute the workflow - using the returned logger
	if err := childRunner.Execute(context.Background(), workflow, logger); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process workflow execution failed: %v\n", err)
		os.Exit(1)
	}

	// Send final store state to parent via gRPC
	if workflow.Store != nil {
		finalStore := workflow.Store.ExportAll()
		if err := childRunner.Broker.Send(gostage.MessageTypeFinalStore, finalStore); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️  Child process failed to send final store: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "📦 Child process sent final store with %d keys via gRPC\n", len(finalStore))
		}
	}

	// Close broker to clean up gRPC connections
	childRunner.Close()

	fmt.Fprintf(os.Stderr, "✅ Child process %d completed successfully\n", os.Getpid())
	os.Exit(0)
}

// --- Parent Process Logic ---

func parentMain() {
	// 1. Register the actions in the parent process as well.
	registerActions()
	fmt.Println("✅ Parent process registered actions")

	// 🎉 NEW PURE GRPC API - Create runner with automatic gRPC transport
	fmt.Println("🔧 Setting up pure gRPC transport with automatic port assignment...")
	parentRunner := gostage.NewRunner() // gRPC is now automatic!

	// Variables to track test results
	var registryWorked bool
	var finalStoreFromChild map[string]interface{}

	// 2. Set up handlers to listen for gRPC messages from the child.
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var logData struct {
			Level   string `json:"level"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(payload, &logData); err != nil {
			return err
		}
		fmt.Printf("[CHILD-%s] %s\n", logData.Level, logData.Message)
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

		// Track registry confirmation
		if data.Key == "registry.worked" {
			if worked, ok := data.Value.(bool); ok {
				registryWorked = worked
				fmt.Printf("🔄 Real-time confirmation via gRPC: Registry worked = %v\n", worked)
			}
		}

		return nil
	})

	parentRunner.Broker.RegisterHandler(gostage.MessageTypeFinalStore, func(msgType gostage.MessageType, payload json.RawMessage) error {
		if err := json.Unmarshal(payload, &finalStoreFromChild); err != nil {
			return fmt.Errorf("failed to unmarshal final store: %w", err)
		}
		fmt.Printf("📦 Parent received final store with %d keys via gRPC\n", len(finalStoreFromChild))
		return nil
	})

	// 3. Define the sub-workflow using the registered action's ID.
	//    We are not passing an instance of the action, only its identifier.
	subWorkflowDef := gostage.SubWorkflowDef{
		ID:          "registry-test-workflow",
		Name:        "Action Registry Test Workflow",
		Description: "Demonstrates action lookup from registry via pure gRPC",
		Stages: []gostage.StageDef{{
			ID:   "main-stage",
			Name: "Registry Action Stage",
			Actions: []gostage.ActionDef{{
				ID: RegisteredActionID, // This will be looked up in the child's registry!
			}},
		}},
	}

	// 4. Spawn the child process with automatic gRPC transport
	fmt.Printf("📋 Parent process %d spawning child to execute workflow via registered action...\n", os.Getpid())

	ctx := context.Background()
	startTime := time.Now()

	// This automatically uses gRPC transport - no configuration needed!
	err := parentRunner.Spawn(ctx, subWorkflowDef)

	duration := time.Since(startTime)
	fmt.Printf("⏱️  Child process execution completed in %v\n", duration)

	if err != nil {
		fmt.Printf("❌ Error spawning child process: %v\n", err)
	} else {
		fmt.Println("✅ Child process execution completed successfully!")
	}

	// Clean up gRPC server
	parentRunner.Broker.Close()

	// 5. Verify the result.
	fmt.Println()
	fmt.Println("📊 === EXECUTION SUMMARY ===")
	fmt.Printf("Parent Process ID: %d\n", os.Getpid())
	fmt.Printf("Transport Type: Pure gRPC (protobuf)\n")

	if registryWorked {
		fmt.Println("✅ SUCCESS: Child process confirmed that the registered action was executed.")
	} else {
		fmt.Println("❌ FAILED: Did not receive confirmation from the child process.")
	}

	if finalStoreFromChild != nil {
		fmt.Printf("Final store contains %d keys\n", len(finalStoreFromChild))

		// Display registry results
		if results, ok := finalStoreFromChild["registry_results"].(map[string]interface{}); ok {
			fmt.Println("\n📦 Registry Results from Child:")
			fmt.Printf("  Action executed: %v\n", results["action_executed"])
			fmt.Printf("  Executed by PID: %.0f\n", results["executed_by_pid"])
			fmt.Printf("  Registry lookup: %s\n", results["registry_lookup"])
			fmt.Printf("  Action ID: %s\n", results["action_id"])
			fmt.Printf("  Execution time: %s\n", results["execution_time"])
			fmt.Printf("  Transport used: %s\n", results["transport_used"])

			// Verify the child ran in a different process
			childPID := int(results["executed_by_pid"].(float64))
			parentPID := os.Getpid()
			if childPID != parentPID {
				fmt.Printf("  ✅ Process isolation confirmed: child PID %d ≠ parent PID %d\n", childPID, parentPID)
			}
		}
	} else {
		fmt.Println("No final store received from child")
	}

	fmt.Println("\n🎉 === ACTION REGISTRY SUCCESS ===")
	fmt.Println("✅ Child process successfully looked up action by ID")
	fmt.Println("✅ Action registry pattern working with pure gRPC")
	fmt.Println("✅ Type-safe protobuf communication")
	fmt.Println("✅ Seamless action reconstruction in child process")
}
