package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/davidroman0O/gostage"
)

// TestMain handles the dual nature of this example (parent + child process)
func TestMain(m *testing.M) {
	// Check for the --gostage-child flag to run as child process
	for _, arg := range os.Args[1:] {
		if arg == "--gostage-child" {
			childMain()
			return
		}
	}

	// Run as parent process
	fmt.Println("🚀 Context Messaging Demonstration")
	main()
}

// Demo actions that showcase context messaging
type DataProcessorAction struct {
	gostage.BaseAction
}

func (a *DataProcessorAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("Processing data with context messaging...")

	// Send standard store updates with automatic context
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "processing_status",
		"value": "in_progress",
	})

	// Simulate some work
	for i := 1; i <= 3; i++ {
		time.Sleep(100 * time.Millisecond)

		// Each message automatically includes workflow, stage, action context
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   fmt.Sprintf("step_%d_completed", i),
			"value": true,
		})

		ctx.Logger.Info("Completed processing step %d/3", i)
	}

	// Send completion with custom context
	customContext := map[string]interface{}{
		"workflow_id": "custom-workflow-override", // Override for demonstration
	}
	ctx.SendWithCustomContext(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "processing_status",
		"value": "completed",
	}, customContext)

	return nil
}

type NotificationAction struct {
	gostage.BaseAction
}

func (a *NotificationAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("Sending notifications...")

	// Send different types of notifications
	notifications := []string{
		"Processing started",
		"Data validation complete",
		"Results ready for review",
	}

	for i, notification := range notifications {
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   fmt.Sprintf("notification_%d", i+1),
			"value": notification,
		})

		ctx.Logger.Info("Sent notification: %s", notification)
		time.Sleep(50 * time.Millisecond)
	}

	return nil
}

// Register our demo actions
func registerDemoActions() {
	gostage.RegisterAction("data-processor", func() gostage.Action {
		return &DataProcessorAction{BaseAction: gostage.NewBaseAction("data-processor", "Processes data with context messaging")}
	})

	gostage.RegisterAction("notification-sender", func() gostage.Action {
		return &NotificationAction{BaseAction: gostage.NewBaseAction("notification-sender", "Sends notifications")}
	})
}

// Child process logic
func childMain() {
	fmt.Fprintf(os.Stderr, "🔧 Child process %d starting...\n", os.Getpid())

	// Register actions that the child will need
	registerDemoActions()

	// Create child runner with context messaging
	childRunner, logger, err := gostage.NewChildRunner()
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to create child runner: %v\n", err)
		os.Exit(1)
	}

	// Request workflow definition from parent
	workflowDef, err := childRunner.RequestWorkflowDefinitionFromParent(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to get workflow definition: %v\n", err)
		os.Exit(1)
	}

	// Create and execute workflow
	workflow, err := gostage.NewWorkflowFromDef(workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to create workflow: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "✅ Child process executing workflow: %s\n", workflowDef.ID)

	if err := childRunner.Execute(context.Background(), workflow, logger); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Workflow execution failed: %v\n", err)
		os.Exit(1)
	}

	// Send final store to parent
	if workflow.Store != nil {
		finalStore := workflow.Store.ExportAll()
		if err := childRunner.Broker.Send(gostage.MessageTypeFinalStore, finalStore); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️  Failed to send final store: %v\n", err)
		}
	}

	childRunner.Close()
	fmt.Fprintf(os.Stderr, "✅ Child process %d completed\n", os.Getpid())
	os.Exit(0)
}

// Parent process logic
func main() {
	registerDemoActions()

	// Create parent runner with gRPC transport
	parentRunner := gostage.NewRunner(gostage.WithGRPCTransport("localhost", 50080))

	// Demonstration 1: Global Handler with Context (receives context metadata)
	parentRunner.Broker.RegisterHandlerWithContext(gostage.MessageTypeStorePut,
		func(msgType gostage.MessageType, payload json.RawMessage, context gostage.MessageContext) error {
			var data map[string]interface{}
			json.Unmarshal(payload, &data)

			fmt.Printf("📨 [GLOBAL] Message from PID:%d, Workflow:%s, Stage:%s, Action:%s, Seq:%d\n",
				context.ProcessID, context.WorkflowID, context.StageID, context.ActionName, context.SequenceNumber)
			fmt.Printf("    📦 %s = %v\n", data["key"], data["value"])

			return nil
		})

	// Demonstration 2: Workflow-Specific Handler
	parentRunner.Broker.RegisterWorkflowHandler(gostage.MessageTypeStorePut, "context-messaging-demo",
		func(msgType gostage.MessageType, payload json.RawMessage, context gostage.MessageContext) error {
			var data map[string]interface{}
			json.Unmarshal(payload, &data)

			fmt.Printf("🎯 [WORKFLOW-SPECIFIC] Context messaging workflow: %s = %v (from %s)\n",
				data["key"], data["value"], context.ActionName)

			return nil
		})

	// Demonstration 3: Stage-Specific Handler
	parentRunner.Broker.RegisterStageHandler(gostage.MessageTypeStorePut, "context-messaging-demo", "processing-stage",
		func(msgType gostage.MessageType, payload json.RawMessage, context gostage.MessageContext) error {
			var data map[string]interface{}
			json.Unmarshal(payload, &data)

			fmt.Printf("🎭 [STAGE-SPECIFIC] Processing stage activity: %s = %v\n", data["key"], data["value"])

			return nil
		})

	// Demonstration 4: Action-Specific Handler
	parentRunner.Broker.RegisterActionHandler(gostage.MessageTypeStorePut, "context-messaging-demo", "notification-stage", "notification-sender",
		func(msgType gostage.MessageType, payload json.RawMessage, context gostage.MessageContext) error {
			var data map[string]interface{}
			json.Unmarshal(payload, &data)

			fmt.Printf("🔔 [ACTION-SPECIFIC] Notification from notification-sender: %s = %v\n", data["key"], data["value"])

			return nil
		})

	// Demonstration 5: Log message handler with context
	parentRunner.Broker.RegisterHandlerWithContext(gostage.MessageTypeLog,
		func(msgType gostage.MessageType, payload json.RawMessage, context gostage.MessageContext) error {
			var logData map[string]string
			json.Unmarshal(payload, &logData)

			processInfo := ""
			if context.IsChildProcess {
				processInfo = fmt.Sprintf("[CHILD:%d]", context.ProcessID)
			} else {
				processInfo = fmt.Sprintf("[PARENT:%d]", context.ProcessID)
			}

			fmt.Printf("📝 %s [%s] %s->%s->%s: %s\n",
				processInfo, logData["level"], context.WorkflowID, context.StageID, context.ActionName, logData["message"])

			return nil
		})

	// Demonstration 6: Final store handler
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeFinalStore, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var storeData map[string]interface{}
		json.Unmarshal(payload, &storeData)

		fmt.Printf("🏁 [FINAL STORE] Received final store with %d keys:\n", len(storeData))
		for key, value := range storeData {
			fmt.Printf("    📦 %s = %v\n", key, value)
		}

		return nil
	})

	// Create the context messaging demonstration workflow
	workflowDef := gostage.SubWorkflowDef{
		ID:          "context-messaging-demo",
		Name:        "Context Messaging Demonstration",
		Description: "Demonstrates comprehensive message context and targeted handlers",
		Stages: []gostage.StageDef{
			{
				ID:   "processing-stage",
				Name: "Data Processing",
				Actions: []gostage.ActionDef{
					{ID: "data-processor"},
				},
			},
			{
				ID:   "notification-stage",
				Name: "Notification Sending",
				Actions: []gostage.ActionDef{
					{ID: "notification-sender"},
				},
			},
		},
	}

	// Execute the demonstration
	fmt.Println()
	fmt.Println("🎬 Starting Context Messaging Demonstration...")
	fmt.Println("   This will show:")
	fmt.Println("   • Automatic context metadata in all messages")
	fmt.Println("   • Global handlers that receive context information")
	fmt.Println("   • Workflow-specific message handlers")
	fmt.Println("   • Stage-specific message handlers")
	fmt.Println("   • Action-specific message handlers")
	fmt.Println("   • Process identification (parent vs child)")
	fmt.Println("   • Message sequencing and session tracking")
	fmt.Println()

	startTime := time.Now()
	err := parentRunner.Spawn(context.Background(), workflowDef)
	duration := time.Since(startTime)

	if err != nil {
		fmt.Printf("❌ Context messaging demo failed: %v\n", err)
	} else {
		fmt.Printf("✅ Context messaging demo completed successfully in %v!\n", duration)
	}

	// Clean up
	parentRunner.Broker.Close()

	fmt.Println()
	fmt.Println("🎯 Key Features Demonstrated:")
	fmt.Println("✓ Automatic context injection (workflow, stage, action, PID)")
	fmt.Println("✓ Message sequencing and session tracking")
	fmt.Println("✓ Global handlers with context metadata")
	fmt.Println("✓ Workflow-specific handlers")
	fmt.Println("✓ Stage-specific handlers")
	fmt.Println("✓ Action-specific handlers")
	fmt.Println("✓ Child vs parent process identification")
	fmt.Println("✓ Custom context override capabilities")
	fmt.Println()
	fmt.Println("🔧 This context messaging system provides:")
	fmt.Println("• Rich context metadata (workflow/stage/action/PID)")
	fmt.Println("• Targeted message handling")
	fmt.Println("• Session tracking and message sequencing")
	fmt.Println("• Same simple ctx.Send() API")
}
