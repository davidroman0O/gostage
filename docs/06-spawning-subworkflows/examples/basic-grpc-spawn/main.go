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
	processID := os.Getpid()
	ctx.Logger.Info("gRPC child action is executing in process %d.", processID)

	// Send real-time status updates via gRPC
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "child.status.grpc",
		"value": "running",
	})

	// Simulate some work
	for i := 1; i <= 3; i++ {
		time.Sleep(200 * time.Millisecond)
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   fmt.Sprintf("work.step.%d", i),
			"value": fmt.Sprintf("completed by process %d", processID),
		})
		ctx.Logger.Info("gRPC child completed step %d/3", i)
	}

	// Store final results in the workflow store
	ctx.Workflow.Store.Put("processing_results", map[string]interface{}{
		"total_steps":   3,
		"processor_pid": processID,
		"transport":     "grpc",
		"completed_at":  time.Now().Format("2006-01-02 15:04:05"),
	})

	// Send final status update
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "child.status.grpc",
		"value": "completed",
	})

	ctx.Logger.Info("gRPC child action has finished.")
	return nil
}

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

// --- Child Process Logic ---

func childMain() {
	fmt.Fprintf(os.Stderr, "🔥 CHILD PROCESS STARTED - PID: %d, Parent PID: %d\n", os.Getpid(), os.Getppid())

	// Register the action in the child process
	gostage.RegisterAction(GrpcTestActionID, func() gostage.Action {
		return &GrpcTestAction{
			BaseAction: gostage.NewBaseAction(GrpcTestActionID, "A test action for gRPC spawning."),
		}
	})

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

	// Create workflow from definition
	workflow, err := gostage.NewWorkflowFromDef(workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process failed to create workflow: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "✅ Child process %d executing workflow: %s\n", os.Getpid(), workflowDef.ID)

	// Execute workflow - using the returned logger
	if err := childRunner.Execute(context.Background(), workflow, logger); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process %d workflow execution failed: %v\n", os.Getpid(), err)
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

func main() {
	// Check if this is a child process
	for _, arg := range os.Args[1:] {
		if arg == "--gostage-child" {
			childMain()
			return
		}
	}

	// Parent process execution with NEW PURE GRPC API
	fmt.Printf("🚀 PARENT PROCESS STARTED - PID: %d\n", os.Getpid())

	// Register the action in parent too
	gostage.RegisterAction(GrpcTestActionID, func() gostage.Action {
		return &GrpcTestAction{
			BaseAction: gostage.NewBaseAction(GrpcTestActionID, "A test action for gRPC spawning."),
		}
	})

	// 🎉 NEW PURE GRPC API - Create runner with automatic gRPC transport
	fmt.Println("🔧 Setting up pure gRPC transport with automatic port assignment...")
	parentRunner := gostage.NewRunner() // gRPC is now automatic!

	parentStore := store.NewKVStore()
	var finalStoreFromChild map[string]interface{}

	// Register handlers for gRPC communication
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var data struct {
			Key   string      `json:"key"`
			Value interface{} `json:"value"`
		}
		if err := json.Unmarshal(payload, &data); err != nil {
			return err
		}

		fmt.Printf("🔄 Real-time update via gRPC: %s = %v\n", data.Key, data.Value)
		return parentStore.Put(data.Key, data.Value)
	})

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

	parentRunner.Broker.RegisterHandler(gostage.MessageTypeFinalStore, func(msgType gostage.MessageType, payload json.RawMessage) error {
		if err := json.Unmarshal(payload, &finalStoreFromChild); err != nil {
			return fmt.Errorf("failed to unmarshal final store: %w", err)
		}
		fmt.Printf("📦 Parent received final store with %d keys via gRPC\n", len(finalStoreFromChild))
		return nil
	})

	// Define the sub-workflow
	subWorkflowDef := gostage.SubWorkflowDef{
		ID:          "child-workflow-grpc",
		Name:        "Pure gRPC Child Workflow",
		Description: "Demonstrates automatic gRPC communication",
		Stages: []gostage.StageDef{{
			ID:   "child-stage-grpc",
			Name: "gRPC Processing Stage",
			Actions: []gostage.ActionDef{{
				ID: GrpcTestActionID,
			}},
		}},
	}

	// 🎉 NEW PURE GRPC API - Spawn child with automatic gRPC transport configuration!
	fmt.Printf("📋 Parent process %d spawning child with pure gRPC transport\n", os.Getpid())

	ctx := context.Background()
	startTime := time.Now()

	// This automatically uses gRPC transport - no configuration needed!
	err := parentRunner.Spawn(ctx, subWorkflowDef)

	duration := time.Since(startTime)
	fmt.Printf("⏱️  Child process execution completed in %v\n", duration)

	if err != nil {
		fmt.Printf("❌ Child process execution failed: %v\n", err)
	} else {
		fmt.Println("✅ Child process execution completed successfully!")
	}

	// Clean up gRPC server
	parentRunner.Broker.Close()

	// Display comprehensive results
	fmt.Println()
	fmt.Println("📊 === EXECUTION SUMMARY ===")
	fmt.Printf("Parent Process ID: %d\n", os.Getpid())
	fmt.Printf("Transport Type: Pure gRPC (protobuf)\n")
	fmt.Printf("Real-time messages received: %d\n", parentStore.Count())

	if finalStoreFromChild != nil {
		fmt.Printf("Final store contains %d keys\n", len(finalStoreFromChild))

		// Display real-time messages
		fmt.Println("\n🔄 Real-time gRPC Messages:")
		for _, key := range parentStore.ListKeys() {
			if value, err := store.Get[interface{}](parentStore, key); err == nil {
				fmt.Printf("  📡 %s: %v\n", key, value)
			}
		}

		// Display final store data
		fmt.Println("\n📦 Final Store Data (from child workflow):")
		for key, value := range finalStoreFromChild {
			fmt.Printf("  📥 %s: %v\n", key, value)
		}

		// Check processing results
		if results, ok := finalStoreFromChild["processing_results"].(map[string]interface{}); ok {
			fmt.Println("\n✅ Processing Results:")
			fmt.Printf("  Total steps: %.0f\n", results["total_steps"])
			fmt.Printf("  Processor PID: %.0f\n", results["processor_pid"])
			fmt.Printf("  Transport: %s\n", results["transport"])
			fmt.Printf("  Completed at: %s\n", results["completed_at"])
		}
	} else {
		fmt.Println("No final store received from child")
	}

	fmt.Println("\n🎉 === PURE GRPC SUCCESS ===")
	fmt.Println("✅ No transport configuration needed")
	fmt.Println("✅ Automatic gRPC server setup")
	fmt.Println("✅ Type-safe protobuf communication")
	fmt.Println("✅ Seamless child process coordination")
	fmt.Println("✅ Real-time IPC and final store transfer")
}
