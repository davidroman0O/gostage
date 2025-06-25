package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// DataProcessorAction demonstrates comprehensive store manipulation with gRPC
type DataProcessorAction struct {
	gostage.BaseAction
}

func (a *DataProcessorAction) Execute(ctx *gostage.ActionContext) error {
	processID := os.Getpid()
	ctx.Logger.Info("DataProcessor running in process %d (gRPC mode)", processID)

	// Send real-time process info via gRPC IPC (type-safe!)
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "realtime_child_pid",
		"value": processID,
	})

	// Read initial configuration from store
	if config, err := store.Get[map[string]interface{}](ctx.Workflow.Store, "config"); err == nil {
		ctx.Logger.Info("Processing with config via gRPC: %+v", config)

		// Send config confirmation via gRPC IPC
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "grpc_config_received",
			"value": true,
		})

		// Process based on config
		if mode, ok := config["mode"].(string); ok {
			switch mode {
			case "development":
				ctx.Workflow.Store.Put("debug_enabled", true)
				ctx.Workflow.Store.Put("log_level", "debug")
				ctx.Workflow.Store.Put("transport_mode", "grpc")
			case "production":
				ctx.Workflow.Store.Put("debug_enabled", false)
				ctx.Workflow.Store.Put("log_level", "info")
				ctx.Workflow.Store.Put("transport_mode", "grpc")
			}
		}
	}

	// Read and process input data
	if inputData, err := store.Get[[]interface{}](ctx.Workflow.Store, "input_data"); err == nil {
		ctx.Logger.Info("Processing %d items via gRPC transport", len(inputData))

		// Send progress updates via gRPC IPC as we process
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "grpc_processing_started",
			"value": time.Now().Format("2006-01-02 15:04:05"),
		})

		results := make([]map[string]interface{}, 0, len(inputData))
		var totalValue float64

		for i, item := range inputData {
			if itemMap, ok := item.(map[string]interface{}); ok {
				// Process each item
				processedItem := map[string]interface{}{
					"id":           i + 1,
					"original":     itemMap,
					"processed_by": processID,
					"processed_at": time.Now().Format("2006-01-02 15:04:05"),
					"transport":    "grpc",
					"type_safe":    true,
				}

				// Extract and sum numeric values
				if value, ok := itemMap["value"].(float64); ok {
					processedItem["doubled_value"] = value * 2
					totalValue += value
				}

				results = append(results, processedItem)

				// Send real-time progress via type-safe gRPC IPC
				ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
					"key":   fmt.Sprintf("grpc_item_%d_processed", i+1),
					"value": true,
				})
			}
		}

		// Store processing results (in final store)
		ctx.Workflow.Store.Put("processed_items", results)
		ctx.Workflow.Store.Put("total_value", totalValue)
		ctx.Workflow.Store.Put("processing_stats", map[string]interface{}{
			"items_processed": len(results),
			"total_value":     totalValue,
			"processor_pid":   processID,
			"processed_at":    time.Now().Format("2006-01-02 15:04:05"),
			"transport_type":  "grpc",
			"protocol":        "protobuf",
			"type_safety":     "enforced",
		})

		// Send completion notification via gRPC IPC
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "grpc_processing_completed",
			"value": fmt.Sprintf("Processed %d items via gRPC, total: %.2f", len(results), totalValue),
		})

		ctx.Logger.Info("Processed %d items via gRPC, total value: %.2f", len(results), totalValue)
	}

	// Add process information with gRPC details (in final store)
	ctx.Workflow.Store.Put("child_process_info", map[string]interface{}{
		"pid":           processID,
		"parent_pid":    os.Getppid(),
		"hostname":      getHostname(),
		"ipc_transport": "grpc",
		"protocol":      "protobuf",
		"type_safe":     true,
	})

	return nil
}

// ValidatorAction validates the processed data via gRPC
type ValidatorAction struct {
	gostage.BaseAction
}

func (a *ValidatorAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("Validator running in process %d (gRPC mode)", os.Getpid())

	// Send validation start notification via gRPC IPC
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "grpc_validation_started",
		"value": time.Now().Format("2006-01-02 15:04:05"),
	})

	// Debug: List all keys in the store
	ctx.Logger.Debug("Store contains %d keys via gRPC: %v", ctx.Workflow.Store.Count(), ctx.Workflow.Store.ListKeys())

	// Validate processed items exist
	if processedItems, err := store.Get[[]map[string]interface{}](ctx.Workflow.Store, "processed_items"); err == nil {
		ctx.Logger.Info("Found processed_items with %d items via gRPC", len(processedItems))

		// Send found items notification via gRPC IPC
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "grpc_validation_items_found",
			"value": len(processedItems),
		})

		validCount := 0
		grpcCount := 0
		for _, itemMap := range processedItems {
			if _, hasID := itemMap["id"]; hasID {
				if _, hasProcessedBy := itemMap["processed_by"]; hasProcessedBy {
					validCount++
					// Check if processed via gRPC
					if transport, ok := itemMap["transport"].(string); ok && transport == "grpc" {
						grpcCount++
					}
				}
			}
		}

		// Store validation results with gRPC specifics (in final store)
		ctx.Workflow.Store.Put("validation_results", map[string]interface{}{
			"total_items":        len(processedItems),
			"valid_items":        validCount,
			"grpc_items":         grpcCount,
			"validation_success": validCount == len(processedItems),
			"grpc_success":       grpcCount == len(processedItems),
			"validated_by":       os.Getpid(),
			"validated_at":       time.Now().Format("2006-01-02 15:04:05"),
			"transport_used":     "grpc",
			"protocol_used":      "protobuf",
		})

		// Send validation completion via gRPC IPC
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "grpc_validation_completed",
			"value": fmt.Sprintf("Validated %d/%d items via gRPC (all type-safe!)", validCount, len(processedItems)),
		})

		ctx.Logger.Info("gRPC validation complete: %d/%d items valid, %d/%d processed via gRPC",
			validCount, len(processedItems), grpcCount, len(processedItems))
	} else {
		ctx.Logger.Error("No processed items found for gRPC validation: %v", err)

		// Send validation error via gRPC IPC
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "grpc_validation_error",
			"value": "No processed items found",
		})

		// Store error in final store
		ctx.Workflow.Store.Put("validation_results", map[string]interface{}{
			"error":          "No processed items found",
			"transport_used": "grpc",
		})
	}

	return nil
}

func getHostname() string {
	if hostname, err := os.Hostname(); err == nil {
		return hostname
	}
	return "unknown"
}

// childMain handles execution when running as a child process with the new pure gRPC approach
func childMain() {
	fmt.Fprintf(os.Stderr, "🔥 CHILD PROCESS STARTED - PID: %d, Parent PID: %d\n", os.Getpid(), os.Getppid())

	// Register actions that the child process will need
	gostage.RegisterAction("data-processor", func() gostage.Action {
		return &DataProcessorAction{
			BaseAction: gostage.NewBaseAction("data-processor", "Processes data from store"),
		}
	})

	gostage.RegisterAction("validator", func() gostage.Action {
		return &ValidatorAction{
			BaseAction: gostage.NewBaseAction("validator", "Validates processed data"),
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

	// Register actions in parent too
	gostage.RegisterAction("data-processor", func() gostage.Action {
		return &DataProcessorAction{
			BaseAction: gostage.NewBaseAction("data-processor", "Processes data from store"),
		}
	})

	gostage.RegisterAction("validator", func() gostage.Action {
		return &ValidatorAction{
			BaseAction: gostage.NewBaseAction("validator", "Validates processed data"),
		}
	})

	// 🎉 NEW PURE GRPC API - Create runner with automatic port assignment
	fmt.Println("🔧 Setting up pure gRPC transport with automatic port assignment...")
	parentRunner := gostage.NewRunner(gostage.WithGRPCTransport())

	var childLogs []string
	var finalStoreFromChild map[string]interface{}
	realtimeStore := store.NewKVStore() // Store for real-time gRPC IPC messages

	// Set up message handlers for gRPC communication
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var logData struct {
			Level   string `json:"level"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(payload, &logData); err != nil {
			return err
		}

		logMessage := fmt.Sprintf("[CHILD-%s] %s", logData.Level, logData.Message)
		childLogs = append(childLogs, logMessage)
		fmt.Println(logMessage)
		return nil
	})

	// Handler for real-time gRPC IPC messages from .Send() calls
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var data struct {
			Key   string      `json:"key"`
			Value interface{} `json:"value"`
		}
		if err := json.Unmarshal(payload, &data); err != nil {
			return err
		}

		err := realtimeStore.Put(data.Key, data.Value)
		if err == nil {
			fmt.Printf("🔄 Real-time update via gRPC: %s = %v\n", data.Key, data.Value)
		}
		return err
	})

	// Handler to capture the final store from child
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeFinalStore, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var storeData map[string]interface{}
		if err := json.Unmarshal(payload, &storeData); err != nil {
			return fmt.Errorf("failed to unmarshal final store: %w", err)
		}
		finalStoreFromChild = storeData
		fmt.Printf("📦 Parent received final store with %d keys via gRPC\n", len(storeData))
		return nil
	})

	// Prepare initial store data for the child process
	fmt.Println("📦 Setting up initial store data for child process...")

	initialStore := map[string]interface{}{
		"config": map[string]interface{}{
			"mode":           "development",
			"debug":          true,
			"max_workers":    4,
			"transport_type": "grpc",
			"protocol":       "protobuf",
			"type_safety":    true,
		},
		"input_data": []interface{}{
			map[string]interface{}{"name": "item1", "value": 10.5, "category": "A"},
			map[string]interface{}{"name": "item2", "value": 25.0, "category": "B"},
			map[string]interface{}{"name": "item3", "value": 15.75, "category": "A"},
			map[string]interface{}{"name": "item4", "value": 30.25, "category": "C"},
		},
		"metadata": map[string]interface{}{
			"created_by":     "parent_process",
			"created_at":     time.Now().Format("2006-01-02 15:04:05"),
			"parent_pid":     os.Getpid(),
			"version":        "1.0.0",
			"transport_type": "grpc",
			"protocol":       "protobuf",
			"type_safety":    "enforced",
		},
	}

	// Define workflow that will process the store data
	workflowDef := gostage.SubWorkflowDef{
		ID:           "grpc-data-processing-workflow",
		Name:         "Pure gRPC Data Processing Workflow",
		Description:  "Demonstrates type-safe store data processing via pure gRPC transport",
		InitialStore: initialStore,
		Stages: []gostage.StageDef{
			{
				ID:   "processing-stage",
				Name: "Data Processing",
				Actions: []gostage.ActionDef{
					{ID: "data-processor"},
				},
			},
			{
				ID:   "validation-stage",
				Name: "Data Validation",
				Actions: []gostage.ActionDef{
					{ID: "validator"},
				},
			},
		},
	}

	fmt.Printf("📋 Parent process %d spawning child to process %d data items\n",
		os.Getpid(), len(initialStore["input_data"].([]interface{})))

	// Spawn child with automatic gRPC transport configuration!
	ctx := context.Background()
	startTime := time.Now()

	// This automatically uses gRPC transport - no configuration needed!
	err := parentRunner.Spawn(ctx, workflowDef)

	duration := time.Since(startTime)
	fmt.Printf("⏱️  Child process execution completed in %v\n", duration)

	if err != nil {
		fmt.Printf("❌ Child process execution failed: %v\n", err)
		fmt.Printf("Total child log messages received: %d\n", len(childLogs))
		return
	}

	fmt.Println("✅ Child process execution completed successfully!")
	fmt.Println()

	// Display comprehensive results
	fmt.Println("📊 === EXECUTION SUMMARY ===")
	fmt.Printf("Parent Process ID: %d\n", os.Getpid())
	fmt.Printf("Transport Type: Pure gRPC (protobuf)\n")
	fmt.Printf("Type Safety: Enforced\n")
	fmt.Printf("Total child log messages: %d\n", len(childLogs))
	fmt.Printf("Real-time IPC messages received: %d\n", realtimeStore.Count())
	if finalStoreFromChild != nil {
		fmt.Printf("Final store contains %d keys\n", len(finalStoreFromChild))
	} else {
		fmt.Println("No final store received from child")
	}
	fmt.Println()

	// Show the communication patterns
	fmt.Println("📦 === COMMUNICATION PATTERNS ===")

	fmt.Println("🔄 Real-time IPC Messages (via .Send()):")
	fmt.Println("   Purpose: Type-safe progress updates, notifications, live monitoring")
	fmt.Println("   Protocol: Binary protobuf over gRPC")
	for _, key := range realtimeStore.ListKeys() {
		if value, err := store.GetOrDefault[interface{}](realtimeStore, key, nil); err == nil {
			fmt.Printf("  🔒 %s: %v (type-safe)\n", key, value)
		}
	}

	if finalStoreFromChild != nil {
		fmt.Println("\n📦 Final Store Data (workflow store export):")
		fmt.Println("   Purpose: Structured data, processing results, persistent state")
		fmt.Println("   Protocol: Binary protobuf serialization")

		fmt.Println("Initial Store (sent to child):")
		for key, value := range initialStore {
			fmt.Printf("  📤 %s: %v\n", key, summarizeValue(value))
		}

		fmt.Println("\nFinal Store (received from child):")
		for key, value := range finalStoreFromChild {
			fmt.Printf("  📥 %s: %v\n", key, summarizeValue(value))
		}

		// Analyze processing results
		if processInfo, ok := finalStoreFromChild["child_process_info"].(map[string]interface{}); ok {
			fmt.Printf("\n✅ Child Process Info:\n")
			fmt.Printf("  PID: %.0f (different from parent: %d)\n", processInfo["pid"], os.Getpid())
			fmt.Printf("  Parent PID: %.0f\n", processInfo["parent_pid"])
			fmt.Printf("  Hostname: %s\n", processInfo["hostname"])
			fmt.Printf("  Transport: %s\n", processInfo["ipc_transport"])
			fmt.Printf("  Protocol: %s\n", processInfo["protocol"])
			fmt.Printf("  Type Safe: %v\n", processInfo["type_safe"])
		}

		if stats, ok := finalStoreFromChild["processing_stats"].(map[string]interface{}); ok {
			fmt.Printf("\n✅ Processing Statistics:\n")
			fmt.Printf("  Items processed: %.0f\n", stats["items_processed"])
			fmt.Printf("  Total value: %.2f\n", stats["total_value"])
			fmt.Printf("  Transport type: %s\n", stats["transport_type"])
			fmt.Printf("  Protocol: %s\n", stats["protocol"])
			fmt.Printf("  Type safety: %s\n", stats["type_safety"])
		}

		if validation, ok := finalStoreFromChild["validation_results"].(map[string]interface{}); ok {
			fmt.Printf("\n✅ Validation Results:\n")

			if errorMsg, hasError := validation["error"]; hasError {
				fmt.Printf("  ❌ Validation Error: %v\n", errorMsg)
			} else {
				fmt.Printf("  Total items: %.0f\n", validation["total_items"])
				fmt.Printf("  Valid items: %.0f\n", validation["valid_items"])
				fmt.Printf("  gRPC items: %.0f\n", validation["grpc_items"])
				fmt.Printf("  Validation success: %v\n", validation["validation_success"])
				fmt.Printf("  gRPC success: %v\n", validation["grpc_success"])
				fmt.Printf("  Transport used: %s\n", validation["transport_used"])
				fmt.Printf("  Protocol used: %s\n", validation["protocol_used"])
			}
		}
	}

	fmt.Println("\n🎉 === PURE GRPC SUCCESS ===")
	fmt.Println("✅ No transport configuration needed")
	fmt.Println("✅ Automatic gRPC server setup")
	fmt.Println("✅ Type-safe protobuf communication")
	fmt.Println("✅ Seamless child process coordination")
	fmt.Println("✅ Real-time IPC and final store transfer")
}

func summarizeValue(value interface{}) string {
	switch v := value.(type) {
	case []interface{}:
		return fmt.Sprintf("array[%d]", len(v))
	case map[string]interface{}:
		return fmt.Sprintf("object{%d keys}", len(v))
	case string:
		if len(v) > 30 {
			return fmt.Sprintf("\"%s...\"", v[:30])
		}
		return fmt.Sprintf("\"%s\"", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
