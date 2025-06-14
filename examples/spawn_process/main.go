package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// DataProcessorAction demonstrates comprehensive store manipulation
type DataProcessorAction struct {
	gostage.BaseAction
}

func (a *DataProcessorAction) Execute(ctx *gostage.ActionContext) error {
	processID := os.Getpid()
	ctx.Logger.Info("DataProcessor running in process %d", processID)

	// Send real-time process info via IPC
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "realtime_child_pid",
		"value": processID,
	})

	// Read initial configuration from store
	if config, err := store.Get[map[string]interface{}](ctx.Workflow.Store, "config"); err == nil {
		ctx.Logger.Info("Processing with config: %+v", config)

		// Send config confirmation via IPC
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "config_received",
			"value": true,
		})

		// Process based on config
		if mode, ok := config["mode"].(string); ok {
			switch mode {
			case "development":
				ctx.Workflow.Store.Put("debug_enabled", true)
				ctx.Workflow.Store.Put("log_level", "debug")
			case "production":
				ctx.Workflow.Store.Put("debug_enabled", false)
				ctx.Workflow.Store.Put("log_level", "info")
			}
		}
	}

	// Read and process input data
	if inputData, err := store.Get[[]interface{}](ctx.Workflow.Store, "input_data"); err == nil {
		ctx.Logger.Info("Processing %d items", len(inputData))

		// Send progress updates via IPC as we process
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "processing_started",
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
				}

				// Extract and sum numeric values
				if value, ok := itemMap["value"].(float64); ok {
					processedItem["doubled_value"] = value * 2
					totalValue += value
				}

				results = append(results, processedItem)

				// Send real-time progress via IPC
				ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
					"key":   fmt.Sprintf("item_%d_processed", i+1),
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
		})

		// Send completion notification via IPC
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "processing_completed",
			"value": fmt.Sprintf("Processed %d items, total: %.2f", len(results), totalValue),
		})

		ctx.Logger.Info("Processed %d items, total value: %.2f", len(results), totalValue)
	}

	// Add process information (in final store)
	ctx.Workflow.Store.Put("child_process_info", map[string]interface{}{
		"pid":        processID,
		"parent_pid": os.Getppid(),
		"hostname":   getHostname(),
	})

	return nil
}

// ValidatorAction validates the processed data
type ValidatorAction struct {
	gostage.BaseAction
}

func (a *ValidatorAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("Validator running in process %d", os.Getpid())

	// Send validation start notification via IPC
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "validation_started",
		"value": time.Now().Format("2006-01-02 15:04:05"),
	})

	// Debug: List all keys in the store
	ctx.Logger.Debug("Store contains %d keys: %v", ctx.Workflow.Store.Count(), ctx.Workflow.Store.ListKeys())

	// Validate processed items exist
	if processedItems, err := store.Get[[]map[string]interface{}](ctx.Workflow.Store, "processed_items"); err == nil {
		ctx.Logger.Info("Found processed_items with %d items", len(processedItems))

		// Send found items notification via IPC
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "validation_items_found",
			"value": len(processedItems),
		})

		validCount := 0
		for _, itemMap := range processedItems {
			if _, hasID := itemMap["id"]; hasID {
				if _, hasProcessedBy := itemMap["processed_by"]; hasProcessedBy {
					validCount++
				}
			}
		}

		// Store validation results (in final store)
		ctx.Workflow.Store.Put("validation_results", map[string]interface{}{
			"total_items":        len(processedItems),
			"valid_items":        validCount,
			"validation_success": validCount == len(processedItems),
			"validated_by":       os.Getpid(),
			"validated_at":       time.Now().Format("2006-01-02 15:04:05"),
		})

		// Send validation completion via IPC
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "validation_completed",
			"value": fmt.Sprintf("Validated %d/%d items successfully", validCount, len(processedItems)),
		})

		ctx.Logger.Info("Validation complete: %d/%d items valid", validCount, len(processedItems))
	} else {
		ctx.Logger.Error("No processed items found for validation: %v", err)

		// Send validation error via IPC
		ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
			"key":   "validation_error",
			"value": "No processed items found",
		})

		// Store error in final store
		ctx.Workflow.Store.Put("validation_results", map[string]interface{}{
			"error": "No processed items found",
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

// childMain handles execution when running as a child process
func childMain() {
	fmt.Fprintf(os.Stderr, "🔥 CHILD PROCESS STARTED - PID: %d, Parent PID: %d\n", os.Getpid(), os.Getppid())

	// Set up broker for parent communication
	broker := gostage.NewRunnerBroker(os.Stdout)

	// Create logger that sends all messages to parent
	logger := &ChildLogger{broker: broker}

	// Create runner with broker
	runner := gostage.NewRunnerWithBroker(broker)

	// Register actions
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

	// Read workflow definition from stdin
	defBytes, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process failed to read workflow definition: %v\n", err)
		os.Exit(1)
	}

	var workflowDef gostage.SubWorkflowDef
	if err := json.Unmarshal(defBytes, &workflowDef); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process failed to unmarshal workflow: %v\n", err)
		os.Exit(1)
	}

	// Create workflow from definition
	workflow, err := gostage.NewWorkflowFromDef(&workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process failed to create workflow: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "✅ Child process %d executing workflow: %s\n", os.Getpid(), workflowDef.ID)

	// Execute workflow
	if err := runner.Execute(context.Background(), workflow, logger); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Child process %d workflow execution failed: %v\n", os.Getpid(), err)
		os.Exit(1)
	}

	// Send final store state to parent
	if workflow.Store != nil {
		finalStore := workflow.Store.ExportAll()
		if err := broker.Send(gostage.MessageTypeFinalStore, finalStore); err != nil {
			fmt.Fprintf(os.Stderr, "⚠️  Child process failed to send final store: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "📦 Child process sent final store with %d keys\n", len(finalStore))
		}
	}

	fmt.Fprintf(os.Stderr, "✅ Child process %d completed successfully\n", os.Getpid())
	os.Exit(0)
}

// ChildLogger implements gostage.Logger and sends all logs via broker
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

	// Parent process execution
	fmt.Printf("🚀 PARENT PROCESS STARTED - PID: %d\n", os.Getpid())

	// Register actions in parent too (needed for spawn functionality)
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

	// Create parent runner
	parentRunner := gostage.NewRunner()
	var childLogs []string
	var finalStoreFromChild map[string]interface{}
	realtimeStore := store.NewKVStore() // Store for real-time IPC messages

	// Set up message handlers
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

	// Handler for real-time IPC messages from .Send() calls
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
			fmt.Printf("🔄 Real-time update: %s = %v\n", data.Key, data.Value)
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
		fmt.Printf("📦 Parent received final store with %d keys\n", len(storeData))
		return nil
	})

	// Prepare initial store data for the child process
	fmt.Println("📦 Setting up initial store data for child process...")

	initialStore := map[string]interface{}{
		"config": map[string]interface{}{
			"mode":        "development",
			"debug":       true,
			"max_workers": 4,
		},
		"input_data": []interface{}{
			map[string]interface{}{"name": "item1", "value": 10.5, "category": "A"},
			map[string]interface{}{"name": "item2", "value": 25.0, "category": "B"},
			map[string]interface{}{"name": "item3", "value": 15.75, "category": "A"},
			map[string]interface{}{"name": "item4", "value": 30.25, "category": "C"},
		},
		"metadata": map[string]interface{}{
			"created_by": "parent_process",
			"created_at": time.Now().Format("2006-01-02 15:04:05"),
			"parent_pid": os.Getpid(),
			"version":    "1.0.0",
		},
	}

	// Define workflow that will process the store data
	workflowDef := gostage.SubWorkflowDef{
		ID:           "data-processing-workflow",
		Name:         "Data Processing Workflow",
		Description:  "Demonstrates store data processing in child process",
		InitialStore: initialStore, // Pass initial store data
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

	// Use regular Spawn method with our message handlers
	ctx := context.Background()
	startTime := time.Now()

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
	fmt.Printf("Total child log messages: %d\n", len(childLogs))
	fmt.Printf("Real-time IPC messages received: %d\n", realtimeStore.Count())
	if finalStoreFromChild != nil {
		fmt.Printf("Final store contains %d keys\n", len(finalStoreFromChild))
	} else {
		fmt.Println("No final store received from child")
	}
	fmt.Println()

	// Show the difference between real-time IPC and final store
	fmt.Println("📦 === COMMUNICATION PATTERNS ===")

	fmt.Println("🔄 Real-time IPC Messages (via .Send()):")
	fmt.Println("   Purpose: Progress updates, notifications, live monitoring")
	for _, key := range realtimeStore.ListKeys() {
		if value, err := store.GetOrDefault[interface{}](realtimeStore, key, nil); err == nil {
			fmt.Printf("  📤 %s: %v\n", key, value)
		}
	}

	fmt.Println("\n📦 Final Store Data (workflow store export):")
	fmt.Println("   Purpose: Structured data, processing results, persistent state")

	fmt.Println("Initial Store (sent to child):")
	for key, value := range initialStore {
		fmt.Printf("  📤 %s: %v\n", key, summarizeValue(value))
	}

	if finalStoreFromChild != nil {
		fmt.Println("\nFinal Store (received from child):")
		for key, value := range finalStoreFromChild {
			fmt.Printf("  📥 %s: %v\n", key, summarizeValue(value))
		}
	} else {
		fmt.Println("\nNo final store data received from child")
	}
	fmt.Println()

	// Analyze processing results (only if we have final store data)
	if finalStoreFromChild != nil {
		fmt.Println("🔍 === PROCESSING ANALYSIS ===")

		if processInfo, ok := finalStoreFromChild["child_process_info"].(map[string]interface{}); ok {
			fmt.Printf("✅ Child Process Info:\n")
			fmt.Printf("  PID: %.0f (different from parent: %d)\n", processInfo["pid"], os.Getpid())
			fmt.Printf("  Parent PID: %.0f\n", processInfo["parent_pid"])
			fmt.Printf("  Hostname: %s\n", processInfo["hostname"])
		}

		if stats, ok := finalStoreFromChild["processing_stats"].(map[string]interface{}); ok {
			fmt.Printf("✅ Processing Statistics:\n")
			fmt.Printf("  Items processed: %.0f\n", stats["items_processed"])
			fmt.Printf("  Total value: %.2f\n", stats["total_value"])
			fmt.Printf("  Processed at: %s\n", stats["processed_at"])
			fmt.Printf("  Processor PID: %.0f\n", stats["processor_pid"])
		}

		if validation, ok := finalStoreFromChild["validation_results"].(map[string]interface{}); ok {
			fmt.Printf("✅ Validation Results:\n")

			// Check if validation had an error
			if errorMsg, hasError := validation["error"]; hasError {
				fmt.Printf("  ❌ Validation Error: %v\n", errorMsg)
			} else {
				// Normal validation results
				if totalItems, ok := validation["total_items"]; ok {
					fmt.Printf("  Total items: %.0f\n", totalItems)
				}
				if validItems, ok := validation["valid_items"]; ok {
					fmt.Printf("  Valid items: %.0f\n", validItems)
				}
				if success, ok := validation["validation_success"]; ok {
					fmt.Printf("  Success: %v\n", success)
				}
				if validatedAt, ok := validation["validated_at"]; ok {
					fmt.Printf("  Validated at: %s\n", validatedAt)
				}
			}
		}
	}

	fmt.Println()
	fmt.Println("🎉 Enhanced Spawn Example completed successfully!")
	fmt.Println("   ✅ Demonstrated passing initial store data to child process")
	fmt.Println("   ✅ Showed comprehensive data processing in child")
	fmt.Println("   ✅ Retrieved complete final store state from child")
	fmt.Println("   ✅ Used real-time IPC messaging for progress updates")
	fmt.Println("   ✅ Demonstrated both communication patterns working together")
}

func summarizeValue(value interface{}) string {
	switch v := value.(type) {
	case map[string]interface{}:
		return fmt.Sprintf("map[%d keys]", len(v))
	case []interface{}:
		return fmt.Sprintf("array[%d items]", len(v))
	case string:
		if len(v) > 50 {
			return fmt.Sprintf("'%s...' (%d chars)", v[:47], len(v))
		}
		return fmt.Sprintf("'%s'", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
