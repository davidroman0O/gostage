package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// MessageFilterMiddleware filters out certain message types
func MessageFilterMiddleware(excludeTypes ...gostage.MessageType) gostage.IPCMiddlewareFunc {
	excludeMap := make(map[gostage.MessageType]bool)
	for _, t := range excludeTypes {
		excludeMap[t] = true
	}

	return gostage.IPCMiddlewareFunc{
		ProcessOutboundFunc: func(msgType gostage.MessageType, payload interface{}) (gostage.MessageType, interface{}, error) {
			if excludeMap[msgType] {
				return "", nil, fmt.Errorf("message type %s is filtered out", msgType)
			}
			return msgType, payload, nil
		},
		ProcessInboundFunc: func(msgType gostage.MessageType, payload json.RawMessage) (gostage.MessageType, json.RawMessage, error) {
			if excludeMap[msgType] {
				return "", nil, fmt.Errorf("message type %s is filtered out", msgType)
			}
			return msgType, payload, nil
		},
	}
}

// MessageTransformMiddleware modifies log messages
func MessageTransformMiddleware() gostage.IPCMiddlewareFunc {
	return gostage.IPCMiddlewareFunc{
		ProcessOutboundFunc: func(msgType gostage.MessageType, payload interface{}) (gostage.MessageType, interface{}, error) {
			if msgType == gostage.MessageTypeLog {
				// Convert to map[string]string if it isn't already
				var logData map[string]string

				switch v := payload.(type) {
				case map[string]string:
					logData = v
				case map[string]interface{}:
					logData = make(map[string]string)
					for k, val := range v {
						if strVal, ok := val.(string); ok {
							logData[k] = strVal
						}
					}
				default:
					// Return original payload if we can't process it
					return msgType, payload, nil
				}

				// Add prefix to all log messages
				if message, exists := logData["message"]; exists {
					logData["message"] = "[ENHANCED] " + message
				}
				// Add timestamp
				logData["timestamp"] = time.Now().Format("15:04:05.000")

				return msgType, logData, nil
			}
			return msgType, payload, nil
		},
	}
}

// MessageEncryptionMiddleware simulates message encryption
func MessageEncryptionMiddleware() gostage.IPCMiddlewareFunc {
	return gostage.IPCMiddlewareFunc{
		ProcessOutboundFunc: func(msgType gostage.MessageType, payload interface{}) (gostage.MessageType, interface{}, error) {
			// Simulate encryption by base64 encoding sensitive data
			if msgType == gostage.MessageTypeStorePut {
				if storeData, ok := payload.(map[string]interface{}); ok {
					if key, exists := storeData["key"]; exists {
						if keyStr, ok := key.(string); ok && strings.Contains(keyStr, "sensitive") {
							storeData["encrypted"] = true
							storeData["key"] = "encrypted_" + keyStr
						}
					}
				}
			}
			return msgType, payload, nil
		},
		ProcessInboundFunc: func(msgType gostage.MessageType, payload json.RawMessage) (gostage.MessageType, json.RawMessage, error) {
			// Simulate decryption
			if msgType == gostage.MessageTypeStorePut {
				var storeData map[string]interface{}
				if err := json.Unmarshal(payload, &storeData); err == nil {
					if encrypted, exists := storeData["encrypted"]; exists && encrypted == true {
						if key, exists := storeData["key"]; exists {
							if keyStr, ok := key.(string); ok && strings.HasPrefix(keyStr, "encrypted_") {
								storeData["key"] = strings.TrimPrefix(keyStr, "encrypted_")
								delete(storeData, "encrypted")
								// Re-marshal the modified data
								if newPayload, err := json.Marshal(storeData); err == nil {
									payload = json.RawMessage(newPayload)
								}
							}
						}
					}
				}
			}
			return msgType, payload, nil
		},
	}
}

// ProcessLifecycleMiddleware tracks process lifecycle
func ProcessLifecycleMiddleware() gostage.SpawnMiddlewareFunc {
	return gostage.SpawnMiddlewareFunc{
		BeforeSpawnFunc: func(ctx context.Context, def gostage.SubWorkflowDef) (context.Context, gostage.SubWorkflowDef, error) {
			fmt.Printf("🚀 [MIDDLEWARE] About to spawn child process for workflow: %s\n", def.ID)

			// We could modify the workflow definition here
			if def.Description == "" {
				def.Description = "Enhanced by middleware"
			}

			// Add context values
			enhancedCtx := context.WithValue(ctx, "spawn_time", time.Now())

			return enhancedCtx, def, nil
		},
		AfterSpawnFunc: func(ctx context.Context, def gostage.SubWorkflowDef, err error) error {
			if spawnTime, ok := ctx.Value("spawn_time").(time.Time); ok {
				duration := time.Since(spawnTime)
				fmt.Printf("⏱️ [MIDDLEWARE] Child process completed in %v\n", duration)
			}

			if err != nil {
				fmt.Printf("❌ [MIDDLEWARE] Child process failed: %v\n", err)
			} else {
				fmt.Printf("✅ [MIDDLEWARE] Child process completed successfully\n")
			}

			return nil
		},
		OnChildMessageFunc: func(msgType gostage.MessageType, payload json.RawMessage) error {
			// Count messages or perform custom logging
			fmt.Printf("📨 [MIDDLEWARE] Received message type: %s\n", msgType)

			// Could implement message routing, metrics collection, etc.
			return nil
		},
	}
}

// MetricsMiddleware collects communication metrics
type MetricsMiddleware struct {
	MessageCount map[gostage.MessageType]int
	TotalBytes   int
}

func NewMetricsMiddleware() *MetricsMiddleware {
	return &MetricsMiddleware{
		MessageCount: make(map[gostage.MessageType]int),
	}
}

func (m *MetricsMiddleware) ProcessOutbound(msgType gostage.MessageType, payload interface{}) (gostage.MessageType, interface{}, error) {
	m.MessageCount[msgType]++
	if data, err := json.Marshal(payload); err == nil {
		m.TotalBytes += len(data)
	}
	return msgType, payload, nil
}

func (m *MetricsMiddleware) ProcessInbound(msgType gostage.MessageType, payload json.RawMessage) (gostage.MessageType, json.RawMessage, error) {
	m.MessageCount[msgType]++
	m.TotalBytes += len(payload)
	return msgType, payload, nil
}

func (m *MetricsMiddleware) BeforeSpawn(ctx context.Context, def gostage.SubWorkflowDef) (context.Context, gostage.SubWorkflowDef, error) {
	fmt.Println("📊 [METRICS] Starting metrics collection")
	return ctx, def, nil
}

func (m *MetricsMiddleware) AfterSpawn(ctx context.Context, def gostage.SubWorkflowDef, err error) error {
	fmt.Println("\n📊 [METRICS] Communication Statistics:")
	for msgType, count := range m.MessageCount {
		fmt.Printf("  %s: %d messages\n", msgType, count)
	}
	fmt.Printf("  Total bytes: %d\n", m.TotalBytes)
	return nil
}

func (m *MetricsMiddleware) OnChildMessage(msgType gostage.MessageType, payload json.RawMessage) error {
	// This is handled by ProcessInbound
	return nil
}

// DemoAction demonstrates various message types for middleware
type DemoAction struct {
	gostage.BaseAction
}

func (a *DemoAction) Execute(ctx *gostage.ActionContext) error {
	processID := os.Getpid()

	ctx.Logger.Info("Regular log message from PID %d", processID)
	ctx.Logger.Debug("Debug message that might be filtered")

	// Send various store updates including sensitive data
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "regular_data",
		"value": "public information",
	})

	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "sensitive_data",
		"value": "secret information",
	})

	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "process_id",
		"value": processID,
	})

	ctx.Logger.Warn("Warning message about something")
	ctx.Logger.Error("Simulated error message")

	return nil
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
	// This now goes through the middleware chain
	l.broker.Send(gostage.MessageTypeLog, payload)
}

// childMain handles execution when running as a child process
func childMain() {
	fmt.Fprintf(os.Stderr, "🔥 CHILD PROCESS STARTED - PID: %d, Parent PID: %d\n", os.Getpid(), os.Getppid())

	// Set up broker for parent communication
	broker := gostage.NewRunnerBroker(os.Stdout)

	// Add the same middleware to child process for consistent behavior
	broker.AddIPCMiddleware(MessageTransformMiddleware())
	broker.AddIPCMiddleware(MessageEncryptionMiddleware())

	// Create logger that sends all messages to parent
	logger := &ChildLogger{broker: broker}

	// Create runner with broker using the convenience constructor
	runner := gostage.NewRunnerWithBroker(broker)

	// Register the demo action
	gostage.RegisterAction("demo-action", func() gostage.Action {
		return &DemoAction{
			BaseAction: gostage.NewBaseAction("demo-action", "Demonstrates middleware"),
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

	fmt.Fprintf(os.Stderr, "✅ Child process %d completed successfully\n", os.Getpid())
	os.Exit(0)
}

func main() {
	// Check if this is a child process by looking for the --gostage-child argument
	for _, arg := range os.Args[1:] {
		if arg == "--gostage-child" {
			childMain()
			return
		}
	}

	// Parent process execution
	fmt.Println("🎭 === MIDDLEWARE DEMONSTRATION ===")
	fmt.Println()

	// Register actions
	gostage.RegisterAction("demo-action", func() gostage.Action {
		return &DemoAction{
			BaseAction: gostage.NewBaseAction("demo-action", "Demonstrates middleware"),
		}
	})

	// Create runner with middleware
	runner := gostage.NewRunner()
	parentStore := store.NewKVStore()

	// Add IPC middleware
	runner.AddIPCMiddleware(MessageTransformMiddleware())
	runner.AddIPCMiddleware(MessageEncryptionMiddleware())

	// Create metrics middleware (implements both interfaces)
	metrics := NewMetricsMiddleware()
	runner.AddIPCMiddleware(metrics)
	runner.UseSpawnMiddleware(metrics)

	// Add lifecycle middleware
	runner.UseSpawnMiddleware(ProcessLifecycleMiddleware())

	// Set up message handlers
	runner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var logData map[string]string
		json.Unmarshal(payload, &logData)

		timestamp := logData["timestamp"]
		if timestamp == "" {
			timestamp = time.Now().Format("15:04:05.000")
		}
		level := logData["level"]
		message := logData["message"]

		fmt.Printf("[%s] [CHILD-%s] %s\n", timestamp, level, message)
		return nil
	})

	runner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var data map[string]interface{}
		json.Unmarshal(payload, &data)

		// Type assert the key and value
		key, keyOk := data["key"].(string)
		value := data["value"]

		if !keyOk {
			return fmt.Errorf("invalid key type: %T", data["key"])
		}

		err := parentStore.Put(key, value)
		if err == nil {
			fmt.Printf("📦 [STORE] %s = %v\n", key, value)
		}
		return err
	})

	// Define workflow
	workflowDef := gostage.SubWorkflowDef{
		ID:          "middleware-demo",
		Name:        "Middleware Demonstration",
		Description: "Shows middleware capabilities",
		Stages: []gostage.StageDef{
			{
				ID: "demo-stage",
				Actions: []gostage.ActionDef{
					{ID: "demo-action"},
				},
			},
		},
	}

	// Run with middleware
	fmt.Println("🚀 Starting middleware-enhanced spawn...")
	err := runner.Spawn(context.Background(), workflowDef)

	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
	} else {
		fmt.Println("\n🎉 Middleware demo completed successfully!")

		// Show final store state
		fmt.Println("\n📦 === FINAL STORE STATE ===")
		for _, key := range parentStore.ListKeys() {
			if value, err := store.Get[interface{}](parentStore, key); err == nil {
				fmt.Printf("  %s: %v\n", key, value)
			}
		}
	}
}
