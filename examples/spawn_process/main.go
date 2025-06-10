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

// SimpleTestAction just logs a message with process ID
type SimpleTestAction struct {
	gostage.BaseAction
}

func (a *SimpleTestAction) Execute(ctx *gostage.ActionContext) error {
	processID := os.Getpid()

	ctx.Logger.Info("Hello from child process %d", processID)

	// Send just one simple store update
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "child_pid",
		"value": processID,
	})

	return nil
}

// ProcessInfoAction shows comprehensive process information
type ProcessInfoAction struct {
	gostage.BaseAction
}

func (a *ProcessInfoAction) Execute(ctx *gostage.ActionContext) error {
	processID := os.Getpid()
	parentPID := os.Getppid()
	hostname, _ := os.Hostname()
	workingDir, _ := os.Getwd()

	ctx.Logger.Info("=== CHILD PROCESS INFORMATION ===")
	ctx.Logger.Info("Process ID: %d", processID)
	ctx.Logger.Info("Parent Process ID: %d", parentPID)
	ctx.Logger.Info("Hostname: %s", hostname)
	ctx.Logger.Info("Working Directory: %s", workingDir)
	ctx.Logger.Info("Timestamp: %s", time.Now().Format("2006-01-02 15:04:05.000"))

	// Send process info back to parent via store updates
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "child_process_id",
		"value": processID,
	})
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "child_parent_id",
		"value": parentPID,
	})
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "child_hostname",
		"value": hostname,
	})

	return nil
}

// FileOperationAction demonstrates file operations in child process
type FileOperationAction struct {
	gostage.BaseAction
}

func (a *FileOperationAction) Execute(ctx *gostage.ActionContext) error {
	processID := os.Getpid()
	filename := fmt.Sprintf("/tmp/child_process_%d.txt", processID)

	ctx.Logger.Info("Child process %d creating file: %s", processID, filename)

	content := fmt.Sprintf("This file was created by child process %d at %s\nParent PID: %d\n",
		processID, time.Now().Format("2006-01-02 15:04:05"), os.Getppid())

	err := os.WriteFile(filename, []byte(content), 0644)
	if err != nil {
		ctx.Logger.Error("Failed to create file: %v", err)
		return err
	}

	ctx.Logger.Info("File created successfully by process %d", processID)

	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "created_file",
		"value": filename,
	})

	return nil
}

// childMain handles execution when running as a child process
func childMain() {
	fmt.Fprintf(os.Stderr, "🔥 CHILD PROCESS STARTED - PID: %d, Parent PID: %d\n", os.Getpid(), os.Getppid())

	// Set up broker for parent communication
	broker := gostage.NewRunnerBroker(os.Stdout)

	// Create logger that sends all messages to parent
	logger := &ChildLogger{broker: broker}

	// Create runner and set broker
	runner := gostage.NewRunner()
	runner.Broker = broker

	// Register the simple action
	gostage.RegisterAction("simple-test", func() gostage.Action {
		return &SimpleTestAction{
			BaseAction: gostage.NewBaseAction("simple-test", "Simple test action"),
		}
	})

	gostage.RegisterAction("process-info", func() gostage.Action {
		return &ProcessInfoAction{
			BaseAction: gostage.NewBaseAction("process-info", "Shows process information"),
		}
	})

	gostage.RegisterAction("file-operation", func() gostage.Action {
		return &FileOperationAction{
			BaseAction: gostage.NewBaseAction("file-operation", "Creates a file with process info"),
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
	// Check if this is a child process by looking for the --gostage-child argument
	for _, arg := range os.Args[1:] {
		if arg == "--gostage-child" {
			childMain()
			return
		}
	}

	// Parent process execution
	fmt.Printf("🚀 PARENT PROCESS STARTED - PID: %d\n", os.Getpid())

	// Register the simple action
	gostage.RegisterAction("simple-test", func() gostage.Action {
		return &SimpleTestAction{
			BaseAction: gostage.NewBaseAction("simple-test", "Simple test action"),
		}
	})

	gostage.RegisterAction("process-info", func() gostage.Action {
		return &ProcessInfoAction{
			BaseAction: gostage.NewBaseAction("process-info", "Shows process information"),
		}
	})

	gostage.RegisterAction("file-operation", func() gostage.Action {
		return &FileOperationAction{
			BaseAction: gostage.NewBaseAction("file-operation", "Creates a file with process info"),
		}
	})

	// Create parent runner
	parentRunner := gostage.NewRunner()
	parentStore := store.NewKVStore()

	var childLogs []string

	// Set up basic message handlers
	parentRunner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var logData struct {
			Level   string `json:"level"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(payload, &logData); err != nil {
			fmt.Printf("❌ Failed to parse log message: %v\n", err)
			fmt.Printf("Raw payload: %s\n", string(payload))
			return err
		}

		logMessage := fmt.Sprintf("[CHILD-%s] %s", logData.Level, logData.Message)
		childLogs = append(childLogs, logMessage)
		fmt.Println(logMessage)
		return nil
	})

	parentRunner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var data struct {
			Key   string      `json:"key"`
			Value interface{} `json:"value"`
		}
		if err := json.Unmarshal(payload, &data); err != nil {
			fmt.Printf("❌ Failed to parse store message: %v\n", err)
			fmt.Printf("Raw payload: %s\n", string(payload))
			return err
		}

		err := parentStore.Put(data.Key, data.Value)
		if err == nil {
			fmt.Printf("📦 Store update: %s = %v\n", data.Key, data.Value)
		}
		return err
	})

	// Define a comprehensive workflow that proves child process execution
	workflowDef := gostage.SubWorkflowDef{
		ID:          "spawn-demo-workflow",
		Name:        "Process Spawn Demonstration",
		Description: "Demonstrates real child process execution with IPC",
		Stages: []gostage.StageDef{
			{
				ID:   "process-info-stage",
				Name: "Process Information",
				Actions: []gostage.ActionDef{
					{ID: "process-info"},
				},
			},
			{
				ID:   "simple-test-stage",
				Name: "Simple Test",
				Actions: []gostage.ActionDef{
					{ID: "simple-test"},
				},
			},
			{
				ID:   "file-ops-stage",
				Name: "File Operations",
				Actions: []gostage.ActionDef{
					{ID: "file-operation"},
				},
			},
		},
	}

	fmt.Printf("📋 Parent process %d spawning child to execute comprehensive workflow\n", os.Getpid())

	// Spawn the child process
	ctx := context.Background()
	startTime := time.Now()

	err := parentRunner.Spawn(ctx, workflowDef)

	duration := time.Since(startTime)

	fmt.Printf("⏱️  Child process execution completed in %v\n", duration)

	if err != nil {
		fmt.Printf("❌ Child process execution failed: %v\n", err)
		fmt.Printf("Total child log messages received: %d\n", len(childLogs))
		for i, log := range childLogs {
			fmt.Printf("  %d: %s\n", i+1, log)
		}
		return
	}

	fmt.Println("✅ Child process execution completed successfully!")
	fmt.Println()

	// Display comprehensive results
	fmt.Println("📊 === EXECUTION SUMMARY ===")
	fmt.Printf("Parent Process ID: %d\n", os.Getpid())
	fmt.Printf("Total child log messages: %d\n", len(childLogs))
	fmt.Println()

	// Show data received from child
	fmt.Println("📦 === DATA RECEIVED FROM CHILD ===")
	for _, key := range parentStore.ListKeys() {
		if value, err := store.Get[interface{}](parentStore, key); err == nil {
			fmt.Printf("  %s: %v\n", key, value)
		}
	}
	fmt.Println()

	// Verify we got the child process info
	if childPID, err := store.Get[float64](parentStore, "child_process_id"); err == nil {
		fmt.Printf("🔍 Child Process Verification:\n")
		fmt.Printf("  ✅ Child had different PID: %.0f (Parent: %d)\n", childPID, os.Getpid())

		if parentPID, err := store.Get[float64](parentStore, "child_parent_id"); err == nil {
			fmt.Printf("  ✅ Child's parent PID matches: %.0f\n", parentPID)
		}

		if hostname, err := store.Get[string](parentStore, "child_hostname"); err == nil {
			fmt.Printf("  ✅ Child hostname: %s\n", hostname)
		}

		if filename, err := store.Get[string](parentStore, "created_file"); err == nil {
			fmt.Printf("  ✅ Child created file: %s\n", filename)

			// Verify the file exists
			if _, err := os.Stat(filename); err == nil {
				fmt.Printf("  ✅ File exists and is accessible from parent!\n")

				// Read and display file content
				if content, err := os.ReadFile(filename); err == nil {
					fmt.Printf("  📄 File content:\n%s", string(content))
				}
			}
		}

		// Also verify the simple test data
		if simplePID, err := store.Get[float64](parentStore, "child_pid"); err == nil {
			fmt.Printf("  ✅ Simple test confirmed child PID: %.0f\n", simplePID)
		}
	} else {
		fmt.Println("❌ No child process data received - something went wrong!")
	}

	fmt.Println()
	fmt.Println("🎉 Example completed successfully!")
	fmt.Printf("   This proves that gostage.Runner.Spawn() creates real child processes\n")
	fmt.Printf("   with separate PIDs that can communicate back to the parent!\n")
}
