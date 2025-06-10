package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/davidroman0O/gostage"
)

// CrossPlatformAction demonstrates platform-specific information
type CrossPlatformAction struct {
	gostage.BaseAction
}

func (a *CrossPlatformAction) Execute(ctx *gostage.ActionContext) error {
	processID := os.Getpid()

	// Get platform information
	goos := runtime.GOOS
	goarch := runtime.GOARCH
	tempDir := os.TempDir()
	workingDir, _ := os.Getwd()
	hostname, _ := os.Hostname()

	ctx.Logger.Info("=== CROSS-PLATFORM PROCESS INFO ===")
	ctx.Logger.Info("Process ID: %d", processID)
	ctx.Logger.Info("Parent Process ID: %d", os.Getppid())
	ctx.Logger.Info("Operating System: %s", goos)
	ctx.Logger.Info("Architecture: %s", goarch)
	ctx.Logger.Info("Hostname: %s", hostname)
	ctx.Logger.Info("Temp Directory: %s", tempDir)
	ctx.Logger.Info("Working Directory: %s", workingDir)

	// Create a cross-platform file
	filename := filepath.Join(tempDir, fmt.Sprintf("gostage_test_%d.txt", processID))
	content := fmt.Sprintf(`GoStage Cross-Platform Test
Process ID: %d
Parent PID: %d  
Platform: %s/%s
Hostname: %s
Timestamp: %s
Temp Dir: %s
Working Dir: %s
`, processID, os.Getppid(), goos, goarch, hostname,
		time.Now().Format("2006-01-02 15:04:05"), tempDir, workingDir)

	ctx.Logger.Info("Creating cross-platform file: %s", filename)
	err := os.WriteFile(filename, []byte(content), 0644)
	if err != nil {
		ctx.Logger.Error("Failed to create file: %v", err)
		return err
	}

	ctx.Logger.Info("File created successfully on %s", goos)

	// Send platform info back to parent
	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "platform_info",
		"value": fmt.Sprintf("%s/%s", goos, goarch),
	})

	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "child_pid",
		"value": processID,
	})

	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "temp_file",
		"value": filename,
	})

	ctx.Send(gostage.MessageTypeStorePut, map[string]interface{}{
		"key":   "temp_dir",
		"value": tempDir,
	})

	return nil
}

// RunCrossPlatformTest demonstrates IPC working across platforms
func RunCrossPlatformTest() {
	fmt.Printf("🌍 === CROSS-PLATFORM IPC TEST ===\n")
	fmt.Printf("Running on: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("Parent PID: %d\n\n", os.Getpid())

	// Register the cross-platform action
	gostage.RegisterAction("cross-platform", func() gostage.Action {
		return &CrossPlatformAction{
			BaseAction: gostage.NewBaseAction("cross-platform", "Cross-platform compatibility test"),
		}
	})

	// Create runner
	runner := gostage.NewRunner()

	// Set up message handlers (same as before - cross-platform!)
	runner.Broker.RegisterHandler(gostage.MessageTypeLog, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var logData map[string]string
		json.Unmarshal(payload, &logData)

		level := logData["level"]
		message := logData["message"]

		fmt.Printf("[CHILD-%s] %s\n", level, message)
		return nil
	})

	runner.Broker.RegisterHandler(gostage.MessageTypeStorePut, func(msgType gostage.MessageType, payload json.RawMessage) error {
		var data map[string]interface{}
		json.Unmarshal(payload, &data)

		key, _ := data["key"].(string)
		value := data["value"]

		fmt.Printf("📦 [STORE] %s = %v\n", key, value)
		return nil
	})

	// Define workflow
	workflowDef := gostage.SubWorkflowDef{
		ID:          "cross-platform-test",
		Name:        "Cross-Platform IPC Test",
		Description: "Tests IPC compatibility across platforms",
		Stages: []gostage.StageDef{
			{
				ID: "platform-stage",
				Actions: []gostage.ActionDef{
					{ID: "cross-platform"},
				},
			},
		},
	}

	// Run the test
	fmt.Println("🚀 Starting cross-platform spawn test...")
	startTime := time.Now()
	err := runner.Spawn(context.Background(), workflowDef)
	duration := time.Since(startTime)

	if err != nil {
		fmt.Printf("❌ Error: %v\n", err)
	} else {
		fmt.Printf("\n✅ Cross-platform test completed successfully in %v!\n", duration)
		fmt.Printf("🌍 IPC worked perfectly on %s/%s\n", runtime.GOOS, runtime.GOARCH)

		// Platform-specific notes
		switch runtime.GOOS {
		case "windows":
			fmt.Println("📝 Windows: Uses named pipes and CreateProcess()")
		case "darwin":
			fmt.Println("📝 macOS: Uses POSIX pipes and fork()/exec()")
		case "linux":
			fmt.Println("📝 Linux: Uses POSIX pipes and fork()/exec()")
		default:
			fmt.Printf("📝 %s: Uses Go's standard cross-platform mechanisms\n", runtime.GOOS)
		}
	}
}

// Uncomment this to run the cross-platform test instead of the middleware demo
// func main() {
// 	// Check if this is a child process
// 	for _, arg := range os.Args[1:] {
// 		if arg == "--gostage-child" {
// 			childMain()
// 			return
// 		}
// 	}
//
// 	RunCrossPlatformTest()
// }
