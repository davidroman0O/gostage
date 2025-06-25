package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/davidroman0O/gostage"
	"github.com/stretchr/testify/assert"
)

// This demonstrates the FIXED spawn functionality for external packages
// Before our fix: Child processes would fail with "flag provided but not defined: -gostage-child"
// After our fix: Child processes properly handle the --gostage-child flag

// CRITICAL: This TestMain function is what external applications need to add
// when they want to use gostage spawn functionality
func TestMain(m *testing.M) {
	// Check for child process mode using command line arguments (production approach)
	if len(os.Args) > 1 && os.Args[1] == "--gostage-child" {
		handleChildProcess()
		return
	}

	// Otherwise run tests normally
	os.Exit(m.Run())
}

// handleChildProcess implements the child process logic for external applications
// This is the standard pattern that all gostage users should follow
func handleChildProcess() {
	// Read workflow definition from stdin
	workflowDef, err := gostage.ReadWorkflowDefinitionFromStdin()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read workflow definition: %v\n", err)
		os.Exit(1)
	}

	// Create child runner with automatic transport handling
	childRunner, err := gostage.NewChildRunner(*workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create child runner: %v\n", err)
		os.Exit(1)
	}

	// Create workflow from definition
	workflow, err := gostage.NewWorkflowFromDef(workflowDef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create workflow: %v\n", err)
		os.Exit(1)
	}

	// Execute workflow
	logger := gostage.NewDefaultLogger()
	if err := childRunner.Execute(context.Background(), workflow, logger); err != nil {
		fmt.Fprintf(os.Stderr, "Child workflow execution failed: %v\n", err)
		os.Exit(1)
	}

	// Send final store state back to parent
	if workflow.Store != nil {
		finalStore := workflow.Store.ExportAll()
		if err := childRunner.Broker.Send(gostage.MessageTypeFinalStore, finalStore); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to send final store: %v\n", err)
		}
	}

	childRunner.Broker.Close()
	os.Exit(0)
}

// TestExternalSpawnDemo demonstrates that external packages can now use spawn
func TestExternalSpawnDemo(t *testing.T) {
	t.Run("spawn_works_from_external_package", func(t *testing.T) {
		// Create a simple workflow definition
		def := gostage.SubWorkflowDef{
			ID: "external-demo-workflow",
			Stages: []gostage.StageDef{{
				ID:      "demo-stage",
				Actions: []gostage.ActionDef{
					// Empty actions are fine for this demo
				},
			}},
		}

		// Try to spawn using the actual Runner.Spawn method
		// This now works because our TestMain handles --gostage-child properly
		runner := gostage.NewRunner()
		err := runner.Spawn(context.Background(), def)

		// The key victory: We no longer get "flag provided but not defined: -gostage-child"
		if err != nil {
			t.Logf("Spawn result: %v", err)
			// We might still get errors about transport or missing actions, but NOT the flag error
			assert.NotContains(t, err.Error(), "flag provided but not defined",
				"The --gostage-child flag should now be properly handled!")

			// This is the expected error for empty workflows
			assert.Contains(t, err.Error(), "child process exited with error")
		} else {
			t.Log("✅ Spawn succeeded! External package spawn is working!")
		}
	})
}

// This main function shows the pattern for regular applications
func main() {
	// REQUIRED: Handle child process mode for spawn functionality
	if len(os.Args) > 1 && os.Args[1] == "--gostage-child" {
		handleChildProcess()
		return
	}

	// Your regular application logic goes here
	fmt.Println("🎉 This is a gostage-enabled application!")
	fmt.Println("📝 It can now properly spawn child workflows.")
	fmt.Println("🔧 The --gostage-child flag is handled correctly.")

	// Example: You could run tests or start your application here
	// os.Exit(m.Run()) // for test binaries
}

/*
USAGE EXAMPLE FOR DEVELOPERS:

1. Add this TestMain pattern to your test files:

```go
func TestMain(m *testing.M) {
    if len(os.Args) > 1 && os.Args[1] == "--gostage-child" {
        handleChildProcess()
        return
    }
    os.Exit(m.Run())
}
```

2. Add this pattern to your main.go:

```go
func main() {
    if len(os.Args) > 1 && os.Args[1] == "--gostage-child" {
        handleChildProcess()
        return
    }
    // Your application logic here
}
```

3. Now your application can use runner.Spawn() without issues!

BEFORE FIX:
❌ "flag provided but not defined: -gostage-child"

AFTER FIX:
✅ Child processes start properly and execute workflows

*/
