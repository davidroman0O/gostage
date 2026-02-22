package gostage

import (
	"context"
	"fmt"
	"os"
	"time"
)

// IsChild returns true if this process was spawned by a parent gostage engine.
// It detects the --gostage-child flag that the parent adds when spawning.
//
//	func main() {
//	    if gostage.IsChild() {
//	        registerChildTasks()
//	        gostage.HandleChild()
//	        return
//	    }
//	    // parent code...
//	}
func IsChild() bool {
	for _, arg := range os.Args[1:] {
		if arg == "--gostage-child" {
			return true
		}
	}
	return false
}

// HandleChild connects to the parent process, receives a workflow definition,
// executes it, sends back the final store state, and exits.
// This replaces the manual NewChildRunner → RequestWorkflowDefinition →
// Execute → Send FinalStore → Close boilerplate.
//
// HandleChild will os.Exit(1) on fatal errors, or return normally on success.
func HandleChild() {
	if err := handleChildInternal(); err != nil {
		fmt.Fprintf(os.Stderr, "gostage child error: %v\n", err)
		os.Exit(1)
	}
}

func handleChildInternal() error {
	// Create the child runner (parses --grpc-address and --grpc-port from args)
	childRunner, logger, err := NewChildRunner()
	if err != nil {
		return fmt.Errorf("failed to create child runner: %w", err)
	}
	defer childRunner.Close()

	// Signal readiness and request the workflow definition from parent
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workflowDef, err := childRunner.RequestWorkflowDefinitionFromParent(ctx)
	if err != nil {
		return fmt.Errorf("failed to get workflow definition: %w", err)
	}

	// Build the workflow from the definition (uses the action registry)
	workflow, err := NewWorkflowFromDef(workflowDef)
	if err != nil {
		return fmt.Errorf("failed to create workflow from definition: %w", err)
	}

	// Execute the workflow
	execCtx := context.Background()
	if err := childRunner.Execute(execCtx, workflow, logger); err != nil {
		// Send error result to parent before returning
		childRunner.Broker.Send(MessageTypeWorkflowResult, map[string]any{
			"success": false,
			"error":   err.Error(),
		})
		return fmt.Errorf("workflow execution failed: %w", err)
	}

	// Send final store state back to parent
	finalStore := workflow.Store.ExportAll()
	if err := childRunner.Broker.Send(MessageTypeFinalStore, finalStore); err != nil {
		return fmt.Errorf("failed to send final store: %w", err)
	}

	return nil
}
