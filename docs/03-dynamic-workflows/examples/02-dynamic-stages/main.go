package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// --- Action for a dynamically created stage ---
type ProcessResourceAction struct {
	gostage.BaseAction
	ResourceType string
}

func (a *ProcessResourceAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Printf("  -> Running dynamically generated stage for resource type: '%s'\n", a.ResourceType)
	// In a real scenario, this action would perform the actual processing
	// for the specific resource type (e.g., connect to a database, resize an image).

	// We can add results to the store.
	results, _ := store.GetOrDefault(ctx.Store(), "processing.results", []string{})
	results = append(results, fmt.Sprintf("Processed %s", a.ResourceType))
	ctx.Store().Put("processing.results", results)

	return nil
}

// --- Action that discovers work and creates dynamic stages ---
type DiscoverWorkAction struct {
	gostage.BaseAction
}

func (a *DiscoverWorkAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("--- DiscoverWorkAction running ---")
	// Simulate discovering different types of work that need to be done.
	// This could come from a config file, a database query, or an API call.
	workItems := []string{"database", "filesystem", "api-cache"}
	fmt.Printf("  ✅ Discovered %d types of work: %v\n", len(workItems), workItems)

	// For each type of work, create a dedicated stage with its own actions.
	for _, workType := range workItems {
		// Create a new stage tailored to this type of work.
		newStage := gostage.NewStage(
			fmt.Sprintf("process-%s-stage", workType),
			fmt.Sprintf("Process %s", strings.Title(workType)),
			fmt.Sprintf("A dynamically generated stage to handle %s processing.", workType),
		)

		// Add a specific action to the new stage.
		newStage.AddAction(&ProcessResourceAction{
			BaseAction: gostage.NewBaseAction(
				fmt.Sprintf("process-%s-action", workType),
				"Processes a specific resource type",
			),
			ResourceType: workType,
		})

		// Add the newly created stage to the workflow.
		// It will be inserted into the execution plan right after the current stage.
		ctx.AddDynamicStage(newStage)
		fmt.Printf("  ✅ Injected dynamic stage for '%s'.\n", workType)
	}

	return nil
}

// --- Final verification action ---
type VerifyAllWorkDoneAction struct {
	gostage.BaseAction
}

func (a *VerifyAllWorkDoneAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("\n--- VerifyAllWorkDoneAction running ---")
	results, err := store.Get[[]string](ctx.Store(), "processing.results")
	if err != nil {
		return fmt.Errorf("could not find processing results in store: %w", err)
	}

	fmt.Printf("  - Verification complete. Found %d processing results:\n", len(results))
	for _, res := range results {
		fmt.Printf("    - %s\n", res)
	}

	if len(results) != 3 {
		return fmt.Errorf("expected 3 processing results, but got %d", len(results))
	}

	fmt.Println("  ✅ All dynamically generated stages executed successfully.")
	return nil
}

func main() {
	workflow := gostage.NewWorkflow(
		"dynamic-stages-workflow",
		"Dynamic Stages Workflow",
		"Demonstrates creating and executing new stages at runtime.",
	)

	// The workflow initially only has two stages.
	// The dynamic stages will be inserted between them.
	discoveryStage := gostage.NewStage("discovery-stage", "Discovery", "Discovers work to be done")
	discoveryStage.AddAction(&DiscoverWorkAction{
		BaseAction: gostage.NewBaseAction("discover-work", "Finds work and creates stages"),
	})

	verificationStage := gostage.NewStage("verification-stage", "Verification", "Verifies all work was completed")
	verificationStage.AddAction(&VerifyAllWorkDoneAction{
		BaseAction: gostage.NewBaseAction("verify-work", "Verifies dynamic stages ran"),
	})

	workflow.AddStage(discoveryStage)
	workflow.AddStage(verificationStage)

	// Execute the workflow
	runner := gostage.NewRunner()
	fmt.Printf("Executing workflow (initially has %d stages)...\n", len(workflow.Stages))
	if err := runner.Execute(context.Background(), workflow, nil); err != nil {
		fmt.Printf("Workflow execution failed: %v\n", err)
	}
	fmt.Printf("\nWorkflow completed (finished with %d stages).\n", len(workflow.Stages))
}
