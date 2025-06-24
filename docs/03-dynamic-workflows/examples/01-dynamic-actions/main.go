package main

import (
	"context"
	"fmt"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// --- Action to Process a Single Item ---
type ProcessItemAction struct {
	gostage.BaseAction
	Item string
}

func (a *ProcessItemAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Printf("  -> Dynamically processing item: %s\n", a.Item)
	// You could do more complex work here, like making an API call for the item.

	// Append to a results list in the store
	results, _ := store.GetOrDefault(ctx.Store(), "processed.items", []string{})
	results = append(results, a.Item)
	ctx.Store().Put("processed.items", results)

	return nil
}

// --- Action to Discover Items and Generate Dynamic Actions ---
type DiscoverItemsAction struct {
	gostage.BaseAction
}

func (a *DiscoverItemsAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("--- DiscoverItemsAction running ---")
	// In a real-world scenario, this might be a database query or API call.
	discoveredItems := []string{"item-a", "item-b", "item-c"}
	fmt.Printf("  ✅ Discovered %d items to process.\n", len(discoveredItems))
	ctx.Store().Put("discovered.items", discoveredItems)

	// For each discovered item, create and add a dynamic action.
	for _, item := range discoveredItems {
		// Create a new action specifically for this item.
		processAction := &ProcessItemAction{
			BaseAction: gostage.NewBaseAction(
				fmt.Sprintf("process-%s", item),
				fmt.Sprintf("Processes the discovered item: %s", item),
			),
			Item: item,
		}
		// Add the action to the current stage's execution queue.
		ctx.AddDynamicAction(processAction)
	}
	fmt.Printf("  ✅ Injected %d dynamic ProcessItemAction instances.\n", len(discoveredItems))
	return nil
}

// --- Verification Action ---
type VerifyProcessingAction struct {
	gostage.BaseAction
}

func (a *VerifyProcessingAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("\n--- VerifyProcessingAction running ---")
	processedItems, err := store.Get[[]string](ctx.Store(), "processed.items")
	if err != nil {
		return fmt.Errorf("failed to get processed items from store: %w", err)
	}
	discoveredItems, _ := store.Get[[]string](ctx.Store(), "discovered.items")

	fmt.Printf("  - Verification complete:\n")
	fmt.Printf("    - Discovered: %d items\n", len(discoveredItems))
	fmt.Printf("    - Processed:  %d items\n", len(processedItems))

	if len(discoveredItems) != len(processedItems) {
		return fmt.Errorf("mismatch between discovered and processed items")
	}
	fmt.Printf("  ✅ All discovered items were processed successfully.\n")
	return nil
}

func main() {
	workflow := gostage.NewWorkflow(
		"dynamic-actions-workflow",
		"Dynamic Actions Workflow",
		"Demonstrates adding new actions to a stage at runtime.",
	)

	stage := gostage.NewStage("processing-stage", "Processing Stage", "")

	// This stage starts with just two actions.
	// The dynamic actions will be inserted between them.
	stage.AddAction(&DiscoverItemsAction{
		BaseAction: gostage.NewBaseAction("discover-items", "Discovers items and creates processing actions"),
	})
	stage.AddAction(&VerifyProcessingAction{
		BaseAction: gostage.NewBaseAction("verify-processing", "Verifies all items were processed"),
	})

	workflow.AddStage(stage)

	runner := gostage.NewRunner()
	fmt.Println("Executing workflow with dynamic actions...")
	if err := runner.Execute(context.Background(), workflow, nil); err != nil {
		fmt.Printf("Workflow execution failed: %v\n", err)
	}
	fmt.Println("\nWorkflow completed.")
}
