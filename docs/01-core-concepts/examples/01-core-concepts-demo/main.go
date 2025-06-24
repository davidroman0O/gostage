package main

import (
	"context"
	"fmt"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// --- Action Definitions ---

// PrepareDataAction sets up some initial data in the workflow's shared store.
type PrepareDataAction struct {
	gostage.BaseAction
}

func (a *PrepareDataAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("--- Executing PrepareDataAction in 'preparation' stage ---")
	// This data will be available to all subsequent stages via the shared store.
	err := ctx.Store().Put("shared.data", "Hello from the first stage!")
	if err != nil {
		return err
	}
	fmt.Println("  ✅ Wrote 'shared.data' to the store.")
	return nil
}

// ProcessDataAction reads the data from the store and acts on it.
type ProcessDataAction struct {
	gostage.BaseAction
}

func (a *ProcessDataAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("\n--- Executing ProcessDataAction in 'processing' stage ---")
	// We can retrieve the data set by the previous stage.
	data, err := store.Get[string](ctx.Store(), "shared.data")
	if err != nil {
		return fmt.Errorf("could not get 'shared.data' from store: %w", err)
	}

	fmt.Printf("  ✅ Read from store: '%s'\n", data)
	fmt.Println("  -> Data successfully processed.")
	return nil
}

func main() {
	// --- Workflow Assembly ---

	// 1. Create the top-level workflow.
	workflow := gostage.NewWorkflow(
		"core-concepts-workflow",
		"Core Concepts Demo",
		"A workflow to demonstrate the main components.",
	)

	// 2. Create the first stage and add the preparation action to it.
	prepStage := gostage.NewStage("preparation", "Preparation Stage", "Prepares initial data.")
	prepStage.AddAction(&PrepareDataAction{
		BaseAction: gostage.NewBaseAction("prepare-data", "Prepares data"),
	})

	// 3. Create the second stage and add the processing action to it.
	processStage := gostage.NewStage("processing", "Processing Stage", "Processes the prepared data.")
	processStage.AddAction(&ProcessDataAction{
		BaseAction: gostage.NewBaseAction("process-data", "Processes data"),
	})

	// 4. Add the stages to the workflow in the desired order of execution.
	workflow.AddStage(prepStage)
	workflow.AddStage(processStage)

	// --- Execution ---

	// 5. Create a runner and execute the workflow.
	runner := gostage.NewRunner()
	fmt.Println("Executing Core Concepts workflow...")
	if err := runner.Execute(context.Background(), workflow, nil); err != nil {
		fmt.Printf("Workflow execution failed: %v\n", err)
	}
	fmt.Println("\nWorkflow completed.")
}
