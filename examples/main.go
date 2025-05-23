package main

import (
	"context"
	"fmt"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// Define a custom action by embedding BaseAction
type GreetingAction struct {
	gostage.BaseAction
}

// Implement the Execute method required by the Action interface
func (a GreetingAction) Execute(ctx *gostage.ActionContext) error {
	name, err := store.GetOrDefault(ctx.Store(), "user.name", "World")
	if err != nil {
		return err
	}

	ctx.Logger.Info("Hello, %s!", name)
	return nil
}

// MyAction is a simple action implementation
type MyAction struct {
	gostage.BaseAction
}

// Execute implements the Action interface
func (a *MyAction) Execute(ctx *gostage.ActionContext) error {
	// Get configuration or use a default
	timeout, err := store.GetOrDefault[int](ctx.Store(), "timeout", 30)
	if err != nil {
		return err
	}

	ctx.Logger.Info("Using timeout: %d seconds", timeout)
	// ... implementation ...

	return nil
}

func main() {
	// Create a new workflow
	wf := gostage.NewWorkflow(
		"hello-world",
		"Hello World Workflow",
		"A simple introductory workflow",
	)

	// Create a stage
	stage := gostage.NewStage(
		"greeting",
		"Greeting Stage",
		"Demonstrates a simple greeting",
	)

	// Add actions to the stage
	stage.AddAction(&GreetingAction{
		BaseAction: gostage.NewBaseAction("greet", "Greeting Action"),
	})

	// Add the stage to the workflow
	wf.AddStage(stage)

	// Set up a logger
	logger := gostage.NewDefaultLogger()

	// Create a runner
	runner := gostage.NewRunner()

	// Execute the workflow
	if err := runner.Execute(context.Background(), wf, logger); err != nil {
		fmt.Printf("Error executing workflow: %v\n", err)
		return
	}

	fmt.Println("Workflow completed successfully!")
}
