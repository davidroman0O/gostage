package main

import (
	"context"
	"fmt"

	"github.com/davidroman0O/gostage"
)

// HelloWorldAction is a custom action that prints a greeting.
// It embeds gostage.BaseAction to get the basic Action functionality.
type HelloWorldAction struct {
	gostage.BaseAction
}

// Execute is where the action's logic resides.
func (a *HelloWorldAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("Hello, gostage World!")
	return nil
}

func main() {
	// 1. Create a new workflow. This is the top-level container for our process.
	workflow := gostage.NewWorkflow(
		"hello-workflow",
		"Hello Workflow",
		"A simple workflow to print a message.",
	)

	// 2. Create a stage. Stages are logical phases within a workflow.
	stage := gostage.NewStage(
		"greeting-stage",
		"Greeting Stage",
		"This stage contains the action that prints our greeting.",
	)

	// 3. Create an instance of our custom action.
	helloAction := &HelloWorldAction{
		BaseAction: gostage.NewBaseAction("hello-action", "Prints a greeting"),
	}

	// 4. Add the action to the stage, and the stage to the workflow.
	// Actions are executed in the order they are added.
	// Stages are executed in the order they are added.
	stage.AddAction(helloAction)
	workflow.AddStage(stage)

	// 5. Create a runner to execute the workflow.
	runner := gostage.NewRunner()

	fmt.Println("Executing workflow...")

	// Execute the workflow. We pass a background context and a nil logger for this simple example.
	err := runner.Execute(context.Background(), workflow, nil)
	if err != nil {
		fmt.Printf("Workflow execution failed: %v\n", err)
	}

	fmt.Println("Workflow executed.")
}
