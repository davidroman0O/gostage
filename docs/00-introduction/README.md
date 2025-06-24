# Introduction to gostage

Welcome to `gostage`! This library is a powerful and flexible workflow orchestration engine designed for Go applications. It enables you to build complex, multi-stage, and stateful processes in a structured and manageable way.

## What is `gostage`?

At its core, `gostage` is a framework for defining and executing workflows. A workflow is a sequence of operations, and `gostage` provides the building blocks to organize these operations logically. It's particularly well-suited for tasks that involve:

-   **Multi-step processes**: ETL (Extract, Transform, Load) pipelines, data processing, infrastructure provisioning, and continuous integration/delivery (CI/CD) pipelines.
-   **Stateful operations**: Workflows where the output of one step is the input for another.
-   **Dynamic and adaptive logic**: Workflows that can change their behavior at runtime based on intermediate results.
-   **Isolated execution**: The ability to run parts of a workflow in separate processes for better stability and resource management.

## Core Philosophy

The design of `gostage` is guided by a few key principles:

-   **Composability**: Workflows are built from smaller, reusable components (`Actions` and `Stages`), making them easy to assemble and reason about.
-   **Flexibility**: Workflows are not static. They can be modified at runtime, allowing for highly adaptive and intelligent systems.
-   **Statefulness**: `gostage` comes with a built-in, type-safe key-value store, making it easy to manage state across the entire workflow.
-   **Extensibility**: Through middleware, you can add cross-cutting concerns like logging, timing, and error handling without cluttering your core business logic.

## "Hello, World!" Example

The best way to understand `gostage` is to see it in action. Let's build a simple workflow that prints "Hello, gostage World!".

You can find the full runnable code for this example at [`examples/01-hello-workflow/main.go`](./examples/01-hello-workflow/main.go).

### Step 1: Define an Action

First, we define a custom `Action`. An action is a struct that implements the `gostage.Action` interface. The easiest way is to embed `gostage.BaseAction` and implement an `Execute` method.

```go
// HelloWorldAction is a custom action that prints a greeting.
type HelloWorldAction struct {
	gostage.BaseAction
}

// Execute is where the action's logic resides.
func (a *HelloWorldAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("Hello, gostage World!")
	return nil
}
```

### Step 2: Build the Workflow

Inside our `main` function, we assemble the workflow.

```go
// 1. Create a workflow.
workflow := gostage.NewWorkflow(
    "hello-workflow",
    "Hello Workflow",
    "A simple workflow to print a message.",
)

// 2. Create a stage to hold our action.
stage := gostage.NewStage(
    "greeting-stage",
    "Greeting Stage",
    "This stage contains our greeting action.",
)

// 3. Create an instance of our action.
helloAction := &HelloWorldAction{
    BaseAction: gostage.NewBaseAction("hello-action", "Prints a greeting"),
}

// 4. Add the action to the stage, and the stage to the workflow.
stage.AddAction(helloAction)
workflow.AddStage(stage)
```

### Step 3: Run the Workflow

Finally, we use a `Runner` to execute the workflow.

```go
// 5. Create a runner and execute the workflow.
runner := gostage.NewRunner()
fmt.Println("Executing workflow...")
err := runner.Execute(context.Background(), workflow, nil)
if err != nil {
    fmt.Printf("Workflow execution failed: %v\n", err)
}
fmt.Println("Workflow executed.")
```

This simple example demonstrates the basic pattern of building and running a workflow. Now that you have a basic idea of what `gostage` is, let's dive into the [Core Concepts](./../01-core-concepts/README.md). 