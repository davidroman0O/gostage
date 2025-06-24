# 01 - Workflows

A **Workflow** is the top-level container in `gostage`. It represents an entire process from start to finish and is responsible for orchestrating the execution of its `Stages`.

## Creating a Workflow

You can create a new workflow using the `gostage.NewWorkflow` function. Every workflow requires a unique ID, a human-readable name, and a description.

```go
import "github.com/davidroman0O/gostage"

// Create a new workflow
workflow := gostage.NewWorkflow(
    "my-first-workflow",
    "My First Workflow",
    "This workflow demonstrates the basic setup.",
)
```

You can also create a workflow with tags, which are useful for categorization and filtering, especially when you have many workflows.

```go
workflow := gostage.NewWorkflowWithTags(
    "data-pipeline",
    "Data Processing Pipeline",
    "An ETL pipeline for user data.",
    []string{"etl", "data", "production"},
)
```

## The Workflow Store

Every workflow instance comes with its own built-in, type-safe key-value store, powered by `store.KVStore`. This store is one of the most powerful features of `gostage`, as it allows you to manage state across the entire workflow.

-   **Shared State**: All stages and actions within a workflow have access to the same store instance.
-   **Passing Data**: An action can write data to the store, and a subsequent action (even in a different stage) can read that data.
-   **Type-Safe**: The store uses generics to preserve the concrete Go types of your data, eliminating the need for constant type assertions.

You can access the store via the `workflow.Store` field or through the `ActionContext` passed to every action's `Execute` method.

```go
// Put data into the workflow's store
workflow.Store.Put("config.retries", 3)

// In an action...
func (a *MyAction) Execute(ctx *gostage.ActionContext) error {
    retries, err := store.Get[int](ctx.Store(), "config.retries")
    if err != nil {
        return err
    }
    // ... use the value
    return nil
}
```

## Adding Stages

A workflow isn't useful without stages. You add stages to a workflow using the `AddStage` method. The stages will be executed sequentially in the order they are added.

```go
// Create two stages
stageA := gostage.NewStage("stage-a", "Stage A", "First phase")
stageB := gostage.NewStage("stage-b", "Stage B", "Second phase")

// Add them to the workflow
workflow.AddStage(stageA)
workflow.AddStage(stageB)

// When executed, stageA will run before stageB.
```

## Execution

Workflows are executed by a `Runner`. The runner manages the execution lifecycle and the application of any middleware.

```go
import (
    "context"
    "github.com/davidroman0O/gostage"
)

runner := gostage.NewRunner()
err := runner.Execute(context.Background(), workflow, nil) // Logger is nil for simplicity
```

---

Next, let's dive deeper into the components that make up a workflow: [**Stages**](./02-stages.md). 