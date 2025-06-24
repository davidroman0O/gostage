# 02 - Stages

A **Stage** represents a logical phase of a `Workflow`. Each workflow is composed of one or more stages that are executed in the order they are added. A stage, in turn, is a container for a sequence of `Actions`.

## Creating a Stage

Similar to a workflow, a stage is created with a unique ID, a name, and a description.

```go
import "github.com/davidroman0O/gostage"

// Create a new stage
stage := gostage.NewStage(
    "data-validation",
    "Data Validation",
    "This stage validates the input data.",
)
```

You can also create a stage with tags for filtering and categorization.

```go
validationStage := gostage.NewStageWithTags(
    "validation",
    "Validation Stage",
    "Validates incoming data.",
    []string{"validation", "pre-processing"},
)
```

## Adding Actions

The primary purpose of a stage is to group and execute a sequence of actions. You can add actions to a stage using the `AddAction` method. The actions will be executed sequentially in the order they are added.

```go
// Create some actions
action1 := &MyAction1{}
action2 := &MyAction2{}

// Add actions to the stage
stage.AddAction(action1)
stage.AddAction(action2)

// When the stage is executed, action1 will run before action2.
```

## Stage-specific Initial Data

Each stage can have its own initial key-value store (`initialStore`). When a stage begins execution, the data from its `initialStore` is merged into the main `Workflow` store.

This is useful for providing stage-specific configuration or data without cluttering the main workflow store until it's needed.

-   **Isolation**: Stage-specific data is kept separate until the stage runs.
-   **Overwriting**: If a key exists in both the stage's `initialStore` and the workflow's main store, the value from the stage will overwrite the workflow's value for the duration of that stage and subsequent stages.

```go
// Set some initial data for a specific stage
stage.SetInitialData("api.endpoint", "https://api.example.com/v2")
stage.SetInitialData("api.timeout", "30s")

// When this stage runs, these values will be available in the main workflow store.
```

## Stage Middleware

Stages also support middleware, which allows you to wrap the execution of all actions within that stage. This is perfect for tasks like:

-   Setting up and tearing down resources that are only needed for a specific stage (e.g., a database connection).
-   Implementing stage-level timing or logging.
-   Handling errors specific to a stage's actions.

We will cover middleware in more detail in a later section.

---

Next, let's look at the smallest and most fundamental unit of work: [**Actions**](./03-actions.md). 