# 03 - Actions

The **Action** is the most fundamental unit of work in `gostage`. It's where your actual business logic lives. Actions are contained within `Stages` and are executed sequentially.

## The Action Interface

Any type that satisfies the `gostage.Action` interface can be used as an action. The interface is simple:

```go
type Action interface {
    // Name returns the action's unique name.
    Name() string

    // Description provides a human-readable explanation of what the action does.
    Description() string

    // Tags returns a list of tags for organization and filtering.
    Tags() []string

    // Execute performs the action's work.
    Execute(ctx *ActionContext) error
}
```

## Creating a Custom Action

While you can implement this interface from scratch, the easiest way to create a new action is to embed the `gostage.BaseAction` struct. `BaseAction` provides the `Name()`, `Description()`, and `Tags()` methods, so you only need to implement `Execute`.

```go
import (
    "fmt"
    "github.com/davidroman0O/gostage"
)

// Define a custom action struct.
type GreetAction struct {
    gostage.BaseAction
    Greeting string
}

// Implement the Execute method for your action.
func (a *GreetAction) Execute(ctx *gostage.ActionContext) error {
    // Access the workflow store.
    name, err := store.Get[string](ctx.Store(), "user.name")
    if err != nil {
        name = "World" // Default value
    }

    fmt.Printf("%s, %s!\n", a.Greeting, name)
    return nil
}

// Create an instance of your action.
greetAction := &GreetAction{
    BaseAction: gostage.NewBaseAction("greet-user", "Greets the user"),
    Greeting:   "Hello",
}
```

## The ActionContext

The `Execute` method of every action receives an `ActionContext`. This context is your gateway to the rest of the workflow environment. It provides:

-   `ctx.GoContext`: The standard `context.Context` for cancellation and deadlines.
-   `ctx.Workflow`: A reference to the parent `Workflow` object.
-   `ctx.Stage`: A reference to the current `Stage` object.
-   `ctx.Action`: A reference to the current `Action` being executed.
-   `ctx.Store()`: A helper method to access the workflow's shared key-value store.
-   `ctx.Logger`: The logger instance for the workflow execution.

The `ActionContext` also provides methods for **dynamic workflow modification**, such as `AddDynamicAction()` and `AddDynamicStage()`, which we will cover in a later section.

---

This concludes the section on Core Concepts. You should now have a solid understanding of how to structure a workflow. Next, we will take a deeper look at [**State Management**](../02-state-management/README.md). 