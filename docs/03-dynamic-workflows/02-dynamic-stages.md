# 02 - Dynamic Stages

Dynamic stages take the concept of runtime modification a step further. Instead of just adding actions to the current stage, you can inject entirely new stages into the workflow. These new stages will be executed immediately after the current stage completes, before the workflow proceeds to the next originally defined stage.

## How to Add a Dynamic Stage

From within an action's `Execute` method, you use `ctx.AddDynamicStage()` to add a new stage to the workflow.

```go
func (a *MyResourceDiscoveryAction) Execute(ctx *gostage.ActionContext) error {
    ctx.Logger.Info("Discovering resources...")

    // Imagine we discover different types of resources that need different processing.
    discoveredResourceTypes := []string{"databases", "caches", "servers"}

    for _, resourceType := range discoveredResourceTypes {
        // Create a new stage tailored to this resource type.
        newStage := gostage.NewStage(
            fmt.Sprintf("process-%s", resourceType),
            fmt.Sprintf("Process %s", resourceType),
            "A dynamically generated stage.",
        )

        // You can add actions to this new stage as you normally would.
        newStage.AddAction(&ProcessResourceAction{Type: resourceType})

        // Add the new stage to the workflow's execution plan.
        ctx.AddDynamicStage(newStage)
        ctx.Logger.Info("Dynamically added stage to process: %s", resourceType)
    }

    return nil
}
```

## Execution Order

When you add dynamic stages, the runner modifies the workflow's sequence of stages.

Consider a workflow with three stages defined statically: `A`, `B`, `C`.

1.  `Stage A` executes.
2.  During its execution, an action within `Stage A` calls `AddDynamicStage` to add two new stages, `D1` and `D2`.
3.  After `Stage A` finishes, the runner executes the dynamic stages in the order they were added: `D1`, then `D2`.
4.  Once all dynamic stages are complete, the runner proceeds to the next *original* stage, `Stage B`.
5.  Finally, `Stage C` executes.

The final execution order for the workflow will be: `Stage A` -> `Stage D1` -> `Stage D2` -> `Stage B` -> `Stage C`.

This is extremely powerful for building workflows that can adapt to their environment. A common pattern is to have an initial "discovery" or "planning" stage that inspects a system and then generates the precise set of subsequent stages required to handle the current state of that system.

---

This concludes the section on Dynamic Workflows. Next, we will explore one of the most important concepts for building maintainable and reusable workflows: [**Middleware**](../04-middleware/README.md). 