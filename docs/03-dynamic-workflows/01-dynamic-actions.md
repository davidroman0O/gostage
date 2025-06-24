# 01 - Dynamic Actions

Dynamic actions allow you to inject new actions into the current stage's execution queue. The new actions will be executed immediately after the current action completes, before moving on to the next action that was originally defined in the stage.

## How to Add a Dynamic Action

From within any action's `Execute` method, you can use `ctx.AddDynamicAction()` to add one or more new actions.

```go
func (a *MyGeneratorAction) Execute(ctx *gostage.ActionContext) error {
    ctx.Logger.Info("Generator action is running...")

    // Some condition is met, so we decide to add more work.
    shouldAddMoreWork := true

    if shouldAddMoreWork {
        // Create a new action to be added dynamically.
        dynamicAction1 := &MyOtherAction{
            BaseAction: gostage.NewBaseAction("dynamic-action-1", "A dynamically added action"),
        }

        // Add it to the execution queue.
        ctx.AddDynamicAction(dynamicAction1)
        ctx.Logger.Info("Dynamically added 'dynamic-action-1'.")
    }

    return nil
}
```

## Execution Order

When you add dynamic actions, the runner adjusts the execution plan for the current stage.

Consider a stage with three actions defined statically: `A`, `B`, `C`.

1.  `Action A` executes.
2.  During its execution, `Action A` calls `AddDynamicAction` to add two new actions, `D1` and `D2`.
3.  After `Action A` finishes, the runner executes the dynamic actions in the order they were added: `D1`, then `D2`.
4.  Once all dynamic actions are complete, the runner proceeds to the next *original* action, `Action B`.
5.  Finally, `Action C` executes.

The final execution order for the stage will be: `A` -> `D1` -> `D2` -> `B` -> `C`.

This mechanism is particularly useful for "fan-out" scenarios where one action discovers multiple items that each need to be processed by a similar follow-up action.

---

Next, we'll look at an even more powerful feature: [**Dynamic Stages**](./02-dynamic-stages.md). 