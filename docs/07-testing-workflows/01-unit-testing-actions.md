# 01 - Unit Testing Actions

Unit testing your `Actions` is the most effective way to ensure your core business logic is correct and robust. Because an action is just a struct that satisfies an interface, you can instantiate it directly in a test and call its `Execute` method without needing a full workflow runner.

The key to unit testing an action is to construct a mock `ActionContext`. This allows you to control the inputs and dependencies that your action receives.

## Steps for Unit Testing an Action

1.  **Create a Test Workflow and Store**: Even for a unit test, the `ActionContext` needs a `Workflow` object, which contains the `KVStore`. Create a simple, empty workflow just for the test.
2.  **Populate the Store**: Use the test workflow's store to `Put` any data that your action expects to find when it runs. This is how you set up the "pre-conditions" for your test.
3.  **Instantiate Your Action**: Create an instance of the action you want to test.
4.  **Create an `ActionContext`**: Manually create an `ActionContext` struct, passing it the test workflow and a logger.
5.  **Execute the Action**: Call the action's `Execute` method directly with the mock context.
6.  **Assert the Results**: Check the error returned by `Execute` and inspect the test workflow's store to verify that the action produced the correct output and side effects.

## Example: Testing a `SumAction`

Let's say we have an action that reads two numbers from the store, adds them, and writes the result back.

**The Action:**
```go
type SumAction struct {
    gostage.BaseAction
}

func (a *SumAction) Execute(ctx *gostage.ActionContext) error {
    a, err := store.Get[int](ctx.Store(), "a")
    if err != nil { return err }

    b, err := store.Get[int](ctx.Store(), "b")
    if err != nil { return err }

    sum := a + b
    return ctx.Store().Put("sum", sum)
}
```

**The Test:**
```go
import (
    "context"
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/davidroman0O/gostage"
    "github.com/davidroman0O/gostage/store"
)

func TestSumAction(t *testing.T) {
    // 1. Create a test workflow to hold the store.
    testWorkflow := gostage.NewWorkflow("test-wf", "", "")

    // 2. Populate the store with input data.
    testWorkflow.Store.Put("a", 10)
    testWorkflow.Store.Put("b", 5)

    // 3. Instantiate the action.
    action := &SumAction{BaseAction: gostage.NewBaseAction("sum", "")}

    // 4. Create the mock ActionContext.
    actionCtx := &gostage.ActionContext{
        GoContext: context.Background(),
        Workflow:  testWorkflow,
        Logger:    nil, // Can use a test logger if needed.
    }

    // 5. Execute the action.
    err := action.Execute(actionCtx)

    // 6. Assert the results.
    assert.NoError(t, err, "Execute should not return an error")

    sum, getErr := store.Get[int](testWorkflow.Store, "sum")
    assert.NoError(t, getErr, "Should be able to get the sum from the store")
    assert.Equal(t, 15, sum, "The sum should be 15")
}
```

This approach lets you test your action's logic thoroughly without the overhead of the full workflow execution engine.

---

Next, we'll see how to test the interaction of multiple actions by [Integration Testing a Stage](./02-integration-testing-stages.md). 