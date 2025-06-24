# 02 - Integration Testing Stages

While unit tests are great for verifying the logic of a single `Action`, it's often necessary to test how multiple actions work together within a `Stage`. This is where integration testing for stages becomes important.

The goal of a stage integration test is to verify the flow of data and the combined side effects of all actions within that stage.

## Steps for Integration Testing a Stage

The easiest way to test a stage is to treat it as a mini-workflow. You create a workflow that contains *only* the stage you want to test and then execute it with a `Runner`.

1.  **Create a Test Workflow**: Instantiate a new `Workflow` for the test.
2.  **Create and Configure the Stage**: Instantiate the `Stage` you want to test and add all the `Actions` that are part of it.
3.  **Set Initial Data**: If the stage's actions require any data to be present in the store, you can set it on the stage's `initialStore` or the main workflow's store. For testing, it's often cleaner to use the stage's `initialStore`.
4.  **Add the Stage to the Workflow**: Add your configured stage to the test workflow.
5.  **Execute with a Runner**: Use `gostage.NewRunner()` to execute the test workflow. This will run all the actions in the stage in the correct order.
6.  **Assert the Results**: After the execution, inspect the workflow's store to verify the final state. Check that the actions produced the expected output and that any state transformations are correct.

## Example: Testing a User Processing Stage

Imagine a stage that first creates a user and then activates them.

**The Actions:**
```go
// Creates a user and puts them in the store.
type CreateUserAction struct{ gostage.BaseAction }
func (a *CreateUserAction) Execute(ctx *gostage.ActionContext) error {
    ctx.Store.Put("user.id", 123)
    ctx.Store.Put("user.status", "pending")
    return nil
}

// Reads the user status and changes it to "active".
type ActivateUserAction struct{ gostage.BaseAction }
func (a *ActivateUserAction) Execute(ctx *gostage.ActionContext) error {
    status, err := store.Get[string](ctx.Store(), "user.status")
    if err != nil || status != "pending" {
        return fmt.Errorf("user was not in 'pending' state")
    }
    return ctx.Store().Put("user.status", "active")
}
```

**The Test:**
```go
func TestUserProcessingStage(t *testing.T) {
    // 1. & 2. Create the test workflow and the stage with its actions.
    testWorkflow := gostage.NewWorkflow("test-wf", "", "")
    userStage := gostage.NewStage("user-processing", "", "")
    userStage.AddAction(&CreateUserAction{BaseAction: gostage.NewBaseAction("create", "")})
    userStage.AddAction(&ActivateUserAction{BaseAction: gostage.NewBaseAction("activate", "")})

    // 3. (Optional) Set initial data if needed. Not required for this example.

    // 4. Add the stage to the workflow.
    testWorkflow.AddStage(userStage)

    // 5. Execute the stage via the runner.
    runner := gostage.NewRunner()
    err := runner.Execute(context.Background(), testWorkflow, nil)

    // 6. Assert the results.
    assert.NoError(t, err, "The stage should execute without errors")

    // Check the final state in the store.
    finalStatus, getErr := store.Get[string](testWorkflow.Store, "user.status")
    assert.NoError(t, getErr)
    assert.Equal(t, "active", finalStatus, "User status should be 'active' after the stage runs")
}
```

This method provides a high-fidelity test of your stage's behavior, as it uses the same execution engine that will run your code in production. 