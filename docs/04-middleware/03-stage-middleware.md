# 03 - Stage Middleware

**Stage Middleware** is the innermost layer of the `gostage` middleware hierarchy. It is applied to a specific `Stage` and wraps the execution of the *entire block* of actions within that stage.

This is the most granular level of "group" middleware and is perfectly suited for managing resources or context that is only relevant to the actions within that single stage.

## The `StageRunnerFunc` Signature

A stage middleware takes a `StageRunnerFunc` and returns a new one. The `StageRunnerFunc` signature is similar to the others but is focused on the execution of the actions within the stage.

```go
type StageRunnerFunc func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error
```

When you call `next()` inside a stage middleware, you are triggering the execution of all the actions within that stage, one by one.

## Common Use Cases

-   **Resource Management**: The most common use case. Set up a resource (like a database transaction or a temporary file) before the first action runs, and tear it down after the last action completes.
-   **Action-Group Timing**: Measure the total time it takes to execute all actions within a specific stage.
-   **Error Handling**: Implement custom error handling or recovery logic that only applies to the actions within one stage.
-   **Data Validation**: Validate that the necessary data is present in the store before the stage's actions attempt to use it.

## Example: Database Transaction Middleware

Here is a classic example of a stage middleware that simulates managing a database transaction for all actions within that stage.

```go
import (
    "context"
    "fmt"
    "github.com/davidroman0O/gostage"
)

// DBTxMiddleware simulates wrapping a stage's actions in a database transaction.
func DBTxMiddleware() gostage.StageMiddleware {
    return func(next gostage.StageRunnerFunc) gostage.StageRunnerFunc {
        return func(ctx context.Context, stage *gostage.Stage, wf *gostage.Workflow, logger gostage.Logger) error {
            // 1. Code here runs BEFORE any action in the stage.
            logger.Info("BEGIN TRANSACTION for stage '%s'", stage.Name)
            // In a real app, you would start the transaction here and
            // maybe put the transaction object in the store for actions to use.
            // wf.Store.Put("db.tx", tx)

            // 2. Call next() to execute all actions in the stage.
            err := next(ctx, stage, wf, logger)

            // 3. Code here runs AFTER all actions in the stage have completed.
            if err != nil {
                // If any action failed, roll back the transaction.
                logger.Error("Error in stage '%s', ROLLING BACK TRANSACTION.", stage.Name)
                // tx.Rollback()
            } else {
                // If all actions succeeded, commit the transaction.
                logger.Info("COMMIT TRANSACTION for stage '%s'", stage.Name)
                // tx.Commit()
            }

            // wf.Store.Delete("db.tx")

            // 4. Return the error.
            return err
        }
    }
}

// How to use it:
// stage := gostage.NewStage(...)
// stage.Use(DBTxMiddleware())
```

By applying this middleware directly to the stage object using `stage.Use()`, you ensure that this transactional logic only applies to the actions within this specific stage.

---

This concludes the section on Middleware. You now have the tools to separate concerns and build robust, reusable workflow components. Next, we will cover [**Spawning Sub-Workflows**](../05-spawning-subworkflows/README.md). 