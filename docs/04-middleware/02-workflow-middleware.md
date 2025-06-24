# 02 - Workflow Middleware

**Workflow Middleware** is the middle layer in the `gostage` middleware hierarchy. It's applied to a specific `Workflow` instance and wraps the execution of each `Stage` within that workflow.

If a workflow has three stages, a workflow middleware will run three times—once for each stage.

## The `WorkflowStageRunnerFunc` Signature

A workflow middleware takes a `WorkflowStageRunnerFunc` and returns a new one. This function signature gives you access to the stage that is about to be executed.

```go
type WorkflowStageRunnerFunc func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error
```

## Common Use Cases

Workflow middleware is perfect for logic that needs to be tied to a specific workflow and repeated for each of its stages.

-   **Stage-Level Monitoring**: Logging or recording metrics before and after each stage runs.
-   **Conditional Stage Execution**: Adding logic to skip certain stages based on their tags or properties in the store.
-   **Data Scoping**: Injecting data into the store that should only be available for the duration of a single stage and its successors.
-   **Stage-Specific Resource Management**: While less common than at the stage middleware level, you could acquire resources needed by a group of stages.

## Example: A Stage-Boundary Logging Middleware

Here is an example of a workflow middleware that logs a message every time a stage starts and finishes within a particular workflow.

```go
import (
    "context"
    "time"
    "github.com/davidroman0O/gostage"
)

// StageBoundaryLogger logs the start and end of each stage.
func StageBoundaryLogger() gostage.WorkflowMiddleware {
    return func(next gostage.WorkflowStageRunnerFunc) gostage.WorkflowStageRunnerFunc {
        return func(ctx context.Context, stage *gostage.Stage, wf *gostage.Workflow, logger gostage.Logger) error {
            // 1. Code here runs BEFORE the stage begins.
            logger.Info("==> Entering Stage: %s", stage.Name)

            // 2. Call next() to execute the stage.
            err := next(ctx, stage, wf, logger)

            // 3. Code here runs AFTER the stage completes.
            if err != nil {
                logger.Error("==> Exiting Stage '%s' with ERROR: %v", stage.Name, err)
            } else {
                logger.Info("==> Exiting Stage: %s", stage.Name)
            }

            // 4. Return the error.
            return err
        }
    }
}

// How to use it:
// workflow := gostage.NewWorkflow(...)
// workflow.Use(StageBoundaryLogger())
```

By applying this middleware directly to the workflow object using `workflow.Use()`, you can add specific behaviors to that workflow without affecting others executed by the same runner.

---

Next, we'll look at the innermost layer: [**Stage Middleware**](./03-stage-middleware.md). 