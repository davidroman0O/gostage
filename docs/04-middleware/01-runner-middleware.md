# 01 - Runner Middleware

**Runner Middleware** is the outermost layer of middleware in `gostage`. It wraps the entire execution of a single `Workflow`. This makes it the ideal place for logic that needs to run once per workflow, such as global setup, teardown, high-level timing, and result handling.

## The `RunnerFunc` Signature

A runner middleware is a function that takes a `RunnerFunc` and returns a new `RunnerFunc`. The `RunnerFunc` has the following signature:

```go
type RunnerFunc func(ctx context.Context, workflow *Workflow, logger Logger) error
```

The `next` function represents the next step in the execution chain, which could be another middleware or the core workflow execution logic itself.

## Common Use Cases

-   **Global Timing**: Measure the total execution time of a workflow.
-   **Error Handling**: Catch any error that might bubble up from the workflow and handle it (e.g., log it, send a notification, or decide to recover).
-   **Resource Management**: Set up global resources (like a database connection pool) that all workflows executed by the runner will use.
-   **Global State Injection**: Inject common configuration or data into the store of every workflow before it runs.

## Example: A Simple Timing Middleware

Here is an example of a runner middleware that measures and logs the total execution time of a workflow.

```go
import (
    "context"
    "fmt"
    "time"
    "github.com/davidroman0O/gostage"
)

// TimingMiddleware measures the total execution time of a workflow.
func TimingMiddleware() gostage.Middleware {
    return func(next gostage.RunnerFunc) gostage.RunnerFunc {
        // This returned function is the actual middleware.
        return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
            // 1. Code here runs BEFORE the workflow starts.
            startTime := time.Now()
            logger.Info("Starting workflow %s at %v", wf.Name, startTime)

            // 2. Call next() to proceed with the workflow's execution.
            // If you don't call next(), the workflow will not run.
            err := next(ctx, wf, logger)

            // 3. Code here runs AFTER the workflow has finished.
            duration := time.Since(startTime)
            wf.Store.Put("workflow.execution_time", duration)
            logger.Info("Workflow %s finished in %v.", wf.Name, duration)

            // 4. Return the error (or handle it).
            return err
        }
    }
}

// How to use it:
// runner := gostage.NewRunner(gostage.WithMiddleware(TimingMiddleware()))
```

In this example, calling `next()` triggers the workflow's execution. The code before the call is the "setup" phase of the middleware, and the code after is the "teardown" phase. This pattern is fundamental to how all middleware in `gostage` works.

---

Next, we'll move one layer deeper to [**Workflow Middleware**](./02-workflow-middleware.md). 