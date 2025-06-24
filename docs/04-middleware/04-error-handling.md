# 04 - Error Handling with Middleware

In any real-world application, things can and will go wrong. An action might fail due to a network error, invalid data, or countless other reasons. `gostage` provides a powerful way to handle these failures gracefully using **middleware**.

By default, if any action in a workflow returns an error, the execution of that workflow stops immediately, and the error is propagated up to the caller of `runner.Execute()`. However, you can intercept these errors with middleware to implement more sophisticated strategies.

## The Error Handling Pattern

The best place to implement global error handling is in a `Runner` middleware, as it wraps the entire execution of a workflow.

An error-handling middleware typically follows this pattern:

1.  Call `next()` to execute the workflow.
2.  Check the `error` returned from the `next()` call.
3.  If the error is `nil`, the workflow succeeded.
4.  If the error is not `nil`, you can:
    -   **Log the error**: Record the details of the failure.
    -   **Inspect the error**: Check the error's type or message to decide what to do.
    -   **Recover**: If the error is not critical, you can handle it and return `nil` from the middleware. This "swallows" the error and allows the runner to proceed as if the workflow had succeeded (e.g., to run the next workflow in a batch).
    -   **Propagate**: If the error is critical, you can simply return it, and it will be propagated to the caller.

## Example: Recoverable Errors Middleware

Here is an example of a middleware that inspects error messages. If an error contains a specific "non-critical" substring, it logs the error but allows execution to continue without failing the entire run.

```go
import (
    "context"
    "strings"
    "github.com/davidroman0O/gostage"
)

// ErrorRecoveryMiddleware handles errors, recovering from non-critical ones.
func ErrorRecoveryMiddleware(recoverablePatterns []string) gostage.Middleware {
    return func(next gostage.RunnerFunc) gostage.RunnerFunc {
        return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
            // Execute the workflow.
            err := next(ctx, wf, logger)

            if err != nil {
                // An error occurred. Check if it's a recoverable one.
                for _, pattern := range recoverablePatterns {
                    if strings.Contains(err.Error(), pattern) {
                        // This is a recoverable error.
                        logger.Warn("Recoverable error in workflow '%s': %v. Continuing execution.", wf.ID, err)
                        // Return nil to "swallow" the error and mark the workflow as successful.
                        return nil
                    }
                }

                // This is a critical error.
                logger.Error("Critical error in workflow '%s': %v", wf.ID, err)
            }

            // Propagate the critical error or return nil if successful.
            return err
        }
    }
}

// How to use it:
// runner := gostage.NewRunner(gostage.WithMiddleware(
//     ErrorRecoveryMiddleware([]string{"non-critical", "timeout"}),
// ))
```

This pattern is especially useful when running a batch of workflows, where you don't want a single, non-critical failure in one workflow to stop the entire batch. 