# Middleware Example

This example demonstrates how to use and create middleware for the gostage Runner. Middleware provides a powerful way to:

1. Intercept workflow execution before and after running
2. Modify the context, workflow, or logger during execution 
3. Perform cross-cutting concerns like logging, timing, and error handling
4. Implement patterns like retry, validation, and state injection

## What is Middleware?

In gostage, middleware is a function that wraps the core workflow execution logic with additional behavior. Each middleware has this signature:

```go
type Middleware func(next RunnerFunc) RunnerFunc
```

Where `RunnerFunc` is:

```go
type RunnerFunc func(ctx context.Context, wf *Workflow, logger Logger) error
```

This design allows middleware to:
- Execute code before and after the workflow runs
- Decide whether to execute the workflow at all
- Modify the context, workflow, or logger passed to the next middleware
- Handle or transform errors returned from the workflow

## Middleware Chain

Middleware is executed in a chain, with each middleware wrapping the next. When you add middleware to a Runner, the order is important:

```go
runner := gostage.NewRunner()
runner.Use(
    middleware3, // Executed first
    middleware2, // Executed second
    middleware1, // Executed last (closest to actual workflow execution)
)
```

Alternatively, you can initialize a runner with middleware:

```go
runner := gostage.NewRunner(
    gostage.WithMiddleware(
        middleware3, // Executed first
        middleware2, // Executed second
        middleware1, // Executed last
    ),
)
```

## Middleware Examples

This example implements several types of middleware for common patterns:

### Built-in Middleware

- **LoggingMiddleware**: Logs workflow execution steps (provided by gostage)
  ```go
  runner.Use(gostage.LoggingMiddleware())
  ```

### Timing and Metrics

- **TimingMiddleware**: Measures workflow execution time
  ```go
  runner.Use(TimingMiddleware())
  ```

### Error Handling

- **ErrorHandlingMiddleware**: Handles and potentially recovers from errors
  ```go
  runner.Use(ErrorHandlingMiddleware([]string{"simulated", "non-critical"}))
  ```

### Validation

- **ValidationMiddleware**: Validates workflows before execution
  ```go
  runner.Use(ValidationMiddleware())
  ```

### Retry Pattern

- **RetryMiddleware**: Implements retry logic for workflows
  ```go
  runner.Use(RetryMiddleware(3, 100*time.Millisecond))
  ```

### State Injection

- **StateInjectionMiddleware**: Injects initial state into workflow store
  ```go
  runner.Use(StateInjectionMiddleware(map[string]interface{}{
      "environment": "production",
      "region":      "us-west-2",
      "timeout":     300,
  }))
  ```

### Tracing and Monitoring

- **TracingMiddleware**: Adds basic tracing functionality
  ```go
  runner.Use(TracingMiddleware("my-service"))
  ```

### Auditing

- **AuditMiddleware**: Records workflow events for auditing purposes
  ```go
  runner.Use(AuditMiddleware())
  ```

## How to Create Your Own Middleware

Creating custom middleware is simple:

1. Define a function that returns a `gostage.Middleware`
2. Inside that function, return a function that takes a `RunnerFunc` and returns a `RunnerFunc`
3. Inside that inner function, do your work before and after calling the `next` function

Example:

```go
func MyCustomMiddleware() gostage.Middleware {
    return func(next gostage.RunnerFunc) gostage.RunnerFunc {
        return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
            // Do something before workflow execution
            logger.Info("Before execution")
            
            // Call the next middleware in the chain
            err := next(ctx, wf, logger)
            
            // Do something after workflow execution
            logger.Info("After execution")
            
            // Return the error (or transform it)
            return err
        }
    }
}
```

## Common Middleware Patterns

### Composition

Combine multiple middlewares for more complex behavior:

```go
// Create a production middleware stack
func ProductionMiddleware() gostage.Middleware {
    return func(next gostage.RunnerFunc) gostage.RunnerFunc {
        // Apply multiple middlewares in sequence
        next = TimingMiddleware()(next)
        next = TracingMiddleware("production")(next)
        next = ErrorHandlingMiddleware([]string{"non-critical"})(next)
        
        return next
    }
}
```

### Conditional Application

Apply middleware only in certain conditions:

```go
func ConditionalMiddleware(enabled bool) gostage.Middleware {
    return func(next gostage.RunnerFunc) gostage.RunnerFunc {
        if !enabled {
            return next // Skip this middleware
        }
        
        return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
            // Apply middleware logic
            return next(ctx, wf, logger)
        }
    }
}
```

### Environment-Specific Middleware

Apply different middleware stacks based on environment:

```go
func GetMiddlewareStack(env string) []gostage.Middleware {
    switch env {
    case "development":
        return []gostage.Middleware{
            LoggingMiddleware(),
            TimingMiddleware(),
        }
    case "production":
        return []gostage.Middleware{
            TracingMiddleware("production"),
            ErrorHandlingMiddleware([]string{"non-critical"}),
            AuditMiddleware(),
        }
    default:
        return []gostage.Middleware{
            LoggingMiddleware(),
        }
    }
}
```

## Running the Example

The example demonstrates four middleware scenarios:

1. Basic middleware chain
2. Error handling middleware
3. State injection middleware
4. Retry middleware

Run the example with:

```
go run ./examples/middleware
```

Each scenario shows a different aspect of middleware functionality, from basic execution wrapping to more complex patterns like error recovery and retry logic. 