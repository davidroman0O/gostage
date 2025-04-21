package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// Define a simple action for testing middlewares
type SimpleAction struct {
	name        string
	description string
	executeFunc func(ctx context.Context, store *store.KVStore) error
}

// NewSimpleAction creates a new simple action
func NewSimpleAction(name, description string, fn func(ctx context.Context, store *store.KVStore) error) *SimpleAction {
	return &SimpleAction{
		name:        name,
		description: description,
		executeFunc: fn,
	}
}

// Execute implements the action's behavior
func (a *SimpleAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Printf("SimpleAction Execute called for action: %s\n", a.name)
	if a.executeFunc != nil {
		return a.executeFunc(ctx.GoContext, ctx.Store)
	}
	return nil
}

// GetName implements the Action interface
func (a *SimpleAction) GetName() string {
	return a.name
}

// Name implements the Action interface
func (a *SimpleAction) Name() string {
	return a.name
}

// Description implements the Action interface
func (a *SimpleAction) Description() string {
	return a.description
}

// Tags returns the action's tags
func (a *SimpleAction) Tags() []string {
	return []string{}
}

// TimingMiddleware creates a middleware that measures and reports workflow execution time
func TimingMiddleware() gostage.Middleware {
	return func(next gostage.RunnerFunc) gostage.RunnerFunc {
		return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
			// Record start time
			startTime := time.Now()
			fmt.Printf("TimingMiddleware: Starting workflow %s (time: %v)\n", wf.Name, startTime.Format(time.RFC3339))
			logger.Info("TimingMiddleware: Starting workflow %s", wf.Name)

			// Execute the workflow
			err := next(ctx, wf, logger)

			// Calculate execution duration
			duration := time.Since(startTime)
			fmt.Printf("TimingMiddleware: Workflow %s completed in %v\n", wf.Name, duration)
			logger.Info("TimingMiddleware: Workflow %s completed in %v", wf.Name, duration)

			// Store the execution time in the workflow store
			wf.Store.Put("metrics.executionTime", duration.String())

			return err
		}
	}
}

// StateInjectionMiddleware creates a middleware that injects initial state into the workflow
func StateInjectionMiddleware(initialState map[string]interface{}) gostage.Middleware {
	return func(next gostage.RunnerFunc) gostage.RunnerFunc {
		return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
			logger.Info("StateInjectionMiddleware: Injecting initial state")

			// Inject each key-value pair into the workflow store
			for key, value := range initialState {
				wf.Store.Put(key, value)
				logger.Info("StateInjectionMiddleware: Injected %s = %v", key, value)
			}

			// Continue with workflow execution
			return next(ctx, wf, logger)
		}
	}
}

// ErrorHandlingMiddleware creates a middleware that handles and potentially recovers from errors
func ErrorHandlingMiddleware(ignoreErrorPatterns []string) gostage.Middleware {
	return func(next gostage.RunnerFunc) gostage.RunnerFunc {
		return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
			logger.Info("ErrorHandlingMiddleware: Starting workflow with error handling")

			// Execute the workflow
			err := next(ctx, wf, logger)

			// Handle errors if any
			if err != nil {
				errMsg := err.Error()
				logger.Info("ErrorHandlingMiddleware: Workflow encountered error: %v", err)

				// Store error information in the workflow store
				wf.Store.Put("error.lastError", errMsg)
				wf.Store.Put("error.timestamp", time.Now())

				// Check if we should ignore this error
				for _, pattern := range ignoreErrorPatterns {
					if strings.Contains(errMsg, pattern) {
						fmt.Printf("ErrorHandlingMiddleware: IGNORING ERROR matching pattern '%s': %v\n", pattern, err)
						logger.Info("ErrorHandlingMiddleware: IGNORING ERROR matching pattern '%s': %v", pattern, err)
						return nil // Return nil to indicate no error (recovered)
					}
				}
			}

			return err
		}
	}
}

// RetryMiddleware creates a middleware that implements retry logic for workflows
func RetryMiddleware(maxRetries int, retryDelay time.Duration) gostage.Middleware {
	return func(next gostage.RunnerFunc) gostage.RunnerFunc {
		return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
			var lastErr error

			// Try up to maxRetries times
			for attempt := 1; attempt <= maxRetries; attempt++ {
				fmt.Printf("RetryMiddleware: Attempt %d/%d for workflow %s\n", attempt, maxRetries, wf.Name)
				logger.Info("RetryMiddleware: Attempt %d/%d for workflow %s", attempt, maxRetries, wf.Name)

				// For retry attempts, mark the attempt number in the store
				if attempt > 1 {
					wf.Store.Put("retry.attempt", attempt)
					wf.Store.Put("retry.lastError", lastErr.Error())
					fmt.Printf("RetryMiddleware: Retrying after error: %v\n", lastErr)
				}

				// Execute the workflow
				err := next(ctx, wf, logger)

				// If successful, return immediately
				if err == nil {
					if attempt > 1 {
						fmt.Printf("RetryMiddleware: Workflow %s succeeded on attempt %d\n", wf.Name, attempt)
						logger.Info("RetryMiddleware: Workflow %s succeeded on attempt %d", wf.Name, attempt)
					}
					return nil
				}

				// Otherwise, record the error and retry if attempts remain
				lastErr = err
				fmt.Printf("RetryMiddleware: Attempt %d failed: %v\n", attempt, err)
				logger.Info("RetryMiddleware: Attempt %d failed: %v", attempt, err)

				// Don't wait after the last attempt
				if attempt < maxRetries {
					fmt.Printf("RetryMiddleware: Waiting %v before retry...\n", retryDelay)
					logger.Info("RetryMiddleware: Waiting %v before retry...", retryDelay)
					time.Sleep(retryDelay)
				}
			}

			// All attempts failed
			fmt.Printf("RetryMiddleware: All %d attempts failed for workflow %s\n", maxRetries, wf.Name)
			logger.Info("RetryMiddleware: All %d attempts failed for workflow %s", maxRetries, wf.Name)
			return fmt.Errorf("failed after %d attempts: %w", maxRetries, lastErr)
		}
	}
}

// ValidationMiddleware creates a middleware that validates workflows before execution
func ValidationMiddleware() gostage.Middleware {
	return func(next gostage.RunnerFunc) gostage.RunnerFunc {
		return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
			logger.Info("ValidationMiddleware: Validating workflow %s", wf.Name)

			// Check that the workflow has at least one stage
			if len(wf.Stages) == 0 {
				return fmt.Errorf("workflow has no stages")
			}

			// Check that each stage has at least one action
			for _, stage := range wf.Stages {
				if len(stage.Actions) == 0 {
					return fmt.Errorf("stage '%s' has no actions", stage.Name)
				}
			}

			logger.Info("ValidationMiddleware: Workflow %s passed validation", wf.Name)
			return next(ctx, wf, logger)
		}
	}
}

// TracingMiddleware creates a middleware that adds basic tracing functionality
func TracingMiddleware(serviceName string) gostage.Middleware {
	return func(next gostage.RunnerFunc) gostage.RunnerFunc {
		return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
			traceID := fmt.Sprintf("trace-%d", time.Now().UnixNano())
			logger.Info("TracingMiddleware: Starting trace %s for workflow %s", traceID, wf.Name)

			// Store trace info in the workflow store
			wf.Store.Put("tracing.id", traceID)
			wf.Store.Put("tracing.service", serviceName)
			wf.Store.Put("tracing.startTime", time.Now())

			// Create a new context with tracing information
			// In a real implementation, this might use OpenTelemetry or other tracing libraries
			tracingCtx := context.WithValue(ctx, "traceID", traceID)

			// Execute the workflow with the tracing context
			err := next(tracingCtx, wf, logger)

			// Record completion in trace
			wf.Store.Put("tracing.endTime", time.Now())
			wf.Store.Put("tracing.success", err == nil)

			if err != nil {
				logger.Info("TracingMiddleware: Trace %s completed with error: %v", traceID, err)
			} else {
				logger.Info("TracingMiddleware: Trace %s completed successfully", traceID)
			}

			return err
		}
	}
}

// AuditMiddleware creates a middleware that logs all workflow events for auditing
func AuditMiddleware() gostage.Middleware {
	return func(next gostage.RunnerFunc) gostage.RunnerFunc {
		return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
			// Create audit entry for workflow start
			auditEvent := map[string]interface{}{
				"type":       "workflow_start",
				"workflowID": wf.ID,
				"timestamp":  time.Now(),
				"user":       ctx.Value("user"),
				"action":     "execute",
			}

			// In a real implementation, this would likely write to a database or audit log service
			logger.Info("AuditMiddleware: Audit event: %v", auditEvent)

			// Execute the workflow
			err := next(ctx, wf, logger)

			// Create audit entry for workflow completion
			auditEvent = map[string]interface{}{
				"type":       "workflow_complete",
				"workflowID": wf.ID,
				"timestamp":  time.Now(),
				"user":       ctx.Value("user"),
				"action":     "complete",
				"success":    err == nil,
			}

			if err != nil {
				auditEvent["error"] = err.Error()
			}

			logger.Info("AuditMiddleware: Audit event: %v", auditEvent)
			return err
		}
	}
}

// ConcurrencyLimitMiddleware creates a middleware that limits concurrent workflow executions
// This is a simulation that would need a real concurrency control mechanism in production
func ConcurrencyLimitMiddleware(maxConcurrent int, timeout time.Duration) gostage.Middleware {
	// In a real implementation, this would use a proper semaphore or rate limiter
	return func(next gostage.RunnerFunc) gostage.RunnerFunc {
		return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
			logger.Info("ConcurrencyLimitMiddleware: Checking concurrency limit (max: %d)", maxConcurrent)

			// Simulate acquiring a semaphore (would be a real implementation in production code)
			// This is just for example purposes
			logger.Info("ConcurrencyLimitMiddleware: Acquired execution slot")

			// Execute the workflow
			err := next(ctx, wf, logger)

			// Simulate releasing a semaphore
			logger.Info("ConcurrencyLimitMiddleware: Released execution slot")

			return err
		}
	}
}

// FailingAction is an action that always fails with the specified error
type FailingAction struct {
	name  string
	error error
}

// NewFailingAction creates a new action that always fails
func NewFailingAction(name string, errorMsg string) *FailingAction {
	return &FailingAction{
		name:  name,
		error: errors.New(errorMsg),
	}
}

// Execute implements the Action interface
func (a *FailingAction) Execute(ctx *gostage.ActionContext) error {
	return a.error
}

// GetName implements the Action interface
func (a *FailingAction) GetName() string {
	return a.name
}

// Name implements the Action interface
func (a *FailingAction) Name() string {
	return a.name
}

// Description implements the Action interface
func (a *FailingAction) Description() string {
	return a.error.Error()
}

// Tags returns the action's tags
func (a *FailingAction) Tags() []string {
	return []string{}
}

// CounterAction is an action that fails a certain number of times before succeeding
type CounterAction struct {
	name        string
	failUntil   int
	currentTry  int
	successData string
}

// NewCounterAction creates a new counter action
func NewCounterAction(name string, failUntil int, successData string) *CounterAction {
	return &CounterAction{
		name:        name,
		failUntil:   failUntil,
		currentTry:  0,
		successData: successData,
	}
}

// Execute implements the Action interface
func (a *CounterAction) Execute(ctx *gostage.ActionContext) error {
	a.currentTry++
	if a.currentTry <= a.failUntil {
		return fmt.Errorf("action %s: attempt %d of %d failed deliberately", a.name, a.currentTry, a.failUntil)
	}

	// Success after the specified number of failures
	ctx.Store.Put("counter.result", a.successData)
	return nil
}

// GetName implements the Action interface
func (a *CounterAction) GetName() string {
	return a.name
}

// Name implements the Action interface
func (a *CounterAction) Name() string {
	return a.name
}

// Description implements the Action interface
func (a *CounterAction) Description() string {
	return fmt.Sprintf("Action that succeeds after %d failures", a.failUntil)
}

// Tags returns the action's tags
func (a *CounterAction) Tags() []string {
	return []string{}
}

// CreateMiddlewareTestWorkflow creates a test workflow for demonstrating middleware
func CreateMiddlewareTestWorkflow(shouldFail bool) *gostage.Workflow {
	// Create the workflow
	wf := gostage.NewWorkflow("mw-test", "Middleware Test", "A workflow for testing middleware functionality")

	// Add tags for categorization
	wf.AddTag("test")
	wf.AddTag("middleware")

	// Create a stage for the first part of the workflow
	stage1 := gostage.NewStage("stage1", "First Stage", "The first stage of the workflow")

	// Add some test actions to the stage that won't fail
	action1 := NewSimpleAction("action1", "Test Action 1", func(ctx context.Context, store *store.KVStore) error {
		// Use a simple counter for tracking executions
		fmt.Printf("SimpleAction action1 executing...\n")

		// Store execution information
		store.Put("action1.executed", true)
		store.Put("action1.timestamp", time.Now())
		store.Put("action1.execution_count", 1) // This will be incremented by middleware if retried
		return nil
	})

	stage1.AddAction(action1)

	// Conditionally add a failing action
	if shouldFail {
		failingAction := NewFailingAction("failing-action", "This action fails deliberately")
		stage1.AddAction(failingAction)
	}

	// Add a counter action that will succeed after 2 failures
	counterAction := NewCounterAction("counter-action", 2, "Success data after retries")
	stage1.AddAction(counterAction)

	// Add the stage to the workflow
	wf.AddStage(stage1)

	return wf
}

func main() {
	// Create a logger
	logger := gostage.NewDefaultLogger()
	fmt.Println("Starting Middleware Example")
	logger.Info("Starting Middleware Example")

	// Create workflows - one that will succeed and one that will fail
	successWorkflow := CreateMiddlewareTestWorkflow(false)
	failWorkflow := CreateMiddlewareTestWorkflow(true)

	// Create a runner with various middleware
	runner := gostage.NewRunner(
		gostage.WithMiddleware(
			LoggingMiddleware(), // Built-in middleware
			TimingMiddleware(),  // Custom timing middleware
			StateInjectionMiddleware(map[string]interface{}{
				"initialValue": 42,
				"startTime":    time.Now(),
			}),
			// Change the error pattern to be more specific so we can see both error recovery and failure
			ErrorHandlingMiddleware([]string{"This action fails deliberately"}), // Will only ignore this specific error
			RetryMiddleware(2, 100*time.Millisecond),                            // Retry up to 2 times
			ValidationMiddleware(),                                              // Validate workflow before execution
			TracingMiddleware("middleware-example"),                             // Add tracing information
			AuditMiddleware(),                                                   // Log audit events
		),
		gostage.WithLogger(logger),
	)

	// Run the success workflow
	fmt.Println("Running success workflow...")
	logger.Info("Running success workflow...")
	err := runner.Execute(context.Background(), successWorkflow, logger)
	if err != nil {
		fmt.Printf("Success workflow failed: %v\n", err)
		logger.Error("Success workflow failed: %v", err)
	} else {
		fmt.Println("Success workflow completed successfully")
		logger.Info("Success workflow completed successfully")
	}

	// Run the failing workflow
	fmt.Println("\nRunning failing workflow...")
	logger.Info("\nRunning failing workflow...")
	err = runner.Execute(context.Background(), failWorkflow, logger)
	if err != nil {
		fmt.Printf("Failing workflow failed as expected: %v\n", err)
		logger.Error("Failing workflow failed as expected: %v", err)
	} else {
		fmt.Println("Failing workflow completed successfully (error was handled)")
		logger.Info("Failing workflow completed successfully (error was handled)")
	}

	// Create a workflow with a different error (that won't be ignored)
	nonIgnoredFailWorkflow := CreateMiddlewareTestWorkflow(false)
	failingAction := NewFailingAction("non-ignored-error", "This error won't be ignored")
	if len(nonIgnoredFailWorkflow.Stages) > 0 {
		nonIgnoredFailWorkflow.Stages[0].AddAction(failingAction)
	}

	// Run the non-ignored failing workflow
	fmt.Println("\nRunning workflow with non-ignored error...")
	logger.Info("\nRunning workflow with non-ignored error...")
	err = runner.Execute(context.Background(), nonIgnoredFailWorkflow, logger)
	if err != nil {
		fmt.Printf("Workflow with non-ignored error failed as expected: %v\n", err)
		logger.Error("Workflow with non-ignored error failed as expected: %v", err)
	} else {
		fmt.Println("Workflow with non-ignored error completed successfully (unexpected)")
		logger.Info("Workflow with non-ignored error completed successfully (unexpected)")
	}

	// Demonstrate using middleware directly in the Execute call
	fmt.Println("\nRunning success workflow with additional middleware...")
	logger.Info("\nRunning success workflow with additional middleware...")

	// Create a context with user information for the audit middleware
	userCtx := context.WithValue(context.Background(), "user", "admin")

	// Execute with additional concurrency limiting middleware
	err = runner.Execute(userCtx, successWorkflow, logger)
	if err != nil {
		fmt.Printf("Workflow execution failed: %v\n", err)
		logger.Error("Workflow execution failed: %v", err)
	} else {
		fmt.Println("Workflow executed successfully with all middleware")
		logger.Info("Workflow executed successfully with all middleware")
	}

	// Output final metrics
	executionTime, err := store.Get[string](successWorkflow.Store, "metrics.executionTime")
	if err == nil {
		fmt.Printf("Success workflow execution time: %v\n", executionTime)
		logger.Info("Success workflow execution time: %v", executionTime)
	} else {
		fmt.Printf("Failed to get execution time: %v\n", err)
	}

	// Check if retry action eventually succeeded
	counterResult, err := store.Get[string](successWorkflow.Store, "counter.result")
	if err == nil {
		fmt.Printf("Counter action result: %v\n", counterResult)
		logger.Info("Counter action result: %v", counterResult)
	} else {
		fmt.Printf("Failed to get counter result: %v\n", err)
	}

	fmt.Println("Middleware Example Completed")
	logger.Info("Middleware Example Completed")
}

// LoggingMiddleware is a built-in middleware that logs workflow execution events
func LoggingMiddleware() gostage.Middleware {
	return func(next gostage.RunnerFunc) gostage.RunnerFunc {
		return func(ctx context.Context, wf *gostage.Workflow, logger gostage.Logger) error {
			// Use both fmt.Printf and logger to ensure we see the output
			fmt.Printf("LoggingMiddleware: Starting workflow %s (%s)\n", wf.Name, wf.ID)
			logger.Info("LoggingMiddleware: Starting workflow %s (%s)", wf.Name, wf.ID)

			// Execute the workflow
			err := next(ctx, wf, logger)

			if err != nil {
				fmt.Printf("LoggingMiddleware: Workflow %s completed with error: %v\n", wf.Name, err)
				logger.Info("LoggingMiddleware: Workflow %s completed with error: %v", wf.Name, err)
			} else {
				fmt.Printf("LoggingMiddleware: Workflow %s completed successfully\n", wf.Name)
				logger.Info("LoggingMiddleware: Workflow %s completed successfully", wf.Name)
			}

			return err
		}
	}
}
