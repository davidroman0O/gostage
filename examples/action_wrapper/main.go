package main

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/examples/common"
	"github.com/davidroman0O/gostage/store"
)

// BaseOperation represents a basic operation that can be wrapped
type BaseOperation struct {
	gostage.BaseAction
	execute func(ctx *gostage.ActionContext) error
}

// NewBaseOperation creates a new base operation
func NewBaseOperation(name, description string, execute func(ctx *gostage.ActionContext) error) *BaseOperation {
	return &BaseOperation{
		BaseAction: gostage.NewBaseAction(name, description),
		execute:    execute,
	}
}

// Execute runs the operation
func (a *BaseOperation) Execute(ctx *gostage.ActionContext) error {
	return a.execute(ctx)
}

// TimingWrapper wraps an action with timing functionality
type TimingWrapper struct {
	gostage.BaseAction
	wrappedAction gostage.Action
}

// NewTimingWrapper creates a new timing wrapper around an action
func NewTimingWrapper(wrappedAction gostage.Action) *TimingWrapper {
	return &TimingWrapper{
		BaseAction:    gostage.NewBaseAction(wrappedAction.Name(), fmt.Sprintf("Timed: %s", wrappedAction.Description())),
		wrappedAction: wrappedAction,
	}
}

// Execute runs the wrapped action with timing
func (a *TimingWrapper) Execute(ctx *gostage.ActionContext) error {
	// Record start time
	startTime := time.Now()
	ctx.Logger.Info("Starting timed execution of %s", a.wrappedAction.Name())

	// Execute the wrapped action
	err := a.wrappedAction.Execute(ctx)

	// Record execution time
	executionTime := time.Since(startTime)
	ctx.Logger.Info("Completed %s in %v", a.wrappedAction.Name(), executionTime)

	// Store execution time in the workflow store
	ctx.Store.Put(fmt.Sprintf("timing.%s", a.wrappedAction.Name()), executionTime.String())

	// Pass through any error from the wrapped action
	return err
}

// LoggingWrapper adds enhanced logging around an action
type LoggingWrapper struct {
	gostage.BaseAction
	wrappedAction gostage.Action
	logLevel      string
}

// NewLoggingWrapper creates a new logging wrapper
func NewLoggingWrapper(wrappedAction gostage.Action, logLevel string) *LoggingWrapper {
	return &LoggingWrapper{
		BaseAction:    gostage.NewBaseAction(wrappedAction.Name(), fmt.Sprintf("Logged: %s", wrappedAction.Description())),
		wrappedAction: wrappedAction,
		logLevel:      logLevel,
	}
}

// Execute runs the wrapped action with enhanced logging
func (a *LoggingWrapper) Execute(ctx *gostage.ActionContext) error {
	// Log action start with context information
	ctx.Logger.Info("[%s] Executing action: %s", a.logLevel, a.wrappedAction.Name())
	ctx.Logger.Info("[%s] Description: %s", a.logLevel, a.wrappedAction.Description())
	ctx.Logger.Info("[%s] Tags: %v", a.logLevel, a.wrappedAction.Tags())

	// Execute the wrapped action
	err := a.wrappedAction.Execute(ctx)

	// Log action completion or failure
	if err != nil {
		ctx.Logger.Error("[%s] Action %s failed: %v", a.logLevel, a.wrappedAction.Name(), err)
	} else {
		ctx.Logger.Info("[%s] Action %s completed successfully", a.logLevel, a.wrappedAction.Name())
	}

	// Pass through any error from the wrapped action
	return err
}

// RetryWrapper adds retry capabilities to an action
type RetryWrapper struct {
	gostage.BaseAction
	wrappedAction gostage.Action
	maxRetries    int
	retryDelay    time.Duration
}

// NewRetryWrapper creates a new retry wrapper
func NewRetryWrapper(wrappedAction gostage.Action, maxRetries int, retryDelay time.Duration) *RetryWrapper {
	return &RetryWrapper{
		BaseAction:    gostage.NewBaseAction(wrappedAction.Name(), fmt.Sprintf("Retry: %s", wrappedAction.Description())),
		wrappedAction: wrappedAction,
		maxRetries:    maxRetries,
		retryDelay:    retryDelay,
	}
}

// Execute runs the wrapped action with retry logic
func (a *RetryWrapper) Execute(ctx *gostage.ActionContext) error {
	var lastErr error

	// Try the operation up to maxRetries times
	for attempt := 1; attempt <= a.maxRetries; attempt++ {
		ctx.Logger.Info("Attempt %d/%d for action %s", attempt, a.maxRetries, a.wrappedAction.Name())

		// Execute the wrapped action
		err := a.wrappedAction.Execute(ctx)

		// If successful, return immediately
		if err == nil {
			ctx.Logger.Info("Action %s succeeded on attempt %d", a.wrappedAction.Name(), attempt)
			return nil
		}

		// Otherwise, record the error and retry if attempts remain
		lastErr = err
		ctx.Logger.Warn("Attempt %d failed for action %s: %v", attempt, a.wrappedAction.Name(), err)

		// Don't wait after the last attempt
		if attempt < a.maxRetries {
			ctx.Logger.Info("Waiting %v before retry...", a.retryDelay)
			time.Sleep(a.retryDelay)
		}
	}

	// All attempts failed
	ctx.Logger.Error("All %d attempts failed for action %s", a.maxRetries, a.wrappedAction.Name())
	return fmt.Errorf("failed after %d attempts: %w", a.maxRetries, lastErr)
}

// CompositeAction represents a collection of actions to be executed in sequence
type CompositeAction struct {
	gostage.BaseAction
	actions []gostage.Action
}

// NewCompositeAction creates a new composite action
func NewCompositeAction(name, description string, actions []gostage.Action) *CompositeAction {
	return &CompositeAction{
		BaseAction: gostage.NewBaseAction(name, description),
		actions:    actions,
	}
}

// Execute runs all contained actions in sequence
func (a *CompositeAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("Executing composite action %s with %d sub-actions", a.Name(), len(a.actions))

	// Execute each action in sequence
	for i, action := range a.actions {
		ctx.Logger.Info("Executing sub-action %d/%d: %s", i+1, len(a.actions), action.Name())

		// Execute the sub-action
		if err := action.Execute(ctx); err != nil {
			ctx.Logger.Error("Sub-action %s failed: %v", action.Name(), err)
			return fmt.Errorf("sub-action %d (%s) failed: %w", i+1, action.Name(), err)
		}

		ctx.Logger.Info("Completed sub-action %s", action.Name())
	}

	ctx.Logger.Info("All sub-actions completed successfully")
	return nil
}

// CreateActionWrapperWorkflow builds a workflow demonstrating action wrappers
func CreateActionWrapperWorkflow() *gostage.Workflow {
	// Create a new workflow
	wf := gostage.NewWorkflow(
		"action-wrapper-demo",
		"Action Wrapper Demonstration",
		"Demonstrates wrapping actions to add functionality",
	)

	// Create a stage for basic operations
	basicStage := gostage.NewStage(
		"basic-operations",
		"Basic Operations",
		"Demonstrates basic operations without wrappers",
	)

	// Add a few basic operations
	basicStage.AddAction(NewBaseOperation(
		"task-1",
		"Simple Task 1",
		func(ctx *gostage.ActionContext) error {
			ctx.Logger.Info("Executing simple task 1")
			time.Sleep(time.Millisecond * 500) // Simulate work
			return nil
		},
	))

	// This operation occasionally fails to demonstrate retries
	basicStage.AddAction(NewBaseOperation(
		"unreliable-task",
		"Unreliable Task",
		func(ctx *gostage.ActionContext) error {
			ctx.Logger.Info("Executing unreliable task")

			// Generate random failure (deterministic for this example)
			if time.Now().UnixNano()%3 == 0 {
				return fmt.Errorf("random failure in unreliable task")
			}

			time.Sleep(time.Millisecond * 300) // Simulate work
			return nil
		},
	))

	// Create a stage for wrapped operations
	wrappedStage := gostage.NewStage(
		"wrapped-operations",
		"Wrapped Operations",
		"Demonstrates operations with various wrappers",
	)

	// Create a base operation with timing wrapper
	operation1 := NewBaseOperation(
		"timed-task",
		"Task with Timing",
		func(ctx *gostage.ActionContext) error {
			ctx.Logger.Info("Executing timed task")
			time.Sleep(time.Second * 1) // Simulate work
			return nil
		},
	)
	wrappedStage.AddAction(NewTimingWrapper(operation1))

	// Create an operation with logging wrapper
	operation2 := NewBaseOperation(
		"logged-task",
		"Task with Logging",
		func(ctx *gostage.ActionContext) error {
			ctx.Logger.Info("Executing logged task")
			time.Sleep(time.Millisecond * 700) // Simulate work
			return nil
		},
	)
	wrappedStage.AddAction(NewLoggingWrapper(operation2, "DEBUG"))

	// Create an operation with retry wrapper - fixed version using store.Get
	operation3 := NewBaseOperation(
		"retried-task",
		"Task with Retry Logic",
		func(ctx *gostage.ActionContext) error {
			ctx.Logger.Info("Executing retried task")

			// Always fail the first time to demonstrate retry
			attemptKey := "retry.attempt.count"
			attemptCount, err := store.GetOrDefault(ctx.Store, attemptKey, 0)
			if err != nil {
				// If there's an error, start with 0
				attemptCount = 0
			}

			// Increment attempt count
			attemptCount++
			ctx.Store.Put(attemptKey, attemptCount)

			// Fail on first attempt
			if attemptCount == 1 {
				return fmt.Errorf("simulated failure on first attempt")
			}

			time.Sleep(time.Millisecond * 400) // Simulate work
			return nil
		},
	)
	wrappedStage.AddAction(NewRetryWrapper(operation3, 3, time.Millisecond*500))

	// Create a composite stage
	compositeStage := gostage.NewStage(
		"composite-operations",
		"Composite Operations",
		"Demonstrates composite actions combining multiple operations",
	)

	// Create a composite action with multiple sub-actions
	subActions := []gostage.Action{
		NewBaseOperation(
			"sub-task-1",
			"Sub Task 1",
			func(ctx *gostage.ActionContext) error {
				ctx.Logger.Info("Executing sub-task 1")
				time.Sleep(time.Millisecond * 200)
				return nil
			},
		),
		NewBaseOperation(
			"sub-task-2",
			"Sub Task 2",
			func(ctx *gostage.ActionContext) error {
				ctx.Logger.Info("Executing sub-task 2")
				time.Sleep(time.Millisecond * 300)
				return nil
			},
		),
		NewBaseOperation(
			"sub-task-3",
			"Sub Task 3",
			func(ctx *gostage.ActionContext) error {
				ctx.Logger.Info("Executing sub-task 3")
				time.Sleep(time.Millisecond * 100)
				return nil
			},
		),
	}

	compositeAction := NewCompositeAction(
		"composite-task",
		"Composite Task with Multiple Sub-Tasks",
		subActions,
	)

	// Wrap the composite action with timing to see total execution time
	timedCompositeAction := NewTimingWrapper(compositeAction)
	compositeStage.AddAction(timedCompositeAction)

	// Create a deeply nested wrapper stage
	nestedStage := gostage.NewStage(
		"nested-wrappers",
		"Nested Wrappers",
		"Demonstrates deeply nested action wrappers",
	)

	// Create a base operation
	deepOperation := NewBaseOperation(
		"deep-task",
		"Deeply Wrapped Task",
		func(ctx *gostage.ActionContext) error {
			ctx.Logger.Info("Executing deeply wrapped task")
			time.Sleep(time.Millisecond * 800)
			return nil
		},
	)

	// Apply multiple wrappers
	// 1. First add logging
	loggedOp := NewLoggingWrapper(deepOperation, "TRACE")
	// 2. Then add retry logic
	retriedOp := NewRetryWrapper(loggedOp, 2, time.Millisecond*200)
	// 3. Finally add timing
	timedOp := NewTimingWrapper(retriedOp)

	nestedStage.AddAction(timedOp)

	// Add all stages to workflow
	wf.AddStage(basicStage)
	wf.AddStage(wrappedStage)
	wf.AddStage(compositeStage)
	wf.AddStage(nestedStage)

	return wf
}

// Main function to run the example
func main() {
	fmt.Println("--- Action Wrapper Workflow Example ---")

	// Create the workflow
	wf := CreateActionWrapperWorkflow()

	// Print workflow information
	fmt.Printf("Workflow: %s - %s\n", wf.ID, wf.Name)
	fmt.Printf("Description: %s\n", wf.Description)
	fmt.Printf("Stages: %d\n\n", len(wf.Stages))

	// Execute the workflow
	fmt.Println("Executing workflow...")

	// Create a context and a console logger
	ctx := context.Background()
	logger := common.NewConsoleLogger(common.LogLevelInfo)

	if err := wf.Execute(ctx, logger); err != nil {
		fmt.Printf("Error executing workflow: %v\n", err)
		return
	}

	fmt.Println("\nWorkflow completed successfully!")
}
