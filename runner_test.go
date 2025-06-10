package gostage

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/store"
	"github.com/stretchr/testify/assert"
)

func TestRunWorkflow(t *testing.T) {
	// Create a simple test workflow
	wf := NewWorkflow("test-runner", "Test Runner", "Test workflow for runner")

	// Create a stage with a simple action
	stage := NewStage("test-stage", "Test Stage", "Test stage for runner")

	// Add an action that succeeds
	action := NewTestAction("test-action", "Test Action", func(ctx *ActionContext) error {
		return nil
	})

	stage.AddAction(action)
	wf.AddStage(stage)

	// Run the workflow with default options
	result := RunWorkflow(wf, DefaultRunOptions())

	// Check results
	assert.True(t, result.Success)
	assert.NoError(t, result.Error)
	assert.Equal(t, "test-runner", result.WorkflowID)
	assert.Greater(t, result.ExecutionTime.Nanoseconds(), int64(0))
}

func TestRunWorkflowWithError(t *testing.T) {
	// Create a simple test workflow
	wf := NewWorkflow("error-workflow", "Error Workflow", "Test workflow that fails")

	// Create a stage with a simple action
	stage := NewStage("error-stage", "Error Stage", "Test stage that fails")

	// Add an action that fails
	expectedErr := errors.New("test error")
	action := NewTestAction("error-action", "Error Action", func(ctx *ActionContext) error {
		return expectedErr
	})

	stage.AddAction(action)
	wf.AddStage(stage)

	// Run the workflow with default options
	result := RunWorkflow(wf, DefaultRunOptions())

	// Check results
	assert.False(t, result.Success)
	assert.Error(t, result.Error)
	assert.True(t, errors.Is(result.Error, expectedErr) || strings.Contains(result.Error.Error(), expectedErr.Error()))
	assert.Equal(t, "error-workflow", result.WorkflowID)
}

func TestRunMultipleWorkflows(t *testing.T) {
	// Create a successful workflow
	wf1 := NewWorkflow("success-1", "Success 1", "First successful workflow")
	stage1 := NewStage("stage-1", "Stage 1", "First stage")
	action1 := NewTestAction("action-1", "Action 1", func(ctx *ActionContext) error {
		return nil
	})
	stage1.AddAction(action1)
	wf1.AddStage(stage1)

	// Create another successful workflow
	wf2 := NewWorkflow("success-2", "Success 2", "Second successful workflow")
	stage2 := NewStage("stage-2", "Stage 2", "Second stage")
	action2 := NewTestAction("action-2", "Action 2", func(ctx *ActionContext) error {
		return nil
	})
	stage2.AddAction(action2)
	wf2.AddStage(stage2)

	// Create a failing workflow
	wf3 := NewWorkflow("failure", "Failure", "Failing workflow")
	stage3 := NewStage("stage-3", "Stage 3", "Third stage")
	action3 := NewTestAction("action-3", "Action 3", func(ctx *ActionContext) error {
		return errors.New("test error")
	})
	stage3.AddAction(action3)
	wf3.AddStage(stage3)

	// Run workflows with default options (stop on first error)
	results := RunWorkflows([]*Workflow{wf1, wf2, wf3}, DefaultRunOptions())

	// Only three workflows should have run, with the third one failing
	assert.Equal(t, 3, len(results))
	assert.True(t, results[0].Success)
	assert.True(t, results[1].Success)
	assert.False(t, results[2].Success)

	// Run with ignoring errors
	options := DefaultRunOptions()
	options.IgnoreErrors = true
	results = RunWorkflows([]*Workflow{wf1, wf2, wf3}, options)

	// All workflows should have run
	assert.Equal(t, 3, len(results))
	assert.True(t, results[0].Success)
	assert.True(t, results[1].Success)
	assert.False(t, results[2].Success)

	// Check formatting
	summary := FormatResults(results)
	assert.Contains(t, summary, "SUCCESS")
	assert.Contains(t, summary, "FAILED")
	assert.Contains(t, summary, "Summary: 2/3 workflows succeeded")
}

func TestRunWorkflowWithContext(t *testing.T) {
	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Create a workflow that checks if the context is done
	wf := NewWorkflow("context-test", "Context Test", "Test context cancellation")
	stage := NewStage("context-stage", "Context Stage", "Test stage for context")

	contextChecked := false
	action := NewTestAction("context-action", "Context Action", func(ctx *ActionContext) error {
		// Cancel the context
		cancel()

		// Check if the context is done
		select {
		case <-ctx.GoContext.Done():
			contextChecked = true
			return nil
		default:
			return errors.New("context not cancelled")
		}
	})

	stage.AddAction(action)
	wf.AddStage(stage)

	// Run with custom context
	options := DefaultRunOptions()
	options.Context = ctx

	result := RunWorkflow(wf, options)

	// Should succeed because we check that the context is cancelled
	assert.True(t, result.Success)
	assert.True(t, contextChecked)
}

// TestRunner_Execute tests that a workflow is executed successfully with the runner
func TestRunner_Execute(t *testing.T) {
	// Create a simple workflow with one stage and one action
	workflow := NewWorkflow("test-wf", "Test Workflow", "A test workflow")
	stage := NewStage("test-stage", "Test Stage", "A test stage")

	// Create a test action that just sets a value in the store
	action := NewActionFunc("test-action", "Test action", func(ctx *ActionContext) error {
		ctx.Store().Put("test-key", "test-value")
		return nil
	})

	stage.AddAction(action)
	workflow.AddStage(stage)

	// Create a logger that captures logs
	logger := &TestLogger{t: t}

	// Create a runner
	runner := NewRunner(WithLogger(logger))

	// Execute the workflow
	err := runner.Execute(context.Background(), workflow, logger)

	// Check for errors
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Check that the action was executed
	value, err := store.Get[string](workflow.Store, "test-key")
	if err != nil {
		t.Errorf("Expected to find key in store, got error: %v", err)
	}

	if value != "test-value" {
		t.Errorf("Expected value to be 'test-value', got: %s", value)
	}
}

// TestRunner_Middleware tests that middleware functions are executed correctly
func TestRunner_Middleware(t *testing.T) {
	// Create a simple workflow
	workflow := NewWorkflow("test-wf", "Test Workflow", "A test workflow")
	stage := NewStage("test-stage", "Test Stage", "A test stage")

	action := NewActionFunc("test-action", "Test action", func(ctx *ActionContext) error {
		return nil
	})

	stage.AddAction(action)
	workflow.AddStage(stage)

	// Create a logger
	logger := &TestLogger{t: t}

	// Create a middleware that sets a key in the workflow store
	middleware := func(next RunnerFunc) RunnerFunc {
		return func(ctx context.Context, w *Workflow, l Logger) error {
			// Set value before execution
			w.Store.Put("middleware-key", "middleware-value")

			// Call the next function in the chain
			err := next(ctx, w, l)

			// Set another value after execution
			w.Store.Put("middleware-after", "after-value")

			return err
		}
	}

	// Create a runner with the middleware
	runner := NewRunner(
		WithLogger(logger),
		WithMiddleware(middleware),
	)

	// Execute the workflow
	err := runner.Execute(context.Background(), workflow, logger)

	// Check for errors
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Check that middleware was executed
	beforeValue, err := store.Get[string](workflow.Store, "middleware-key")
	if err != nil {
		t.Errorf("Expected to find middleware-key in store, got error: %v", err)
	}

	if beforeValue != "middleware-value" {
		t.Errorf("Expected value to be 'middleware-value', got: %s", beforeValue)
	}

	// Check that middleware was executed after workflow
	afterValue, err := store.Get[string](workflow.Store, "middleware-after")
	if err != nil {
		t.Errorf("Expected to find middleware-after in store, got error: %v", err)
	}

	if afterValue != "after-value" {
		t.Errorf("Expected value to be 'after-value', got: %s", afterValue)
	}
}

// TestRunner_MultipleMiddleware tests that multiple middleware functions are executed in the correct order
func TestRunner_MultipleMiddleware(t *testing.T) {
	// Create a simple workflow
	workflow := NewWorkflow("test-wf", "Test Workflow", "A test workflow")
	stage := NewStage("test-stage", "Test Stage", "A test stage")

	// Create a test action that adds to the order
	action := NewActionFunc("test-action", "Test action", func(ctx *ActionContext) error {
		// Get the current order
		orderValue, err := store.GetOrDefault[string](ctx.Store(), "order", "")
		if err != nil {
			return err
		}

		// Add our position
		orderValue += "action-"

		// Store the updated order
		ctx.Store().Put("order", orderValue)
		return nil
	})

	stage.AddAction(action)
	workflow.AddStage(stage)

	// Create a logger
	logger := &TestLogger{t: t}

	// Create middlewares that add to the execution order
	middleware1 := func(next RunnerFunc) RunnerFunc {
		return func(ctx context.Context, w *Workflow, l Logger) error {
			// Get the current order
			orderValue, err := store.GetOrDefault[string](w.Store, "order", "")
			if err == nil {
				// Add our position before
				orderValue += "m1-before-"
				w.Store.Put("order", orderValue)
			}

			// Call the next function
			err = next(ctx, w, l)

			// Get the updated order
			orderValue, err = store.GetOrDefault[string](w.Store, "order", "")
			if err == nil {
				// Add our position after
				orderValue += "m1-after-"
				w.Store.Put("order", orderValue)
			}

			return err
		}
	}

	middleware2 := func(next RunnerFunc) RunnerFunc {
		return func(ctx context.Context, w *Workflow, l Logger) error {
			// Get the current order
			orderValue, err := store.GetOrDefault[string](w.Store, "order", "")
			if err == nil {
				// Add our position before
				orderValue += "m2-before-"
				w.Store.Put("order", orderValue)
			}

			// Call the next function
			err = next(ctx, w, l)

			// Get the updated order
			orderValue, err = store.GetOrDefault[string](w.Store, "order", "")
			if err == nil {
				// Add our position after
				orderValue += "m2-after-"
				w.Store.Put("order", orderValue)
			}

			return err
		}
	}

	// Create a runner with the middlewares
	runner := NewRunner(
		WithLogger(logger),
		WithMiddleware(middleware1, middleware2),
	)

	// Execute the workflow
	err := runner.Execute(context.Background(), workflow, logger)

	// Check for errors
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}

	// Check the execution order
	orderValue, err := store.Get[string](workflow.Store, "order")
	if err != nil {
		t.Errorf("Expected to find order in store, got error: %v", err)
	}

	// The expected order should be: m1-before-m2-before-action-m2-after-m1-after
	expectedOrder := "m1-before-m2-before-action-m2-after-m1-after-"
	if orderValue != expectedOrder {
		t.Errorf("Expected order to be '%s', got: %s", expectedOrder, orderValue)
	}
}

// TestRunner_MiddlewareErrors tests that errors from middleware are properly propagated
func TestRunner_MiddlewareErrors(t *testing.T) {
	// Create a simple workflow
	workflow := NewWorkflow("test-wf", "Test Workflow", "A test workflow")
	stage := NewStage("test-stage", "Test Stage", "A test stage")

	action := NewActionFunc("test-action", "Test action", func(ctx *ActionContext) error {
		return nil
	})

	stage.AddAction(action)
	workflow.AddStage(stage)

	// Create a logger
	logger := &TestLogger{t: t}

	// Create a middleware that returns an error
	expectedError := errors.New("middleware error")
	errorMiddleware := func(next RunnerFunc) RunnerFunc {
		return func(ctx context.Context, w *Workflow, l Logger) error {
			return expectedError
		}
	}

	// Create a runner with the middleware
	runner := NewRunner(
		WithLogger(logger),
		WithMiddleware(errorMiddleware),
	)

	// Execute the workflow
	err := runner.Execute(context.Background(), workflow, logger)

	// Check that the expected error was returned
	if err != expectedError {
		t.Errorf("Expected error: %v, got: %v", expectedError, err)
	}
}

// TestStoreInjectionMiddleware tests the built-in store injection middleware
func TestStoreInjectionMiddleware(t *testing.T) {
	// Create a simple workflow
	workflow := NewWorkflow("test-wf", "Test Workflow", "A test workflow")
	stage := NewStage("test-stage", "Test Stage", "A test stage")

	// Create an action that checks for the injected value
	action := NewActionFunc("test-action", "Test action", func(ctx *ActionContext) error {
		value, err := store.Get[string](ctx.Store(), "injected-key")
		if err != nil {
			return errors.New("injected value not found")
		}

		if value != "injected-value" {
			return errors.New("injected value not correct")
		}

		return nil
	})

	stage.AddAction(action)
	workflow.AddStage(stage)

	// Create a logger
	logger := &TestLogger{t: t}

	// Create a store injection middleware
	injectionValues := map[string]interface{}{
		"injected-key": "injected-value",
	}

	// Create a runner with the store injection middleware
	runner := NewRunner(
		WithLogger(logger),
		WithMiddleware(StoreInjectionMiddleware(injectionValues)),
	)

	// Execute the workflow
	err := runner.Execute(context.Background(), workflow, logger)

	// Check for errors
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
}

// TestTimeLimitMiddleware tests the built-in time limit middleware
func TestTimeLimitMiddleware(t *testing.T) {
	// Create a simple workflow
	workflow := NewWorkflow("test-wf", "Test Workflow", "A test workflow")
	stage := NewStage("test-stage", "Test Stage", "A test stage")

	// Create an action that sleeps longer than the timeout
	action := NewActionFunc("test-action", "Test action", func(ctx *ActionContext) error {
		// Sleep for longer than the timeout
		select {
		case <-time.After(200 * time.Millisecond):
			return nil
		case <-ctx.GoContext.Done():
			return ctx.GoContext.Err()
		}
	})

	stage.AddAction(action)
	workflow.AddStage(stage)

	// Create a logger
	logger := &TestLogger{t: t}

	// Create a runner with the time limit middleware (100ms timeout)
	runner := NewRunner(
		WithLogger(logger),
		WithMiddleware(TimeLimitMiddleware(100*time.Millisecond)),
	)

	// Execute the workflow
	err := runner.Execute(context.Background(), workflow, logger)

	// Check that we got a context deadline exceeded error
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context deadline exceeded error, got: %v", err)
	}
}

// NewActionFunc creates a new action from a function
func NewActionFunc(name, description string, fn func(*ActionContext) error) Action {
	return &funcAction{
		BaseAction: NewBaseAction(name, description),
		fn:         fn,
	}
}

// funcAction implements Action using a function
type funcAction struct {
	BaseAction
	fn func(*ActionContext) error
}

// Execute runs the action function
func (a *funcAction) Execute(ctx *ActionContext) error {
	return a.fn(ctx)
}

// TestMiddleware tests that middleware can modify workflow execution
func TestMiddleware(t *testing.T) {
	// Create a simple middleware that adds values to the store
	middleware := func(next RunnerFunc) RunnerFunc {
		return func(ctx context.Context, w *Workflow, l Logger) error {
			// Set value before execution
			w.Store.Put("middleware-key", "middleware-value")

			// Call the next function in the chain
			err := next(ctx, w, l)

			// Set another value after execution
			w.Store.Put("middleware-after", "after-value")

			return err
		}
	}

	// Create a workflow with a store
	workflow := NewWorkflow("test-wf", "Test Workflow", "Test workflow")

	// Create a stage with an action that checks store values
	stage := NewStage("test-stage", "Test Stage", "Test stage")
	stage.AddAction(NewTestAction("test-action", "Test Action", func(ctx *ActionContext) error {
		// The middleware should have set this value
		val, err := store.Get[string](ctx.Store(), "middleware-key")
		assert.NoError(t, err)
		assert.Equal(t, "middleware-value", val)
		return nil
	}))

	workflow.AddStage(stage)

	// Create a runner with the middleware
	runner := NewRunner(WithMiddleware(middleware))

	// Execute the workflow
	err := runner.Execute(context.Background(), workflow, NewDefaultLogger())
	assert.NoError(t, err)

	// Check the after value was set
	val, err := store.Get[string](workflow.Store, "middleware-after")
	assert.NoError(t, err)
	assert.Equal(t, "after-value", val)
}

// TestNewRunnerWithBroker tests the new constructor with broker
func TestNewRunnerWithBroker(t *testing.T) {
	// Create a custom broker
	broker := NewRunnerBroker(os.Stdout)

	// Test NewRunnerWithBroker constructor
	runner := NewRunnerWithBroker(broker)

	// Verify the broker was set correctly
	assert.Equal(t, broker, runner.Broker, "Broker should be set correctly")
	assert.NotNil(t, runner.defaultLogger, "Default logger should be set")
	assert.NotNil(t, runner.middleware, "Middleware slice should be initialized")

	// Test with additional options
	testLogger := &TestLogger{t: t}
	runner2 := NewRunnerWithBroker(broker, WithLogger(testLogger))

	assert.Equal(t, broker, runner2.Broker, "Broker should be set correctly")
	assert.Equal(t, testLogger, runner2.defaultLogger, "Custom logger should be set")
}

// TestWithBrokerOption tests the WithBroker option
func TestWithBrokerOption(t *testing.T) {
	// Create a custom broker
	broker := NewRunnerBroker(os.Stdout)

	// Test WithBroker option
	runner := NewRunner(WithBroker(broker))

	// Verify the broker was set correctly
	assert.Equal(t, broker, runner.Broker, "Broker should be set correctly via option")

	// Test combining with other options
	testLogger := &TestLogger{t: t}
	runner2 := NewRunner(
		WithBroker(broker),
		WithLogger(testLogger),
	)

	assert.Equal(t, broker, runner2.Broker, "Broker should be set correctly")
	assert.Equal(t, testLogger, runner2.defaultLogger, "Logger should be set correctly")
}
