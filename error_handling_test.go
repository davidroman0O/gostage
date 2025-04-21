package gostage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/davidroman0O/gostage/store"
	"github.com/stretchr/testify/assert"
)

// TestErrorHandlerMiddlewareExecution tests that an error handler middleware can properly handle errors
// from workflow execution and either propagate them or recover from them based on defined patterns.
func TestErrorHandlerMiddlewareExecution(t *testing.T) {
	// Test cases with different error patterns
	tests := []struct {
		name           string
		errorMessage   string
		ignorePatterns []string
		shouldRecover  bool
	}{
		{
			name:           "Error not in ignore patterns",
			errorMessage:   "critical error",
			ignorePatterns: []string{"non-critical", "warning"},
			shouldRecover:  false, // This error should propagate
		},
		{
			name:           "Error in ignore patterns",
			errorMessage:   "non-critical error",
			ignorePatterns: []string{"non-critical", "warning"},
			shouldRecover:  true, // This error should be ignored
		},
		{
			name:           "Error partially matching pattern",
			errorMessage:   "this is a warning level issue",
			ignorePatterns: []string{"non-critical", "warning"},
			shouldRecover:  true, // This error should be ignored due to partial match
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a workflow with a failing action
			workflow := NewWorkflow("error-test", "Error Handler Test", "Tests error handling middleware")
			stage := NewStage("error-stage", "Error Stage", "Stage with failing action")

			// Create an action that always fails with the specified error message
			action := NewTestAction("failing-action", "Action that always fails", func(ctx *ActionContext) error {
				return errors.New(tc.errorMessage)
			})

			stage.AddAction(action)
			workflow.AddStage(stage)

			// Create a runner with the error handling middleware
			runner := NewRunner()

			// Add the error handling middleware
			runner.Use(func(next RunnerFunc) RunnerFunc {
				return func(ctx context.Context, wf *Workflow, logger Logger) error {
					// Execute the workflow
					err := next(ctx, wf, logger)

					// Handle errors if any
					if err != nil {
						errMsg := err.Error()

						// Check if we should ignore this error
						for _, pattern := range tc.ignorePatterns {
							if strings.Contains(errMsg, pattern) {
								// Return nil to indicate recovery
								return nil
							}
						}
					}

					return err
				}
			})

			// Execute the workflow with a proper logger
			logger := &TestLogger{t: t}
			err := runner.Execute(context.Background(), workflow, logger)

			// Verify the error handling
			if tc.shouldRecover {
				assert.NoError(t, err, "Error should have been recovered")

				// Verify the error was stored in the workflow's store
				errMsg, getErr := store.GetOrDefault[string](workflow.Store, "error.lastError", "")
				assert.NoError(t, getErr, "Should be able to retrieve error message from store")
				assert.Empty(t, errMsg, "No error should be stored since it was recovered")
			} else {
				assert.Error(t, err, "Error should have been propagated")
				assert.Contains(t, err.Error(), tc.errorMessage, "Error should contain the original message")
			}
		})
	}
}

// TestErrorHandlingWithMultipleWorkflows tests that when a workflow fails with a recoverable error,
// subsequent workflows can still be executed if the middleware recovers from the error.
func TestErrorHandlingWithMultipleWorkflows(t *testing.T) {
	// Create first workflow that will fail with a non-critical error
	workflow1 := NewWorkflow("failing-workflow", "Failing Workflow", "Workflow that fails with non-critical error")
	stage1 := NewStage("failing-stage", "Failing Stage", "Stage that fails")
	stage1Executed := false
	stage1.AddAction(NewTestAction("failing-action", "Action that fails", func(ctx *ActionContext) error {
		stage1Executed = true
		return errors.New("non-critical workflow error")
	}))
	workflow1.AddStage(stage1)

	// Create second workflow that should execute even though the first one failed
	workflow2 := NewWorkflow("subsequent-workflow", "Subsequent Workflow", "Workflow that runs after error recovery")
	stage2 := NewStage("subsequent-stage", "Subsequent Stage", "Stage that should execute")
	stage2Executed := false
	stage2.AddAction(NewTestAction("subsequent-action", "Action that should execute", func(ctx *ActionContext) error {
		stage2Executed = true
		return nil
	}))
	workflow2.AddStage(stage2)

	// Create runner with error handling middleware
	runner := NewRunner()

	// Use error handling middleware
	runner.Use(func(next RunnerFunc) RunnerFunc {
		return func(ctx context.Context, wf *Workflow, logger Logger) error {
			err := next(ctx, wf, logger)

			if err != nil {
				// Store the error in the workflow store
				wf.Store.Put("error.lastError", err.Error())

				// Recover from non-critical errors
				if strings.Contains(err.Error(), "non-critical") {
					return nil // Recover from error
				}
			}

			return err
		}
	})

	// Execute both workflows with options to continue on error
	logger := &TestLogger{t: t}
	options := RunOptions{
		Logger:       logger,
		Context:      context.Background(),
		IgnoreErrors: true, // Continue executing workflows even after errors
	}

	results := runner.ExecuteWorkflows([]*Workflow{workflow1, workflow2}, options)

	// First workflow should have failed but been recovered
	assert.Equal(t, 2, len(results), "Both workflows should have been executed")
	assert.True(t, results[0].Success, "First workflow should be marked as successful due to error handling")
	assert.Nil(t, results[0].Error, "Error should be nil due to middleware recovery")

	// Second workflow should have executed successfully
	assert.True(t, results[1].Success, "Second workflow should have executed successfully")
	assert.Nil(t, results[1].Error, "Second workflow should have no errors")

	// Verify both stages executed
	assert.True(t, stage1Executed, "Stage in first workflow should have executed")
	assert.True(t, stage2Executed, "Stage in second workflow should have executed")

	// Verify the error was stored in the first workflow's store
	errMsg, getErr := store.GetOrDefault[string](workflow1.Store, "error.lastError", "")
	assert.NoError(t, getErr, "Should be able to get error from store")
	assert.Contains(t, errMsg, "non-critical", "Error message should be stored")
}
