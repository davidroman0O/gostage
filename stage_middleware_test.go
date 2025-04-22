package gostage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStageMiddleware tests the new stage middleware functionality
func TestStageMiddleware(t *testing.T) {
	t.Run("basic_stage_middleware", func(t *testing.T) {
		// Create a workflow
		wf := NewWorkflow("middleware-test", "Middleware Test", "Testing stage middleware")

		// Create a stage with middleware
		stage := NewStage("test-stage", "Test Stage", "A stage for testing middleware")

		// Add logging middleware to the stage
		stage.Use(LoggingStageMiddleware())

		// Add a custom middleware that tracks execution
		executionOrder := []string{}
		stage.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "before-stage")
				err := next(ctx, s, w, logger)
				executionOrder = append(executionOrder, "after-stage")
				return err
			}
		})

		// Add an action to the stage
		action := NewTestAction("test-action", "Test Action", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "action-executed")
			return nil
		})
		stage.AddAction(action)

		// Add the stage to the workflow
		wf.AddStage(stage)

		// Execute the workflow
		logger := &TestLogger{t: t}
		runner := NewRunner()
		err := runner.Execute(context.Background(), wf, logger)

		// Verify execution
		assert.NoError(t, err)
		assert.Equal(t, []string{
			"before-stage",
			"action-executed",
			"after-stage",
		}, executionOrder)
	})

	t.Run("container_stage_middleware", func(t *testing.T) {
		// Create a workflow
		wf := NewWorkflow("container-test", "Container Test", "Testing container middleware")

		// Create a stage with container middleware
		stage := NewStage("container-stage", "Container Stage", "A stage using container middleware")

		// Add container middleware to the stage
		// In a real app, this would actually start/stop containers
		containerStarted := false
		containerStopped := false

		stage.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				// Start container
				containerStarted = true
				logger.Info("Container started")

				// Run the stage
				err := next(ctx, s, w, logger)

				// Stop container
				containerStopped = true
				logger.Info("Container stopped")

				return err
			}
		})

		// Add a simple action
		action := NewTestAction("container-action", "Container Action", func(ctx *ActionContext) error {
			ctx.Logger.Info("Action executed in container")
			return nil
		})
		stage.AddAction(action)

		// Add the stage to the workflow
		wf.AddStage(stage)

		// Execute the workflow
		logger := &TestLogger{t: t}
		runner := NewRunner()
		err := runner.Execute(context.Background(), wf, logger)

		// Verify execution
		assert.NoError(t, err)
		assert.True(t, containerStarted, "Container should be started")
		assert.True(t, containerStopped, "Container should be stopped")
	})

	t.Run("error_handling_in_middleware", func(t *testing.T) {
		// Create a workflow
		wf := NewWorkflow("error-test", "Error Test", "Testing error handling in middleware")

		// Create a stage with middleware that handles errors
		stage := NewStage("error-stage", "Error Stage", "A stage for testing error handling")

		// Track cleanup
		cleanupCalled := false

		// Add middleware that ensures cleanup happens even on error
		stage.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				logger.Info("Starting stage with resource allocation")

				// Run the stage
				err := next(ctx, s, w, logger)

				// Always do cleanup
				cleanupCalled = true
				logger.Info("Cleaning up resources")

				return err
			}
		})

		// Add an action that will fail
		action := NewTestAction("failing-action", "Failing Action", func(ctx *ActionContext) error {
			return fmt.Errorf("intentional test failure")
		})
		stage.AddAction(action)

		// Add the stage to the workflow
		wf.AddStage(stage)

		// Execute the workflow
		logger := &TestLogger{t: t}
		runner := NewRunner()
		err := runner.Execute(context.Background(), wf, logger)

		// Verify execution
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "intentional test failure")
		assert.True(t, cleanupCalled, "Cleanup should be called even on error")
	})

	t.Run("action_position_info", func(t *testing.T) {
		// Create a workflow
		wf := NewWorkflow("position-test", "Position Test", "Testing action position information")

		// Create a stage
		stage := NewStage("position-stage", "Position Stage", "A stage for testing action positions")

		// Collect position information
		positions := []string{}

		// Add three actions to check first, middle, and last positions
		action1 := NewTestAction("first-action", "First Action", func(ctx *ActionContext) error {
			positions = append(positions, fmt.Sprintf("Action %d, IsLast: %v", ctx.ActionIndex, ctx.IsLastAction))
			return nil
		})

		action2 := NewTestAction("middle-action", "Middle Action", func(ctx *ActionContext) error {
			positions = append(positions, fmt.Sprintf("Action %d, IsLast: %v", ctx.ActionIndex, ctx.IsLastAction))
			return nil
		})

		action3 := NewTestAction("last-action", "Last Action", func(ctx *ActionContext) error {
			positions = append(positions, fmt.Sprintf("Action %d, IsLast: %v", ctx.ActionIndex, ctx.IsLastAction))
			return nil
		})

		stage.AddAction(action1)
		stage.AddAction(action2)
		stage.AddAction(action3)

		// Add the stage to the workflow
		wf.AddStage(stage)

		// Execute the workflow
		logger := &TestLogger{t: t}
		runner := NewRunner()
		err := runner.Execute(context.Background(), wf, logger)

		// Verify execution
		assert.NoError(t, err)
		assert.Equal(t, []string{
			"Action 0, IsLast: false",
			"Action 1, IsLast: false",
			"Action 2, IsLast: true",
		}, positions)
	})

	t.Run("multiple_middlewares_execution_order", func(t *testing.T) {
		// Create a workflow
		wf := NewWorkflow("multi-middleware-test", "Multiple Middleware Test", "Testing multiple middleware execution order")

		// Create a stage
		stage := NewStage("multi-middleware-stage", "Multiple Middleware Stage", "A stage for testing multiple middlewares")

		// Track execution order
		executionOrder := []string{}

		// Add first middleware
		stage.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "middleware1-before")
				logger.Info("First middleware - before stage")

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, "middleware1-after")
				logger.Info("First middleware - after stage")
				return err
			}
		})

		// Add second middleware
		stage.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "middleware2-before")
				logger.Info("Second middleware - before stage")

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, "middleware2-after")
				logger.Info("Second middleware - after stage")
				return err
			}
		})

		// Add third middleware
		stage.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "middleware3-before")
				logger.Info("Third middleware - before stage")

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, "middleware3-after")
				logger.Info("Third middleware - after stage")
				return err
			}
		})

		// Add a simple action
		action := NewTestAction("action", "Test Action", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "action-executed")
			ctx.Logger.Info("Action executed")
			return nil
		})
		stage.AddAction(action)

		// Add the stage to the workflow
		wf.AddStage(stage)

		// Execute the workflow
		logger := &TestLogger{t: t}
		runner := NewRunner()
		err := runner.Execute(context.Background(), wf, logger)

		// Verify execution
		assert.NoError(t, err)

		// The middlewares should execute in this order:
		// 1. First middleware starts
		// 2. Second middleware starts
		// 3. Third middleware starts
		// 4. Action executes
		// 5. Third middleware completes
		// 6. Second middleware completes
		// 7. First middleware completes
		expectedOrder := []string{
			"middleware1-before",
			"middleware2-before",
			"middleware3-before",
			"action-executed",
			"middleware3-after",
			"middleware2-after",
			"middleware1-after",
		}

		assert.Equal(t, expectedOrder, executionOrder,
			"Middlewares should execute in correct order (first in, last out)")
	})

	t.Run("multiple_middlewares_with_multiple_actions", func(t *testing.T) {
		// Create a workflow
		wf := NewWorkflow("multi-action-test", "Multiple Actions Test", "Testing middleware with multiple actions")

		// Create a stage
		stage := NewStage("multi-action-stage", "Multiple Actions Stage", "A stage with multiple actions")

		// Track execution order
		executionOrder := []string{}

		// Add first middleware
		stage.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "middleware1-start")
				logger.Info("First middleware - start")

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, "middleware1-end")
				logger.Info("First middleware - end")
				return err
			}
		})

		// Add second middleware
		stage.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "middleware2-start")
				logger.Info("Second middleware - start")

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, "middleware2-end")
				logger.Info("Second middleware - end")
				return err
			}
		})

		// Add multiple actions
		action1 := NewTestAction("action1", "First Action", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "action1-executed")
			ctx.Logger.Info("Action 1 executed")
			return nil
		})

		action2 := NewTestAction("action2", "Second Action", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "action2-executed")
			ctx.Logger.Info("Action 2 executed")
			return nil
		})

		action3 := NewTestAction("action3", "Third Action", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "action3-executed")
			ctx.Logger.Info("Action 3 executed")
			return nil
		})

		stage.AddAction(action1)
		stage.AddAction(action2)
		stage.AddAction(action3)

		// Add the stage to the workflow
		wf.AddStage(stage)

		// Execute the workflow
		logger := &TestLogger{t: t}
		runner := NewRunner()
		err := runner.Execute(context.Background(), wf, logger)

		// Verify execution
		assert.NoError(t, err)

		// The expected execution order is:
		// 1. First middleware starts
		// 2. Second middleware starts
		// 3. All actions execute sequentially
		// 4. Second middleware completes
		// 5. First middleware completes
		expectedOrder := []string{
			"middleware1-start",
			"middleware2-start",
			"action1-executed",
			"action2-executed",
			"action3-executed",
			"middleware2-end",
			"middleware1-end",
		}

		assert.Equal(t, expectedOrder, executionOrder,
			"Middleware should wrap all actions together, not individually")
	})
}
