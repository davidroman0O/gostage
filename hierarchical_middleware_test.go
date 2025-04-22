package gostage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestHierarchicalMiddleware tests the complete hierarchical middleware structure
func TestHierarchicalMiddleware(t *testing.T) {
	t.Run("complete_middleware_hierarchy", func(t *testing.T) {
		// Create a workflow with multiple stages
		wf := NewWorkflow("hierarchy-test", "Hierarchy Test", "Testing complete middleware hierarchy")

		// Track execution order
		executionOrder := []string{}

		// Add workflow middleware
		wf.Use(func(next WorkflowStageRunnerFunc) WorkflowStageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, fmt.Sprintf("workflow-middleware-before-stage:%s", s.ID))
				logger.Info("Workflow middleware before stage: %s", s.Name)

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, fmt.Sprintf("workflow-middleware-after-stage:%s", s.ID))
				logger.Info("Workflow middleware after stage: %s", s.Name)
				return err
			}
		})

		// Create first stage with middleware
		stage1 := NewStage("stage1", "First Stage", "First stage with middleware")
		stage1.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "stage1-middleware-before-actions")
				logger.Info("Stage 1 middleware before actions")

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, "stage1-middleware-after-actions")
				logger.Info("Stage 1 middleware after actions")
				return err
			}
		})

		// Create second stage with middleware
		stage2 := NewStage("stage2", "Second Stage", "Second stage with middleware")
		stage2.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "stage2-middleware-before-actions")
				logger.Info("Stage 2 middleware before actions")

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, "stage2-middleware-after-actions")
				logger.Info("Stage 2 middleware after actions")
				return err
			}
		})

		// Add actions to stage 1
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

		stage1.AddAction(action1)
		stage1.AddAction(action2)

		// Add actions to stage 2
		action3 := NewTestAction("action3", "Third Action", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "action3-executed")
			ctx.Logger.Info("Action 3 executed")
			return nil
		})

		stage2.AddAction(action3)

		// Add stages to workflow
		wf.AddStage(stage1)
		wf.AddStage(stage2)

		// Create runner with middleware
		runner := NewRunner()
		runner.Use(func(next RunnerFunc) RunnerFunc {
			return func(ctx context.Context, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "runner-middleware-before-workflow")
				logger.Info("Runner middleware before workflow")

				err := next(ctx, w, logger)

				executionOrder = append(executionOrder, "runner-middleware-after-workflow")
				logger.Info("Runner middleware after workflow")
				return err
			}
		})

		// Execute the workflow
		logger := &TestLogger{t: t}
		err := runner.Execute(context.Background(), wf, logger)

		// Verify execution
		assert.NoError(t, err)

		// The expected execution order should demonstrate the hierarchical structure:
		// 1. Runner middleware (before)
		// 2. For each stage:
		//    a. Workflow middleware (before)
		//    b. Stage middleware (before)
		//    c. Actions execute
		//    d. Stage middleware (after)
		//    e. Workflow middleware (after)
		// 3. Runner middleware (after)
		expectedOrder := []string{
			"runner-middleware-before-workflow",

			// First stage
			"workflow-middleware-before-stage:stage1",
			"stage1-middleware-before-actions",
			"action1-executed",
			"action2-executed",
			"stage1-middleware-after-actions",
			"workflow-middleware-after-stage:stage1",

			// Second stage
			"workflow-middleware-before-stage:stage2",
			"stage2-middleware-before-actions",
			"action3-executed",
			"stage2-middleware-after-actions",
			"workflow-middleware-after-stage:stage2",

			"runner-middleware-after-workflow",
		}

		assert.Equal(t, expectedOrder, executionOrder,
			"Middleware should execute in hierarchical order")
	})
}
