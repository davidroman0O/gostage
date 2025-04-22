package gostage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestComprehensiveMiddleware tests a complex scenario with multiple workflows,
// multiple stages, and multiple actions, with varying middleware configurations
func TestComprehensiveMiddleware(t *testing.T) {
	t.Run("multiple_workflows_stages_actions", func(t *testing.T) {
		// Create a global execution order tracker
		executionOrder := []string{}

		// Create a runner with middleware
		runner := NewRunner()
		runner.Use(func(next RunnerFunc) RunnerFunc {
			return func(ctx context.Context, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, fmt.Sprintf("runner-workflow-start:%s", w.ID))
				logger.Info("Runner middleware: Starting workflow %s", w.ID)

				err := next(ctx, w, logger)

				executionOrder = append(executionOrder, fmt.Sprintf("runner-workflow-end:%s", w.ID))
				logger.Info("Runner middleware: Completed workflow %s", w.ID)
				return err
			}
		})

		// --------------------------------------------------------
		// Create first workflow with 3 stages
		// --------------------------------------------------------
		wf1 := NewWorkflow("workflow1", "First Workflow", "First test workflow")

		// Add workflow middleware
		wf1.Use(func(next WorkflowStageRunnerFunc) WorkflowStageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, fmt.Sprintf("wf1-stage-start:%s", s.ID))
				logger.Info("Workflow 1 middleware: Starting stage %s", s.Name)

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, fmt.Sprintf("wf1-stage-end:%s", s.ID))
				logger.Info("Workflow 1 middleware: Completed stage %s", s.Name)
				return err
			}
		})

		// Create stages for workflow 1
		wf1Stage1 := NewStage("wf1-stage1", "WF1 Stage 1", "First stage in workflow 1")
		wf1Stage2 := NewStage("wf1-stage2", "WF1 Stage 2", "Second stage in workflow 1")
		wf1Stage3 := NewStage("wf1-stage3", "WF1 Stage 3", "Third stage in workflow 1")

		// Add stage middleware
		wf1Stage1.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "wf1-stage1-start")
				logger.Info("WF1 Stage 1 middleware: Starting actions")

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, "wf1-stage1-end")
				logger.Info("WF1 Stage 1 middleware: Completed actions")
				return err
			}
		})

		wf1Stage2.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "wf1-stage2-start")
				logger.Info("WF1 Stage 2 middleware: Starting actions")

				// This middleware will add a dynamic stage after this one
				dynStage := NewStage("wf1-dynamic-stage", "WF1 Dynamic Stage", "Dynamically added stage")
				dynStage.Use(func(next StageRunnerFunc) StageRunnerFunc {
					return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
						executionOrder = append(executionOrder, "wf1-dynamic-stage-start")
						logger.Info("WF1 Dynamic Stage middleware: Starting actions")

						err := next(ctx, s, w, logger)

						executionOrder = append(executionOrder, "wf1-dynamic-stage-end")
						logger.Info("WF1 Dynamic Stage middleware: Completed actions")
						return err
					}
				})

				// Add an action to the dynamic stage
				dynAction := NewTestAction("dynamic-action", "Dynamic Action", func(ctx *ActionContext) error {
					executionOrder = append(executionOrder, "wf1-dynamic-action-executed")
					ctx.Logger.Info("WF1 Dynamic Action executed")
					return nil
				})
				dynStage.AddAction(dynAction)

				// Save the dynamic stage to be added after this one
				actionCtx := &ActionContext{
					GoContext:     ctx,
					Workflow:      w,
					Stage:         s,
					Logger:        logger,
					dynamicStages: []*Stage{dynStage},
				}
				w.Context["dynamicStages"] = actionCtx.dynamicStages

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, "wf1-stage2-end")
				logger.Info("WF1 Stage 2 middleware: Completed actions")
				return err
			}
		})

		// Simple middleware for the third stage
		wf1Stage3.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "wf1-stage3-start")
				logger.Info("WF1 Stage 3 middleware: Starting actions")

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, "wf1-stage3-end")
				logger.Info("WF1 Stage 3 middleware: Completed actions")
				return err
			}
		})

		// Add actions to stages
		wf1Stage1.AddAction(NewTestAction("wf1-action1", "WF1 Action 1", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "wf1-action1-executed")
			ctx.Logger.Info("WF1 Action 1 executed")
			return nil
		}))

		wf1Stage1.AddAction(NewTestAction("wf1-action2", "WF1 Action 2", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "wf1-action2-executed")
			ctx.Logger.Info("WF1 Action 2 executed")
			return nil
		}))

		wf1Stage2.AddAction(NewTestAction("wf1-action3", "WF1 Action 3", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "wf1-action3-executed")
			ctx.Logger.Info("WF1 Action 3 executed")
			return nil
		}))

		wf1Stage3.AddAction(NewTestAction("wf1-action4", "WF1 Action 4", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "wf1-action4-executed")
			ctx.Logger.Info("WF1 Action 4 executed")
			return nil
		}))

		// Add stages to workflow 1
		wf1.AddStage(wf1Stage1)
		wf1.AddStage(wf1Stage2)
		wf1.AddStage(wf1Stage3)

		// --------------------------------------------------------
		// Create second workflow with 2 stages
		// --------------------------------------------------------
		wf2 := NewWorkflow("workflow2", "Second Workflow", "Second test workflow")

		// Add workflow middleware
		wf2.Use(func(next WorkflowStageRunnerFunc) WorkflowStageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, fmt.Sprintf("wf2-stage-start:%s", s.ID))
				logger.Info("Workflow 2 middleware: Starting stage %s", s.Name)

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, fmt.Sprintf("wf2-stage-end:%s", s.ID))
				logger.Info("Workflow 2 middleware: Completed stage %s", s.Name)
				return err
			}
		})

		// Create stages for workflow 2
		wf2Stage1 := NewStage("wf2-stage1", "WF2 Stage 1", "First stage in workflow 2")
		wf2Stage2 := NewStage("wf2-stage2", "WF2 Stage 2", "Second stage in workflow 2 (with error)")

		// Add stage middleware
		wf2Stage1.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "wf2-stage1-start")
				logger.Info("WF2 Stage 1 middleware: Starting actions")

				err := next(ctx, s, w, logger)

				executionOrder = append(executionOrder, "wf2-stage1-end")
				logger.Info("WF2 Stage 1 middleware: Completed actions")
				return err
			}
		})

		// Error-handling middleware for stage 2
		wf2Stage2.Use(func(next StageRunnerFunc) StageRunnerFunc {
			return func(ctx context.Context, s *Stage, w *Workflow, logger Logger) error {
				executionOrder = append(executionOrder, "wf2-stage2-start")
				logger.Info("WF2 Stage 2 middleware: Starting actions")

				err := next(ctx, s, w, logger)

				// Even if there's an error, this code will run
				executionOrder = append(executionOrder, "wf2-stage2-end")
				logger.Info("WF2 Stage 2 middleware: Completed actions with error: %v", err)

				// Let the error propagate
				return err
			}
		})

		// Add actions to stages
		wf2Stage1.AddAction(NewTestAction("wf2-action1", "WF2 Action 1", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "wf2-action1-executed")
			ctx.Logger.Info("WF2 Action 1 executed")
			return nil
		}))

		wf2Stage1.AddAction(NewTestAction("wf2-action2", "WF2 Action 2", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "wf2-action2-executed")
			ctx.Logger.Info("WF2 Action 2 executed")
			return nil
		}))

		// Action that will generate an error
		wf2Stage2.AddAction(NewTestAction("wf2-error-action", "WF2 Error Action", func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "wf2-error-action-executed")
			ctx.Logger.Info("WF2 Error Action executed")
			return fmt.Errorf("intentional test error")
		}))

		// Add stages to workflow 2
		wf2.AddStage(wf2Stage1)
		wf2.AddStage(wf2Stage2)

		// --------------------------------------------------------
		// Run both workflows
		// --------------------------------------------------------
		logger := &TestLogger{t: t}

		// Run workflow 1 (should succeed)
		err1 := runner.Execute(context.Background(), wf1, logger)
		assert.NoError(t, err1, "Workflow 1 should complete successfully")

		// Run workflow 2 (should fail with error)
		err2 := runner.Execute(context.Background(), wf2, logger)
		assert.Error(t, err2, "Workflow 2 should fail with error")
		assert.Contains(t, err2.Error(), "intentional test error", "Error should contain expected message")

		// --------------------------------------------------------
		// Verify execution order
		// --------------------------------------------------------
		expectedOrder := []string{
			// Workflow 1 execution
			"runner-workflow-start:workflow1",

			// Stage 1 of workflow 1
			"wf1-stage-start:wf1-stage1",
			"wf1-stage1-start",
			"wf1-action1-executed",
			"wf1-action2-executed",
			"wf1-stage1-end",
			"wf1-stage-end:wf1-stage1",

			// Stage 2 of workflow 1
			"wf1-stage-start:wf1-stage2",
			"wf1-stage2-start",
			"wf1-action3-executed",
			"wf1-stage2-end",
			"wf1-stage-end:wf1-stage2",

			// Dynamic stage created by Stage 2
			"wf1-stage-start:wf1-dynamic-stage",
			"wf1-dynamic-stage-start",
			"wf1-dynamic-action-executed",
			"wf1-dynamic-stage-end",
			"wf1-stage-end:wf1-dynamic-stage",

			// Stage 3 of workflow 1
			"wf1-stage-start:wf1-stage3",
			"wf1-stage3-start",
			"wf1-action4-executed",
			"wf1-stage3-end",
			"wf1-stage-end:wf1-stage3",

			"runner-workflow-end:workflow1",

			// Workflow 2 execution
			"runner-workflow-start:workflow2",

			// Stage 1 of workflow 2
			"wf2-stage-start:wf2-stage1",
			"wf2-stage1-start",
			"wf2-action1-executed",
			"wf2-action2-executed",
			"wf2-stage1-end",
			"wf2-stage-end:wf2-stage1",

			// Stage 2 of workflow 2 (with error)
			"wf2-stage-start:wf2-stage2",
			"wf2-stage2-start",
			"wf2-error-action-executed",
			"wf2-stage2-end",
			"wf2-stage-end:wf2-stage2",

			"runner-workflow-end:workflow2",
		}

		assert.Equal(t, expectedOrder, executionOrder, "Middleware should execute in expected order across multiple workflows")
	})
}
