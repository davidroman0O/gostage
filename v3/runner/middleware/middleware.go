package middleware

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/davidroman0O/gostage/v3/types"
)

// WorkflowLogger logs start/completion of each stage within a workflow, including duration.
func WorkflowLogger() types.WorkflowMiddleware {
	return func(next types.WorkflowStageRunnerFunc) types.WorkflowStageRunnerFunc {
		return func(ctx context.Context, stage types.Stage, workflow types.Workflow, logger types.Logger) error {
			safeInfo(logger, "workflow %s stage %s starting", workflow.Name(), stage.Name())
			started := time.Now()
			err := next(ctx, stage, workflow, logger)
			elapsed := time.Since(started)
			if err != nil {
				safeError(logger, "workflow %s stage %s failed after %s: %v", workflow.Name(), stage.Name(), elapsed, err)
			} else {
				safeInfo(logger, "workflow %s stage %s completed in %s", workflow.Name(), stage.Name(), elapsed)
			}
			return err
		}
	}
}

// StageTimer provides per-stage timing logs when attached via Stage.Use.
func StageTimer() types.StageMiddleware {
	return func(next types.StageRunnerFunc) types.StageRunnerFunc {
		return func(ctx context.Context, stage types.Stage, workflow types.Workflow, logger types.Logger) error {
			planned := actionNames(stage.ActionList())
			safeInfo(logger, "stage %s entering with actions: %s", stage.Name(), strings.Join(planned, ", "))
			started := time.Now()
			err := next(ctx, stage, workflow, logger)
			elapsed := time.Since(started)
			current := actionNames(stage.ActionList())
			if len(current) != len(planned) {
				safeInfo(logger, "stage %s dynamic actions now: %s", stage.Name(), strings.Join(current, ", "))
			}
			if err != nil {
				safeError(logger, "stage %s failed after %s: %v", stage.Name(), elapsed, err)
			} else {
				safeInfo(logger, "stage %s finished in %s", stage.Name(), elapsed)
			}
			return err
		}
	}
}

// ActionProgress reports percentage progress through the current stage using the ctx broker.
func ActionProgress() types.ActionMiddleware {
	return func(next types.ActionRunnerFunc) types.ActionRunnerFunc {
		return func(ctx types.Context, action types.Action, index int, isLast bool) error {
			stage := ctx.Stage()
			total := 0
			if stage != nil {
				total = len(stage.ActionList())
			}
			safeInfo(ctx.Logger(), "action %s starting (%d/%d)", action.Name(), index+1, max(total, index+1))
			err := next(ctx, action, index, isLast)

			broker := ctx.Broker()
			status := statusFromError(err, isLast)
			if broker != nil && stage != nil {
				total := len(stage.ActionList())
				if total == 0 {
					total = index + 1
				}
				percent := int(float64(index+1) / float64(total) * 100)
				msg := fmt.Sprintf("%s:%s", action.Name(), status)
				_ = broker.ProgressCause(msg, percent)
			}
			safeInfo(ctx.Logger(), "action %s finished status %s", action.Name(), status)
			return err
		}
	}
}

func statusFromError(err error, isLast bool) string {
	if err != nil {
		return "failed"
	}
	if isLast {
		return "completed"
	}
	return "running"
}

func safeInfo(logger types.Logger, format string, args ...interface{}) {
	if logger != nil {
		logger.Info(format, args...)
	}
}

func safeError(logger types.Logger, format string, args ...interface{}) {
	if logger != nil {
		logger.Error(format, args...)
	}
}

func actionNames(actions []types.Action) []string {
	names := make([]string, 0, len(actions))
	for _, act := range actions {
		names = append(names, act.Name())
	}
	return names
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
