package workflow

import (
	"context"
	"time"

	"github.com/davidroman0O/gostage/v3/layers/foundation/clock"
	rt "github.com/davidroman0O/gostage/v3/shared/runtime"
)

// MiddlewareBuilder provides a fluent API for composing middleware chains.
// Middleware is applied in LIFO (Last In First Out) order - the last middleware
// added wraps the innermost execution.
//
// Example:
//
//	builder := NewMiddlewareBuilder()
//	builder.Workflow(loggingMiddleware).Stage(timingMiddleware).Action(retryMiddleware)
//	wfMw, stMw, actMw := builder.Build()
type MiddlewareBuilder struct {
	workflow []rt.WorkflowMiddleware
	stage    []rt.StageMiddleware
	action   []rt.ActionMiddleware
}

// NewMiddlewareBuilder creates a new middleware builder.
func NewMiddlewareBuilder() *MiddlewareBuilder {
	return &MiddlewareBuilder{
		workflow: make([]rt.WorkflowMiddleware, 0),
		stage:    make([]rt.StageMiddleware, 0),
		action:   make([]rt.ActionMiddleware, 0),
	}
}

// Workflow adds a workflow-level middleware to the chain.
// Middleware is applied in reverse order (last added wraps innermost).
func (b *MiddlewareBuilder) Workflow(mw rt.WorkflowMiddleware) *MiddlewareBuilder {
	if mw != nil {
		b.workflow = append(b.workflow, mw)
	}
	return b
}

// Stage adds a stage-level middleware to the chain.
// Middleware is applied in reverse order (last added wraps innermost).
func (b *MiddlewareBuilder) Stage(mw rt.StageMiddleware) *MiddlewareBuilder {
	if mw != nil {
		b.stage = append(b.stage, mw)
	}
	return b
}

// Action adds an action-level middleware to the chain.
// Middleware is applied in reverse order (last added wraps innermost).
func (b *MiddlewareBuilder) Action(mw rt.ActionMiddleware) *MiddlewareBuilder {
	if mw != nil {
		b.action = append(b.action, mw)
	}
	return b
}

// Build returns the composed middleware chains.
// Returns workflow middleware, stage middleware, and action middleware in that order.
func (b *MiddlewareBuilder) Build() ([]rt.WorkflowMiddleware, []rt.StageMiddleware, []rt.ActionMiddleware) {
	wf := make([]rt.WorkflowMiddleware, len(b.workflow))
	copy(wf, b.workflow)
	st := make([]rt.StageMiddleware, len(b.stage))
	copy(st, b.stage)
	act := make([]rt.ActionMiddleware, len(b.action))
	copy(act, b.action)
	return wf, st, act
}

// WithLogging adds a logging middleware that logs workflow/stage/action execution.
// This is a convenience method for common middleware patterns.
func (b *MiddlewareBuilder) WithLogging(logger rt.Logger) *MiddlewareBuilder {
	if logger == nil {
		return b
	}
	// Add logging middleware at each level
	b.Workflow(func(next rt.WorkflowStageRunnerFunc) rt.WorkflowStageRunnerFunc {
		return func(ctx context.Context, stage rt.Stage, workflow rt.Workflow, log rt.Logger) error {
			logger.Info("workflow started: id=%s name=%s", workflow.ID(), workflow.Name())
			err := next(ctx, stage, workflow, log)
			if err != nil {
				logger.Error("workflow failed: id=%s error=%v", workflow.ID(), err)
			} else {
				logger.Info("workflow completed: id=%s", workflow.ID())
			}
			return err
		}
	})
	b.Stage(func(next rt.StageRunnerFunc) rt.StageRunnerFunc {
		return func(ctx context.Context, stage rt.Stage, workflow rt.Workflow, log rt.Logger) error {
			logger.Info("stage started: id=%s name=%s", stage.ID(), stage.Name())
			err := next(ctx, stage, workflow, log)
			if err != nil {
				logger.Error("stage failed: id=%s error=%v", stage.ID(), err)
			} else {
				logger.Info("stage completed: id=%s", stage.ID())
			}
			return err
		}
	})
	b.Action(func(next rt.ActionRunnerFunc) rt.ActionRunnerFunc {
		return func(ctx rt.Context, action rt.Action, index int, isLast bool) error {
			logger.Info("action started: name=%s index=%d", action.Name(), index)
			err := next(ctx, action, index, isLast)
			if err != nil {
				logger.Error("action failed: name=%s error=%v", action.Name(), err)
			} else {
				logger.Info("action completed: name=%s", action.Name())
			}
			return err
		}
	})
	return b
}

// WithTiming adds timing middleware that measures execution duration.
// This is a convenience method for common middleware patterns.
func (b *MiddlewareBuilder) WithTiming() *MiddlewareBuilder {
	c := clock.DefaultClock()
	b.Workflow(func(next rt.WorkflowStageRunnerFunc) rt.WorkflowStageRunnerFunc {
		return func(ctx context.Context, stage rt.Stage, workflow rt.Workflow, log rt.Logger) error {
			start := c.Now()
			err := next(ctx, stage, workflow, log)
			duration := time.Since(start)
			log.Info("workflow duration: id=%s duration=%v", workflow.ID(), duration)
			return err
		}
	})
	b.Stage(func(next rt.StageRunnerFunc) rt.StageRunnerFunc {
		return func(ctx context.Context, stage rt.Stage, workflow rt.Workflow, log rt.Logger) error {
			start := c.Now()
			err := next(ctx, stage, workflow, log)
			duration := time.Since(start)
			log.Info("stage duration: id=%s duration=%v", stage.ID(), duration)
			return err
		}
	})
	b.Action(func(next rt.ActionRunnerFunc) rt.ActionRunnerFunc {
		return func(ctx rt.Context, action rt.Action, index int, isLast bool) error {
			start := c.Now()
			err := next(ctx, action, index, isLast)
			duration := time.Since(start)
			ctx.Logger().Info("action duration: name=%s duration=%v", action.Name(), duration)
			return err
		}
	})
	return b
}
