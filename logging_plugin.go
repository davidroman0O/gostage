package gostage

import (
	"context"
	"time"
)

// loggingPlugin implements Plugin with structured logging at engine, step, and task levels.
type loggingPlugin struct {
	logger Logger
}

// LoggingPlugin returns a Plugin that logs workflow, step, and task execution
// using the provided Logger. It logs start/end events with duration and status.
//
//	engine, _ := gostage.New(gostage.WithPlugin(gostage.LoggingPlugin(myLogger)))
func LoggingPlugin(logger Logger) Plugin {
	return &loggingPlugin{logger: logger}
}

func (p *loggingPlugin) EngineMiddleware() EngineMiddleware {
	return func(ctx context.Context, wf *Workflow, runID RunID, next func() error) error {
		p.logger.Info("workflow %s run %s started", wf.ID, runID)
		start := time.Now()
		err := next()
		dur := time.Since(start)
		if err != nil {
			p.logger.Error("workflow %s run %s failed after %s: %v", wf.ID, runID, dur, err)
		} else {
			p.logger.Info("workflow %s run %s completed in %s", wf.ID, runID, dur)
		}
		return err
	}
}

func (p *loggingPlugin) StepMiddleware() StepMiddleware {
	return func(ctx context.Context, s *step, runID RunID, next func() error) error {
		p.logger.Debug("step %s started (run %s)", s.id, runID)
		start := time.Now()
		err := next()
		dur := time.Since(start)
		if err != nil {
			p.logger.Warn("step %s failed after %s: %v", s.id, dur, err)
		} else {
			p.logger.Debug("step %s completed in %s", s.id, dur)
		}
		return err
	}
}

func (p *loggingPlugin) TaskMiddleware() TaskMiddleware {
	return func(tctx *Ctx, taskName string, next func() error) error {
		p.logger.Debug("task %s started", taskName)
		start := time.Now()
		err := next()
		dur := time.Since(start)
		if err != nil {
			p.logger.Warn("task %s failed after %s: %v", taskName, dur, err)
		} else {
			p.logger.Debug("task %s completed in %s", taskName, dur)
		}
		return err
	}
}

func (p *loggingPlugin) ChildMiddleware() ChildMiddleware {
	return nil
}
