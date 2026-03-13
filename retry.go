package gostage

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// executeTaskFn wraps a task function with the engine's task middleware chain and panic recovery.
// Used by retryTask to ensure consistent middleware application.
func (e *Engine) executeTaskFn(taskCtx *Ctx, taskName string, fn func(*Ctx) error) error {
	if len(e.taskMiddleware) > 0 {
		final := func() error { return fn(taskCtx) }
		chain := final
		for j := 0; j < len(e.taskMiddleware); j++ {
			mw := e.taskMiddleware[j]
			next := chain
			chain = func() error {
				return mw(taskCtx, taskName, next)
			}
		}
		return func() (rerr error) {
			defer func() {
				if r := recover(); r != nil {
					rerr = fmt.Errorf("middleware panic: %v", r)
				}
			}()
			return chain()
		}()
	}
	return func() (rerr error) {
		defer func() {
			if r := recover(); r != nil {
				rerr = fmt.Errorf("task %q panic: %v", taskName, r)
			}
		}()
		return fn(taskCtx)
	}()
}

// resolveRetryConfig returns the retry count and delay for a task,
// falling back to workflow defaults if the task has no explicit config.
func resolveRetryConfig(td *taskDef, wf *Workflow) (int, RetryStrategy) {
	retries := td.retries
	strategy := td.retryStrategy
	if retries < 0 && wf.cfg.defaultRetries > 0 {
		retries = wf.cfg.defaultRetries
		if strategy == nil {
			strategy = wf.cfg.defaultRetryStrategy
		}
		if strategy == nil && wf.cfg.defaultRetryDelay > 0 {
			strategy = FixedDelay(wf.cfg.defaultRetryDelay)
		}
	}
	if retries < 0 {
		retries = 0
	}
	if strategy == nil && td.retryDelay > 0 {
		strategy = FixedDelay(td.retryDelay)
	}
	return retries, strategy
}

// retryTask executes a task function with retry logic, middleware, and optional per-task timeout.
// This is the unified retry loop used by both executeSingle and executeForEachItem,
// ensuring consistent handling of non-retryable signals (bail, suspend, sleep).
func (e *Engine) retryTask(ctx context.Context, taskName string, taskCtx *Ctx,
	fn func(*Ctx) error, retries int, strategy RetryStrategy, taskTimeout time.Duration) error {

	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 && strategy != nil {
			delay := strategy.Delay(attempt - 1)
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		// Per-task timeout wrapping
		var cancel context.CancelFunc
		attemptCtx := ctx
		if taskTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, taskTimeout)
		}
		taskCtx.goCtx = attemptCtx

		err := e.executeTaskFn(taskCtx, taskName, fn)

		if cancel != nil {
			cancel()
		}

		if err == nil {
			return nil
		}

		// Non-retryable signals: bail, suspend, sleep
		var bailErr *BailError
		var suspendErr *SuspendError
		var sleepErr *sleepError
		if errors.As(err, &bailErr) || errors.As(err, &suspendErr) || errors.As(err, &sleepErr) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lastErr = err
	}

	return lastErr
}
