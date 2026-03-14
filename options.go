package gostage

import (
	"fmt"
	"time"
)

// WithSQLite configures the engine to use SQLite for durable persistence. The
// database is opened using a pure-Go SQLite implementation (modernc.org/sqlite),
// so no CGo or external C libraries are required. WAL (Write-Ahead Logging)
// mode is enabled automatically for better concurrent read performance.
//
// The path argument is a file path for on-disk storage, or ":memory:" for an
// in-memory SQLite database (useful for testing). Schema migrations run
// automatically on first use.
//
// Without this option, the engine defaults to an in-memory store that does not
// survive process restarts and does not support timer-based sleep recovery.
//
//	engine, err := gostage.New(gostage.WithSQLite("app.db"))
//	engine, err := gostage.New(gostage.WithSQLite(":memory:"))
func WithSQLite(path string) EngineOption {
	return func(e *Engine) error {
		p, err := newSQLitePersistence(path)
		if err != nil {
			return fmt.Errorf("sqlite persistence: %w", err)
		}
		e.persistence = p
		return nil
	}
}

// WithPersistence configures the engine with a custom Persistence implementation,
// replacing the default in-memory store. Use this to integrate with databases or
// storage backends not covered by WithSQLite.
//
// The provided implementation must be safe for concurrent use. The engine calls
// Close on the persistence layer during Engine.Close, so the implementation
// should release resources there.
//
// Without this option (or WithSQLite), the engine uses an in-memory store.
func WithPersistence(p Persistence) EngineOption {
	return func(e *Engine) error {
		e.persistence = p
		return nil
	}
}

// WithLogger configures the engine's logger for operational messages such as
// persistence errors, shutdown warnings, and recovery progress.
//
// Without this option, the engine uses a no-op logger (DefaultLogger) that
// silently discards all messages.
func WithLogger(logger Logger) EngineOption {
	return func(e *Engine) error {
		e.logger = logger
		return nil
	}
}

// WithTimeout sets a global per-run timeout applied to every workflow executed
// via RunSync. When the timeout elapses, the run's context is cancelled and
// the workflow receives a context.DeadlineExceeded error, resulting in a
// Cancelled status in the Result.
//
// This timeout does not apply to runs started via Run (asynchronous); those
// inherit the caller's context deadline.
//
// Without this option, RunSync runs have no engine-imposed deadline (they rely
// on the caller's context).
func WithTimeout(d time.Duration) EngineOption {
	return func(e *Engine) error {
		e.timeout = d
		return nil
	}
}

// WithWorkerPoolSize sets the number of worker goroutines in the engine's
// bounded worker pool. The pool executes asynchronous runs (via Run) and
// timer-based wake operations for sleeping workflows.
//
// Without this option, the pool size defaults to 2 * runtime.NumCPU(), with a
// minimum of 4. When all workers are busy, new submissions block until a
// worker becomes available (backpressure).
func WithWorkerPoolSize(n int) EngineOption {
	return func(e *Engine) error {
		e.poolSize = n
		return nil
	}
}

// WithEngineMiddleware adds engine-level middleware that wraps the entire
// workflow execution. Multiple middleware are applied in registration order,
// forming an onion-style chain where the first registered middleware is the
// outermost wrapper. Panics inside middleware are recovered and surfaced as
// errors.
func WithEngineMiddleware(m EngineMiddleware) EngineOption {
	return func(e *Engine) error {
		e.engineMiddleware = append(e.engineMiddleware, m)
		return nil
	}
}

// WithStepMiddleware adds step-level middleware that wraps individual step
// execution. The middleware receives step metadata (ID, name, kind, tags) and
// can inspect or modify the execution by calling or skipping the next function.
// Multiple middleware are applied in registration order.
func WithStepMiddleware(m StepMiddleware) EngineOption {
	return func(e *Engine) error {
		e.stepMiddleware = append(e.stepMiddleware, m)
		return nil
	}
}

// WithTaskMiddleware adds task-level middleware that wraps task function
// invocation. The middleware receives the task context and task name, and can
// add cross-cutting behavior such as logging, metrics, or access control.
// Multiple middleware are applied in registration order.
func WithTaskMiddleware(m TaskMiddleware) EngineOption {
	return func(e *Engine) error {
		e.taskMiddleware = append(e.taskMiddleware, m)
		return nil
	}
}

// WithChildMiddleware adds child-process-level middleware that wraps child
// process execution in ForEach spawn operations. The middleware receives the
// SpawnJob with job metadata and can add pre/post-processing around child
// execution. Multiple middleware are applied in registration order.
func WithChildMiddleware(m ChildMiddleware) EngineOption {
	return func(e *Engine) error {
		e.childMiddleware = append(e.childMiddleware, m)
		return nil
	}
}

// WithPlugin registers middleware at all levels supported by the plugin in a
// single call. The Plugin interface has methods for each middleware level
// (EngineMiddleware, StepMiddleware, TaskMiddleware, ChildMiddleware); any
// method that returns nil is skipped. This is a convenience for plugins that
// need to install middleware at multiple levels.
func WithPlugin(p Plugin) EngineOption {
	return func(e *Engine) error {
		if m := p.EngineMiddleware(); m != nil {
			e.engineMiddleware = append(e.engineMiddleware, m)
		}
		if m := p.StepMiddleware(); m != nil {
			e.stepMiddleware = append(e.stepMiddleware, m)
		}
		if m := p.TaskMiddleware(); m != nil {
			e.taskMiddleware = append(e.taskMiddleware, m)
		}
		if m := p.ChildMiddleware(); m != nil {
			e.childMiddleware = append(e.childMiddleware, m)
		}
		return nil
	}
}

// WithAutoRecover enables automatic crash recovery during New. When set, the
// engine calls Recover immediately after starting the worker pool and timer
// scheduler. This scans persistence for interrupted workflows: runs that were
// Running are marked Failed (crashed mid-execution), sleeping runs past their
// wake time are resumed immediately, and sleeping runs with future wake times
// are re-registered with the timer scheduler.
//
// If recovery encounters an error listing runs from persistence, New returns
// that error and the engine is not created.
//
// Without this option, interrupted runs from previous process lifetimes remain
// in their last persisted state until Recover is called manually.
func WithAutoRecover() EngineOption {
	return func(e *Engine) error {
		// Recovery happens after all options are applied, in New() post-init
		// We mark the intent here; actual recovery runs after pool/scheduler start
		e.autoRecover = true
		return nil
	}
}

// WithCacheSize limits the engine's in-memory workflow template cache to n
// entries. The cache stores workflow definitions for timer-based sleep/wake
// recovery and Resume operations, using LRU (least-recently-used) eviction
// when at capacity.
//
// Workflows pinned by sleeping runs are protected from eviction. If all cached
// entries are pinned, the cache grows beyond the limit rather than evicting a
// sleeping workflow's definition (which would cause the wake to fail).
//
// A value of 0 means unlimited (no eviction). The default is 1000.
func WithCacheSize(n int) EngineOption {
	return func(e *Engine) error {
		e.cacheSize = n
		return nil
	}
}

// WithShutdownTimeout sets the maximum time Close waits for the worker pool to
// drain in-flight jobs. If workers do not finish within the deadline, Close
// proceeds with a bounded grace period and logs a warning. This prevents
// Close from blocking indefinitely when a workflow is stuck.
//
// Without this option, the default is 0 (Close blocks indefinitely until all
// workers finish).
func WithShutdownTimeout(d time.Duration) EngineOption {
	return func(e *Engine) error {
		e.shutdownTimeout = d
		return nil
	}
}

// WithStateLimit limits the number of entries in each workflow run's key-value
// state store. When the limit is reached, attempts to set a new key are
// rejected with ErrStateLimitExceeded. Updates to existing keys always succeed
// regardless of the limit. This prevents runaway workflows from consuming
// unbounded memory or persistence storage.
//
// Without this option, the default is 0 (unlimited entries).
func WithStateLimit(n int) EngineOption {
	return func(e *Engine) error {
		e.stateLimit = n
		return nil
	}
}

// WithRunGC enables automatic background garbage collection of terminal runs.
// A background goroutine wakes every interval and deletes runs in terminal
// status (Completed, Failed, Bailed, Cancelled) whose UpdatedAt timestamp is
// older than ttl. The goroutine stops when the engine is closed.
//
// The ttl parameter controls the minimum age before a run is eligible for
// deletion. A ttl of 0 means all terminal runs are purged on every sweep
// regardless of age. The interval parameter controls how often the sweep runs.
//
// Without this option, terminal runs accumulate in persistence indefinitely
// until manually removed via PurgeRuns or DeleteRun.
//
//	engine, err := gostage.New(gostage.WithRunGC(24*time.Hour, 5*time.Minute))
func WithRunGC(ttl, interval time.Duration) EngineOption {
	return func(e *Engine) error {
		e.gcTTL = ttl
		e.gcInterval = interval
		return nil
	}
}

// WithEventHandler registers an observer for engine lifecycle events such as
// run creation, completion, failure, suspension, sleep, cancellation, wake,
// and deletion. Multiple handlers can be registered and are called in
// registration order.
//
// Handlers are invoked synchronously in the goroutine that triggers the event,
// so they should return quickly to avoid blocking workflow execution. Panics
// inside handlers are recovered and logged; they do not propagate to the
// workflow or crash the engine.
//
// Handlers must be safe for concurrent use, as events may fire from multiple
// goroutines simultaneously (e.g., the worker pool and the timer scheduler).
func WithEventHandler(h EventHandler) EngineOption {
	return func(e *Engine) error {
		e.eventHandlers = append(e.eventHandlers, h)
		return nil
	}
}

// withNoPool is an unexported option used by HandleChild to skip starting the
// worker pool. Child processes never submit async jobs via engine.Run(), so the
// pool would idle for the entire child lifetime without being used.
func withNoPool() EngineOption {
	return func(e *Engine) error {
		e.noPool = true
		return nil
	}
}

// withNoScheduler is an unexported option used by HandleChild to skip starting
// the timer scheduler. Child processes use in-memory persistence and never enter
// sleep steps durably, so the scheduler goroutine would idle without being used.
func withNoScheduler() EngineOption {
	return func(e *Engine) error {
		e.noScheduler = true
		return nil
	}
}
