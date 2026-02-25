package gostage

import (
	"fmt"
	"time"
)

// WithSQLite configures the engine to use SQLite for persistence.
//
//	engine, _ := gostage.New(gostage.WithSQLite("app.db"))
//	engine, _ := gostage.New(gostage.WithSQLite(":memory:"))
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

// WithPersistence configures the engine with a custom Persistence implementation.
func WithPersistence(p Persistence) EngineOption {
	return func(e *Engine) error {
		e.persistence = p
		return nil
	}
}

// WithLogger configures the engine's logger.
func WithLogger(logger Logger) EngineOption {
	return func(e *Engine) error {
		e.logger = logger
		return nil
	}
}

// WithTimeout sets a global timeout for workflow execution.
func WithTimeout(d time.Duration) EngineOption {
	return func(e *Engine) error {
		e.timeout = d
		return nil
	}
}

// WithWorkerPoolSize sets the number of worker goroutines in the pool.
// Default is 2 * runtime.NumCPU().
func WithWorkerPoolSize(n int) EngineOption {
	return func(e *Engine) error {
		e.poolSize = n
		return nil
	}
}

// WithEngineMiddleware adds engine-level middleware.
func WithEngineMiddleware(m EngineMiddleware) EngineOption {
	return func(e *Engine) error {
		e.engineMiddleware = append(e.engineMiddleware, m)
		return nil
	}
}

// WithStepMiddleware adds step-level middleware.
func WithStepMiddleware(m StepMiddleware) EngineOption {
	return func(e *Engine) error {
		e.stepMiddleware = append(e.stepMiddleware, m)
		return nil
	}
}

// WithTaskMiddleware adds task-level middleware.
func WithTaskMiddleware(m TaskMiddleware) EngineOption {
	return func(e *Engine) error {
		e.taskMiddleware = append(e.taskMiddleware, m)
		return nil
	}
}

// WithChildMiddleware adds child-process-level middleware.
func WithChildMiddleware(m ChildMiddleware) EngineOption {
	return func(e *Engine) error {
		e.childMiddleware = append(e.childMiddleware, m)
		return nil
	}
}

// WithPlugin registers middleware at all levels supported by the plugin.
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

// WithAutoRecover enables automatic crash recovery on engine startup.
// The engine scans persistence for interrupted workflows and resumes them.
func WithAutoRecover() EngineOption {
	return func(e *Engine) error {
		// Recovery happens after all options are applied, in New() post-init
		// We mark the intent here; actual recovery runs after pool/scheduler start
		e.autoRecover = true
		return nil
	}
}

// WithCacheSize limits the workflow cache to n entries.
// When full, the least-recently-used entry is evicted. 0 means unlimited.
// Default is 1000.
func WithCacheSize(n int) EngineOption {
	return func(e *Engine) error {
		e.cacheSize = n
		return nil
	}
}

// WithShutdownTimeout sets the maximum time Close() waits for workers to finish.
// If workers do not finish within the deadline, Close() proceeds and logs a warning.
// Default is 0 (blocks indefinitely).
func WithShutdownTimeout(d time.Duration) EngineOption {
	return func(e *Engine) error {
		e.shutdownTimeout = d
		return nil
	}
}

// WithStateLimit limits the number of entries in each workflow run's state.
// When the limit is reached, new keys are rejected with ErrStateLimitExceeded.
// Updating an existing key always succeeds. 0 means unlimited (default).
func WithStateLimit(n int) EngineOption {
	return func(e *Engine) error {
		e.stateLimit = n
		return nil
	}
}
