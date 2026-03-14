package gostage

// Accessor methods for the spawn sub-package.
// These expose internal Engine fields through exported methods so the
// spawn package can operate on them without import cycles.

// EnginePersistence returns the engine's persistence layer.
func (e *Engine) EnginePersistence() Persistence { return e.persistence }

// EngineLogger returns the engine's logger.
func (e *Engine) EngineLogger() Logger { return e.logger }

// EngineRegistry returns the engine's task/condition/map registry.
func (e *Engine) EngineRegistry() *Registry { return e.registry }

// StepMiddlewareChain returns the engine's step middleware chain.
func (e *Engine) StepMiddlewareChain() []StepMiddleware { return e.stepMiddleware }

// TaskMiddlewareChain returns the engine's task middleware chain.
func (e *Engine) TaskMiddlewareChain() []TaskMiddleware { return e.taskMiddleware }

// ChildMiddlewareChain returns the engine's child middleware chain.
func (e *Engine) ChildMiddlewareChain() []ChildMiddleware { return e.childMiddleware }

// EngineStateLimit returns the max state entries per run.
func (e *Engine) EngineStateLimit() int { return e.stateLimit }

// SetSpawnRunner installs a SpawnRunner on the engine.
// Called by the spawn sub-package's WithSpawn() engine option.
func (e *Engine) SetSpawnRunner(r SpawnRunner) { e.spawnRunner = r }

// DispatchMessage routes an IPC message through the engine's handler system.
func (e *Engine) DispatchMessage(msgType string, payload map[string]any, runID RunID) {
	e.dispatchMessage(msgType, payload, runID)
}

// EmitEvent sends a lifecycle event to all registered event handlers.
func (e *Engine) EmitEvent(event EngineEvent) {
	e.emitEvent(event)
}

// CloseCh returns the channel that is closed when the engine shuts down.
func (e *Engine) CloseCh() <-chan struct{} { return e.closeCh }

// WithNoPool exports the unexported withNoPool option for the spawn package.
// Child processes use this to skip creating the worker pool.
func WithNoPool() EngineOption { return withNoPool() }

// WithNoScheduler exports the unexported withNoScheduler option for the spawn package.
// Child processes use this to skip creating the timer scheduler.
func WithNoScheduler() EngineOption { return withNoScheduler() }
