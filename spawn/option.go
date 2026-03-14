// Package spawn provides gRPC-based child process spawning for ForEach steps.
// Use WithSpawn() as an engine option to enable it, and WithSpawn() on individual
// ForEach steps to opt in to process-level isolation per iteration.
package spawn

import gostage "github.com/davidroman0O/gostage"

// WithSpawn configures the engine to support ForEach with child process
// spawning via gRPC. Without this option, ForEach steps using WithSpawn()
// will return an error at runtime.
//
//	engine, err := gostage.New(spawn.WithSpawn())
func WithSpawn() gostage.EngineOption {
	return func(e *gostage.Engine) error {
		e.SetSpawnRunner(NewRunner())
		return nil
	}
}
