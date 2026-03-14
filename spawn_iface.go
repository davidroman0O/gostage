package gostage

import "context"

// SpawnConfig carries the step-level configuration needed by a SpawnRunner
// to execute ForEach items in child processes. This avoids exposing the
// unexported step type across package boundaries.
type SpawnConfig struct {
	StepID        string
	TaskName      string
	SubWorkflow   *Workflow
	Concurrency   int
	CollectionKey string
}

// SpawnRunner executes ForEach items in isolated child processes.
// The spawn sub-package provides the default gRPC-based implementation.
type SpawnRunner interface {
	ExecuteForEachSpawn(ctx context.Context, e *Engine, wf *Workflow, cfg SpawnConfig, items []any, runID RunID, resuming bool) error
	Close() error
}
