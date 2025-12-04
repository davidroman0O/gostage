// Package engine provides execution status tracking for workflow stages and actions.
package engine

import (
	rt "github.com/davidroman0O/gostage/v3/shared/runtime"
)

// ExecutionStatus represents the lifecycle state of a stage or action.
type ExecutionStatus string

const (
	// StatusPending indicates the stage/action is waiting to be executed.
	StatusPending ExecutionStatus = "pending"
	// StatusRunning indicates the stage/action is currently executing.
	StatusRunning ExecutionStatus = "running"
	// StatusCompleted indicates the stage/action completed successfully.
	StatusCompleted ExecutionStatus = "completed"
	// StatusFailed indicates the stage/action failed.
	StatusFailed ExecutionStatus = "failed"
	// StatusSkipped indicates the stage/action was skipped.
	StatusSkipped ExecutionStatus = "skipped"
	// StatusRemoved indicates the stage/action was removed.
	StatusRemoved ExecutionStatus = "removed"
	// StatusCancelled indicates the stage/action was cancelled.
	StatusCancelled ExecutionStatus = "cancelled"
)

// StageStatus captures lifecycle details for a stage.
type StageStatus struct {
	ID          string
	Name        string
	Description string
	Tags        []string
	Status      ExecutionStatus
	Dynamic     bool
	CreatedBy   string
}

// ActionStatus captures lifecycle details for an action.
type ActionStatus struct {
	StageID     string
	Name        string
	Description string
	Tags        []string
	Status      ExecutionStatus
	Dynamic     bool
	CreatedBy   string
}

// DynamicStage describes a stage generated during execution.
type DynamicStage struct {
	Stage     rt.Stage
	CreatedBy string
}

// DynamicAction describes an action generated during execution.
type DynamicAction struct {
	StageID   string
	Action    rt.Action
	CreatedBy string
}
