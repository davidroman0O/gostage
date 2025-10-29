package engine

import (
	rt "github.com/davidroman0O/gostage/v3/runtime"
)

// ExecutionStatus represents the lifecycle state of a stage or action.
type ExecutionStatus string

const (
	StatusPending   ExecutionStatus = "pending"
	StatusRunning   ExecutionStatus = "running"
	StatusCompleted ExecutionStatus = "completed"
	StatusFailed    ExecutionStatus = "failed"
	StatusSkipped   ExecutionStatus = "skipped"
	StatusRemoved   ExecutionStatus = "removed"
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
