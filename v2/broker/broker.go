package broker

import (
	"context"

	"github.com/davidroman0O/gostage/v2/state"
)

// Broker defines the event reporting contract used by the runner.
type Broker interface {
	WorkflowRegistered(ctx context.Context, wf state.WorkflowRecord) error
	WorkflowStatus(ctx context.Context, workflowID string, status state.WorkflowState) error
	StageRegistered(ctx context.Context, workflowID string, stage state.StageRecord) error
	StageStatus(ctx context.Context, workflowID, stageID string, status state.WorkflowState) error
	ActionRegistered(ctx context.Context, workflowID, stageID string, action state.ActionRecord) error
	ActionStatus(ctx context.Context, workflowID, stageID, actionName string, status state.WorkflowState) error
	ActionProgress(ctx context.Context, workflowID, stageID, actionName string, progress int, message string) error
	ActionRemoved(ctx context.Context, workflowID, stageID, actionName, createdBy string) error
	StageRemoved(ctx context.Context, workflowID, stageID, createdBy string) error
	ExecutionSummary(ctx context.Context, workflowID string, report state.ExecutionReport) error
}

// Local implements Broker on top of a state.Manager.
type Local struct {
	manager state.Manager
}

// NewLocal creates a Broker backed by the provided manager.
func NewLocal(manager state.Manager) *Local {
	return &Local{manager: manager}
}

func (l *Local) WorkflowRegistered(ctx context.Context, wf state.WorkflowRecord) error {
	return l.manager.WorkflowRegistered(ctx, wf)
}

func (l *Local) WorkflowStatus(ctx context.Context, workflowID string, status state.WorkflowState) error {
	return l.manager.WorkflowStatus(ctx, workflowID, status)
}

func (l *Local) StageRegistered(ctx context.Context, workflowID string, stage state.StageRecord) error {
	return l.manager.StageRegistered(ctx, workflowID, stage)
}

func (l *Local) StageStatus(ctx context.Context, workflowID, stageID string, status state.WorkflowState) error {
	return l.manager.StageStatus(ctx, workflowID, stageID, status)
}

func (l *Local) ActionRegistered(ctx context.Context, workflowID, stageID string, action state.ActionRecord) error {
	return l.manager.ActionRegistered(ctx, workflowID, stageID, action)
}

func (l *Local) ActionStatus(ctx context.Context, workflowID, stageID, actionName string, status state.WorkflowState) error {
	return l.manager.ActionStatus(ctx, workflowID, stageID, actionName, status)
}

func (l *Local) ActionProgress(ctx context.Context, workflowID, stageID, actionName string, progress int, message string) error {
	return l.manager.ActionProgress(ctx, workflowID, stageID, actionName, progress, message)
}

func (l *Local) ActionRemoved(ctx context.Context, workflowID, stageID, actionName, createdBy string) error {
	return l.manager.ActionRemoved(ctx, workflowID, stageID, actionName, createdBy)
}

func (l *Local) StageRemoved(ctx context.Context, workflowID, stageID, createdBy string) error {
	return l.manager.StageRemoved(ctx, workflowID, stageID, createdBy)
}

func (l *Local) ExecutionSummary(ctx context.Context, workflowID string, report state.ExecutionReport) error {
	return l.manager.StoreExecutionSummary(ctx, workflowID, report)
}
