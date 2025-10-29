package state

import "context"

// Writer captures the persistence operations exercised by the scheduler and
// remote coordinator when recording workflow execution state.
type Writer interface {
	WorkflowRegistered(ctx context.Context, wf WorkflowRecord) error
	WorkflowStatus(ctx context.Context, workflowID string, status WorkflowState) error
	StageRegistered(ctx context.Context, workflowID string, stage StageRecord) error
	StageStatus(ctx context.Context, workflowID, stageID string, status WorkflowState) error
	ActionRegistered(ctx context.Context, workflowID, stageID string, action ActionRecord) error
	ActionStatus(ctx context.Context, workflowID, stageID, actionName string, status WorkflowState) error
	ActionProgress(ctx context.Context, workflowID, stageID, actionName string, progress int, message string) error
	ActionEvent(ctx context.Context, workflowID, stageID, actionName, kind, message string, metadata map[string]any) error
	ActionRemoved(ctx context.Context, workflowID, stageID, actionName, createdBy string) error
	StageRemoved(ctx context.Context, workflowID, stageID, createdBy string) error
	StoreExecutionSummary(ctx context.Context, workflowID string, report ExecutionReport) error
}
