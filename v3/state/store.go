package state

import "context"

type Store interface {
	RecordWorkflow(ctx context.Context, rec WorkflowRecord) error
	UpdateWorkflowStatus(ctx context.Context, update WorkflowStatusUpdate) error
	RecordStage(ctx context.Context, workflowID WorkflowID, stage StageRecord) error
	UpdateStageStatus(ctx context.Context, update StageStatusUpdate) error
	RecordAction(ctx context.Context, workflowID WorkflowID, stageID string, action ActionRecord) error
	UpdateActionStatus(ctx context.Context, update ActionStatusUpdate) error
	StoreSummary(ctx context.Context, id WorkflowID, summary ResultSummary) error
	WaitResult(ctx context.Context, id WorkflowID) (ResultSummary, error)
	Close() error
}

// ProgressRecorder is an optional extension implemented by stores that can
// persist incremental action progress updates.
type ProgressRecorder interface {
	ActionProgress(ctx context.Context, workflowID, stageID, actionID string, progress int, message string) error
}
