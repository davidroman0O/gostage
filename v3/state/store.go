package state

import (
	"context"

	"github.com/davidroman0O/gostage/v3/workflow"
)

type Store interface {
	RecordWorkflow(ctx context.Context, rec WorkflowRecord) error
	RecordStage(ctx context.Context, workflowID WorkflowID, stage workflow.Stage, dynamic bool, createdBy string, state WorkflowState) error
	RecordAction(ctx context.Context, workflowID WorkflowID, stageID string, action workflow.Action, dynamic bool, createdBy string, state WorkflowState) error
	StoreSummary(ctx context.Context, id WorkflowID, summary ResultSummary) error
	WaitResult(ctx context.Context, id WorkflowID) (ResultSummary, error)
	Close() error
}
