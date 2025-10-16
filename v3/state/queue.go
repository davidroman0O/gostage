package state

import (
	"context"

	"github.com/davidroman0O/gostage/v3/workflow"
)

type Queue interface {
	Enqueue(ctx context.Context, def workflow.Definition, priority Priority, metadata map[string]any) (WorkflowID, error)
	Claim(ctx context.Context, sel Selector, workerID string) (*ClaimedWorkflow, error)
	Release(ctx context.Context, id WorkflowID) error
	Ack(ctx context.Context, id WorkflowID, summary ResultSummary) error
	Cancel(ctx context.Context, id WorkflowID) error
	Stats(ctx context.Context) (QueueStats, error)
	PendingCount(ctx context.Context, sel Selector) (int, error)
	Close() error
}
