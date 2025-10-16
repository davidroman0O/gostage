package state

import "context"

// ManagerObserver receives callbacks when the StoreManager records lifecycle
// events for workflows, stages, actions, and summaries. All methods are
// optional; implementers can embed ObserverFuncs to override selected hooks.
type ManagerObserver interface {
	WorkflowRegistered(ctx context.Context, wf WorkflowRecord)
	WorkflowStatus(ctx context.Context, workflowID string, status WorkflowState)
	StageRegistered(ctx context.Context, workflowID string, stage StageRecord)
	StageStatus(ctx context.Context, workflowID, stageID string, status WorkflowState)
	ActionRegistered(ctx context.Context, workflowID, stageID string, action ActionRecord)
	ActionStatus(ctx context.Context, workflowID, stageID, actionID string, status WorkflowState)
	ActionProgress(ctx context.Context, workflowID, stageID, actionID string, progress int, message string)
	ActionEvent(ctx context.Context, workflowID, stageID, actionID, kind, message string, metadata map[string]any)
	ActionRemoved(ctx context.Context, workflowID, stageID, actionID, createdBy string)
	StageRemoved(ctx context.Context, workflowID, stageID, createdBy string)
	ExecutionSummary(ctx context.Context, workflowID string, summary ResultSummary)
}

// ObserverFuncs implements ManagerObserver by delegating to function fields.
// Functions left nil are treated as no-ops.
type ObserverFuncs struct {
	OnWorkflowRegistered func(ctx context.Context, wf WorkflowRecord)
	OnWorkflowStatus     func(ctx context.Context, workflowID string, status WorkflowState)
	OnStageRegistered    func(ctx context.Context, workflowID string, stage StageRecord)
	OnStageStatus        func(ctx context.Context, workflowID, stageID string, status WorkflowState)
	OnActionRegistered   func(ctx context.Context, workflowID, stageID string, action ActionRecord)
	OnActionStatus       func(ctx context.Context, workflowID, stageID, actionID string, status WorkflowState)
	OnActionProgress     func(ctx context.Context, workflowID, stageID, actionID string, progress int, message string)
	OnActionEvent        func(ctx context.Context, workflowID, stageID, actionID, kind, message string, metadata map[string]any)
	OnActionRemoved      func(ctx context.Context, workflowID, stageID, actionID, createdBy string)
	OnStageRemoved       func(ctx context.Context, workflowID, stageID, createdBy string)
	OnExecutionSummary   func(ctx context.Context, workflowID string, summary ResultSummary)
}

func (o ObserverFuncs) WorkflowRegistered(ctx context.Context, wf WorkflowRecord) {
	if o.OnWorkflowRegistered != nil {
		o.OnWorkflowRegistered(ctx, wf)
	}
}

func (o ObserverFuncs) WorkflowStatus(ctx context.Context, workflowID string, status WorkflowState) {
	if o.OnWorkflowStatus != nil {
		o.OnWorkflowStatus(ctx, workflowID, status)
	}
}

func (o ObserverFuncs) StageRegistered(ctx context.Context, workflowID string, stage StageRecord) {
	if o.OnStageRegistered != nil {
		o.OnStageRegistered(ctx, workflowID, stage)
	}
}

func (o ObserverFuncs) StageStatus(ctx context.Context, workflowID, stageID string, status WorkflowState) {
	if o.OnStageStatus != nil {
		o.OnStageStatus(ctx, workflowID, stageID, status)
	}
}

func (o ObserverFuncs) ActionRegistered(ctx context.Context, workflowID, stageID string, action ActionRecord) {
	if o.OnActionRegistered != nil {
		o.OnActionRegistered(ctx, workflowID, stageID, action)
	}
}

func (o ObserverFuncs) ActionStatus(ctx context.Context, workflowID, stageID, actionID string, status WorkflowState) {
	if o.OnActionStatus != nil {
		o.OnActionStatus(ctx, workflowID, stageID, actionID, status)
	}
}

func (o ObserverFuncs) ActionProgress(ctx context.Context, workflowID, stageID, actionID string, progress int, message string) {
	if o.OnActionProgress != nil {
		o.OnActionProgress(ctx, workflowID, stageID, actionID, progress, message)
	}
}

func (o ObserverFuncs) ActionEvent(ctx context.Context, workflowID, stageID, actionID, kind, message string, metadata map[string]any) {
	if o.OnActionEvent != nil {
		o.OnActionEvent(ctx, workflowID, stageID, actionID, kind, message, metadata)
	}
}

func (o ObserverFuncs) ActionRemoved(ctx context.Context, workflowID, stageID, actionID, createdBy string) {
	if o.OnActionRemoved != nil {
		o.OnActionRemoved(ctx, workflowID, stageID, actionID, createdBy)
	}
}

func (o ObserverFuncs) StageRemoved(ctx context.Context, workflowID, stageID, createdBy string) {
	if o.OnStageRemoved != nil {
		o.OnStageRemoved(ctx, workflowID, stageID, createdBy)
	}
}

func (o ObserverFuncs) ExecutionSummary(ctx context.Context, workflowID string, summary ResultSummary) {
	if o.OnExecutionSummary != nil {
		o.OnExecutionSummary(ctx, workflowID, summary)
	}
}

// ManagerOption mutates StoreManager construction.
type ManagerOption func(*StoreManager)

// WithManagerObservers registers one or more observers that will receive
// lifecycle callbacks from the StoreManager.
func WithManagerObservers(obs ...ManagerObserver) ManagerOption {
	return func(m *StoreManager) {
		if len(obs) == 0 {
			return
		}
		m.observers = append(m.observers, obs...)
	}
}
