package gostage

import (
	"context"
	"time"

	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

type telemetryManager struct {
	delegate   state.Manager
	dispatcher *node.TelemetryDispatcher
}

func wrapWithTelemetry(delegate state.Manager, dispatcher *node.TelemetryDispatcher) state.Manager {
	if dispatcher == nil {
		return delegate
	}
	return &telemetryManager{
		delegate:   delegate,
		dispatcher: dispatcher,
	}
}

func (m *telemetryManager) WorkflowRegistered(ctx context.Context, wf state.WorkflowRecord) error {
	if err := m.delegate.WorkflowRegistered(ctx, wf); err != nil {
		return err
	}
	m.emit(telemetry.Event{
		Kind:       telemetry.EventWorkflowRegistered,
		WorkflowID: string(wf.ID),
		Metadata: map[string]any{
			"name": wf.Name,
			"tags": append([]string(nil), wf.Tags...),
			"type": wf.Type,
		},
	})
	return nil
}

func (m *telemetryManager) WorkflowStatus(ctx context.Context, workflowID string, status state.WorkflowState) error {
	if err := m.delegate.WorkflowStatus(ctx, workflowID, status); err != nil {
		return err
	}
	m.emit(telemetry.Event{
		Kind:       telemetry.EventKind("workflow." + string(status)),
		WorkflowID: workflowID,
		Metadata: map[string]any{
			"status": status,
		},
	})
	return nil
}

func (m *telemetryManager) StageRegistered(ctx context.Context, workflowID string, stage state.StageRecord) error {
	if err := m.delegate.StageRegistered(ctx, workflowID, stage); err != nil {
		return err
	}
	m.emit(telemetry.Event{
		Kind:       telemetry.EventStageRegistered,
		WorkflowID: workflowID,
		StageID:    stage.ID,
		Metadata: map[string]any{
			"name":       stage.Name,
			"tags":       append([]string(nil), stage.Tags...),
			"dynamic":    stage.Dynamic,
			"created_by": stage.CreatedBy,
			"status":     stage.Status,
		},
	})
	return nil
}

func (m *telemetryManager) StageStatus(ctx context.Context, workflowID, stageID string, status state.WorkflowState) error {
	if err := m.delegate.StageStatus(ctx, workflowID, stageID, status); err != nil {
		return err
	}
	m.emit(telemetry.Event{
		Kind:       telemetry.EventKind("stage." + string(status)),
		WorkflowID: workflowID,
		StageID:    stageID,
		Metadata: map[string]any{
			"status": status,
		},
	})
	return nil
}

func (m *telemetryManager) ActionRegistered(ctx context.Context, workflowID, stageID string, action state.ActionRecord) error {
	if err := m.delegate.ActionRegistered(ctx, workflowID, stageID, action); err != nil {
		return err
	}
	m.emit(telemetry.Event{
		Kind:       telemetry.EventActionRegistered,
		WorkflowID: workflowID,
		StageID:    stageID,
		ActionID:   action.Name,
		Metadata: map[string]any{
			"description": action.Description,
			"tags":        append([]string(nil), action.Tags...),
			"dynamic":     action.Dynamic,
			"created_by":  action.CreatedBy,
			"status":      action.Status,
		},
	})
	return nil
}

func (m *telemetryManager) ActionStatus(ctx context.Context, workflowID, stageID, actionName string, status state.WorkflowState) error {
	if err := m.delegate.ActionStatus(ctx, workflowID, stageID, actionName, status); err != nil {
		return err
	}
	m.emit(telemetry.Event{
		Kind:       telemetry.EventKind("action." + string(status)),
		WorkflowID: workflowID,
		StageID:    stageID,
		ActionID:   actionName,
		Metadata: map[string]any{
			"status": status,
		},
	})
	return nil
}

func (m *telemetryManager) ActionProgress(ctx context.Context, workflowID, stageID, actionName string, progress int, message string) error {
	if err := m.delegate.ActionProgress(ctx, workflowID, stageID, actionName, progress, message); err != nil {
		return err
	}
	metadata := map[string]any{
		"progress": progress,
	}
	if message != "" {
		metadata["message"] = message
	}
	m.emit(telemetry.Event{
		Kind:       telemetry.EventActionProgressKind,
		WorkflowID: workflowID,
		StageID:    stageID,
		ActionID:   actionName,
		Message:    message,
		Progress: &telemetry.Progress{
			Percent: progress,
			Message: message,
		},
		Metadata: metadata,
	})
	return nil
}

func (m *telemetryManager) ActionEvent(ctx context.Context, workflowID, stageID, actionName, kind, message string, metadata map[string]any) error {
	if err := m.delegate.ActionEvent(ctx, workflowID, stageID, actionName, kind, message, metadata); err != nil {
		return err
	}
	var metaCopy map[string]any
	if len(metadata) > 0 {
		metaCopy = make(map[string]any, len(metadata))
		for k, v := range metadata {
			metaCopy[k] = v
		}
	}
	m.emit(telemetry.Event{
		Kind:       telemetry.EventKind(kind),
		WorkflowID: workflowID,
		StageID:    stageID,
		ActionID:   actionName,
		Message:    message,
		Metadata:   metaCopy,
	})
	return nil
}

func (m *telemetryManager) ActionRemoved(ctx context.Context, workflowID, stageID, actionName, createdBy string) error {
	if err := m.delegate.ActionRemoved(ctx, workflowID, stageID, actionName, createdBy); err != nil {
		return err
	}
	m.emit(telemetry.Event{
		Kind:       telemetry.EventActionRemoved,
		WorkflowID: workflowID,
		StageID:    stageID,
		ActionID:   actionName,
		Metadata: map[string]any{
			"removed_by": createdBy,
		},
	})
	return nil
}

func (m *telemetryManager) StageRemoved(ctx context.Context, workflowID, stageID, createdBy string) error {
	if err := m.delegate.StageRemoved(ctx, workflowID, stageID, createdBy); err != nil {
		return err
	}
	m.emit(telemetry.Event{
		Kind:       telemetry.EventStageRemoved,
		WorkflowID: workflowID,
		StageID:    stageID,
		Metadata: map[string]any{
			"removed_by": createdBy,
		},
	})
	return nil
}

func (m *telemetryManager) StoreExecutionSummary(ctx context.Context, workflowID string, report state.ExecutionReport) error {
	if err := m.delegate.StoreExecutionSummary(ctx, workflowID, report); err != nil {
		return err
	}
	metadata := map[string]any{
		"success": report.Success,
		"status":  report.Status,
	}
	if report.ErrorMessage != "" {
		metadata["error"] = report.ErrorMessage
	}
	if report.Duration > 0 {
		metadata["duration"] = report.Duration
	}
	if report.Attempt > 0 {
		metadata["attempt"] = report.Attempt
	}
	m.emit(telemetry.Event{
		Kind:       telemetry.EventWorkflowSummary,
		WorkflowID: workflowID,
		Error:      report.ErrorMessage,
		Metadata:   metadata,
	})
	return nil
}

func (m *telemetryManager) emit(evt telemetry.Event) {
	if m.dispatcher == nil {
		return
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now()
	}
	m.dispatcher.Dispatch(evt)
}
