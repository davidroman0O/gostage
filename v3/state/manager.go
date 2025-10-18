package state

import (
	"context"
	"errors"
	"maps"
	"time"

	"github.com/davidroman0O/gostage/v3/internal/locks"
)

// StoreManager bridges the legacy state.Manager contract used by the runner
// with the new queue/store implementation backed by sqlc. It keeps an
// in-memory shadow of workflow metadata so incremental status updates can be
// persisted without the runner needing to resend the full record each time.
type StoreManager struct {
	store Store

	mu        locks.RWMutex
	workflows map[WorkflowID]*workflowSnapshot

	now       func() time.Time
	observers []ManagerObserver
}

type workflowSnapshot struct {
	record  WorkflowRecord
	summary *ResultSummary
}

// NewStoreManager constructs a Manager backed by the provided Store. The
// returned Manager is safe for concurrent use.
func NewStoreManager(store Store, opts ...ManagerOption) (*StoreManager, error) {
	if store == nil {
		return nil, errors.New("state: store manager requires Store")
	}
	mgr := &StoreManager{
		store:     store,
		workflows: make(map[WorkflowID]*workflowSnapshot),
		now:       time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(mgr)
		}
	}
	return mgr, nil
}

// WorkflowRegistered records the workflow metadata and initial status.
func (m *StoreManager) WorkflowRegistered(ctx context.Context, wf WorkflowRecord) error {
	if wf.ID == "" {
		return errors.New("state: workflow id required")
	}
	clone := cloneWorkflowRecord(wf)
	if clone.CreatedAt.IsZero() {
		clone.CreatedAt = m.now()
	}

	m.mu.Lock()
	m.workflows[clone.ID] = &workflowSnapshot{record: clone}
	m.mu.Unlock()

	if err := m.store.RecordWorkflow(ctx, clone); err != nil {
		return err
	}
	m.notifyWorkflowRegistered(ctx, clone)
	return nil
}

// WorkflowStatus updates the workflow state and persists it.
func (m *StoreManager) WorkflowStatus(ctx context.Context, workflowID string, status WorkflowState) error {
	id := WorkflowID(workflowID)
	snap, err := m.getSnapshot(id)
	if err != nil {
		return err
	}

	now := m.now()
	switch status {
	case WorkflowRunning:
		if snap.record.StartedAt == nil {
			snap.record.StartedAt = &now
		}
	case WorkflowCompleted:
		snap.record.Success = true
		fallthrough
	case WorkflowFailed, WorkflowCancelled, WorkflowSkipped, WorkflowRemoved:
		snap.record.CompletedAt = ptrTime(now)
		if snap.record.StartedAt != nil {
			snap.record.Duration = now.Sub(*snap.record.StartedAt)
		}
		if status == WorkflowFailed {
			snap.record.Success = false
		}
	}
	snap.record.State = status

	update := WorkflowStatusUpdate{
		ID:     id,
		Status: status,
	}
	if snap.record.StartedAt != nil {
		update.StartedAt = snap.record.StartedAt
	}
	if snap.record.CompletedAt != nil {
		update.CompletedAt = snap.record.CompletedAt
	}
	duration := snap.record.Duration
	update.Duration = &duration
	success := snap.record.Success
	update.Success = &success
	errStr := snap.record.Error
	update.Error = &errStr

	if err := m.store.UpdateWorkflowStatus(ctx, update); err != nil {
		return err
	}
	m.notifyWorkflowStatus(ctx, workflowID, status)
	return nil
}

// StageRegistered persists metadata for a stage within a workflow.
func (m *StoreManager) StageRegistered(ctx context.Context, workflowID string, stage StageRecord) error {
	id := WorkflowID(workflowID)
	snap, err := m.getSnapshot(id)
	if err != nil {
		return err
	}

	stageCopy := cloneStageRecord(stage)
	if snap.record.Stages == nil {
		snap.record.Stages = make(map[string]*StageRecord)
	}
	snap.record.Stages[stageCopy.ID] = &stageCopy

	if err := m.store.RecordStage(ctx, id, stageCopy); err != nil {
		return err
	}
	m.notifyStageRegistered(ctx, string(id), stageCopy)
	return nil
}

// StageStatus updates the persisted stage status.
func (m *StoreManager) StageStatus(ctx context.Context, workflowID, stageID string, status WorkflowState) error {
	id := WorkflowID(workflowID)
	snap, err := m.getSnapshot(id)
	if err != nil {
		return err
	}
	stage := ensureStageRecord(&snap.record, stageID)
	var startedAt, completedAt *time.Time
	now := time.Now()

	if status == WorkflowRunning && stage.StartedAt == nil {
		stage.StartedAt = cloneTimePointer(&now)
		startedAt = stage.StartedAt
	}
	if isTerminalState(status) {
		if stage.StartedAt == nil {
			stage.StartedAt = cloneTimePointer(&now)
			startedAt = stage.StartedAt
		}
		if stage.CompletedAt == nil {
			stage.CompletedAt = cloneTimePointer(&now)
			completedAt = stage.CompletedAt
		}
	}
	stage.Status = status

	update := StageStatusUpdate{
		WorkflowID:  id,
		StageID:     stageID,
		Status:      status,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
	}

	if err := m.store.UpdateStageStatus(ctx, update); err != nil {
		return err
	}
	m.notifyStageStatus(ctx, workflowID, stageID, status)
	return nil
}

// ActionRegistered persists the action metadata for a stage.
func (m *StoreManager) ActionRegistered(ctx context.Context, workflowID, stageID string, action ActionRecord) error {
	id := WorkflowID(workflowID)
	snap, err := m.getSnapshot(id)
	if err != nil {
		return err
	}

	stage := ensureStageRecord(&snap.record, stageID)
	if stage.Actions == nil {
		stage.Actions = make(map[string]*ActionRecord)
	}
	actionCopy := cloneActionRecord(action)
	stage.Actions[actionCopy.Name] = &actionCopy

	if err := m.store.RecordAction(ctx, id, stageID, actionCopy); err != nil {
		return err
	}
	m.notifyActionRegistered(ctx, workflowID, stageID, actionCopy)
	return nil
}

// ActionStatus updates the persisted action status.
func (m *StoreManager) ActionStatus(ctx context.Context, workflowID, stageID, actionName string, status WorkflowState) error {
	id := WorkflowID(workflowID)
	snap, err := m.getSnapshot(id)
	if err != nil {
		return err
	}
	stage := ensureStageRecord(&snap.record, stageID)
	action := ensureActionRecord(stage, actionName)
	var startedAt, completedAt *time.Time
	now := time.Now()

	if status == WorkflowRunning && action.StartedAt == nil {
		action.StartedAt = cloneTimePointer(&now)
		startedAt = action.StartedAt
	}
	if isTerminalState(status) {
		if action.StartedAt == nil {
			action.StartedAt = cloneTimePointer(&now)
			startedAt = action.StartedAt
		}
		if action.CompletedAt == nil {
			action.CompletedAt = cloneTimePointer(&now)
			completedAt = action.CompletedAt
		}
	}
	action.Status = status

	update := ActionStatusUpdate{
		WorkflowID:  id,
		StageID:     stageID,
		ActionID:    actionName,
		Status:      status,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
	}
	if err := m.store.UpdateActionStatus(ctx, update); err != nil {
		return err
	}
	m.notifyActionStatus(ctx, workflowID, stageID, actionName, status)
	return nil
}

// ActionProgress forwards to the underlying store if it supports the
// ProgressRecorder interface and notifies observers.
func (m *StoreManager) ActionProgress(ctx context.Context, workflowID, stageID, actionName string, progress int, message string) error {
	if recorder, ok := m.store.(ProgressRecorder); ok {
		if err := recorder.ActionProgress(ctx, workflowID, stageID, actionName, progress, message); err != nil {
			return err
		}
	}
	m.notifyActionProgress(ctx, workflowID, stageID, actionName, progress, message)
	return nil
}

// ActionEvent records a custom action-level event for observers.
func (m *StoreManager) ActionEvent(ctx context.Context, workflowID, stageID, actionName, kind, message string, metadata map[string]any) error {
	m.notifyActionEvent(ctx, workflowID, stageID, actionName, kind, message, metadata)
	return nil
}

// ActionRemoved marks the action as removed and updates persistence.
func (m *StoreManager) ActionRemoved(ctx context.Context, workflowID, stageID, actionName, createdBy string) error {
	id := WorkflowID(workflowID)
	snap, err := m.getSnapshot(id)
	if err != nil {
		return err
	}
	stage := ensureStageRecord(&snap.record, stageID)
	action := ensureActionRecord(stage, actionName)
	action.Status = WorkflowRemoved
	action.CreatedBy = createdBy

	update := ActionStatusUpdate{
		WorkflowID: id,
		StageID:    stageID,
		ActionID:   actionName,
		Status:     WorkflowRemoved,
	}
	if err := m.store.UpdateActionStatus(ctx, update); err != nil {
		return err
	}
	m.notifyActionRemoved(ctx, workflowID, stageID, actionName, createdBy)
	return nil
}

// StageRemoved marks the stage as removed and updates persistence.
func (m *StoreManager) StageRemoved(ctx context.Context, workflowID, stageID, createdBy string) error {
	id := WorkflowID(workflowID)
	snap, err := m.getSnapshot(id)
	if err != nil {
		return err
	}
	stage := ensureStageRecord(&snap.record, stageID)
	stage.Status = WorkflowRemoved
	stage.CreatedBy = createdBy

	update := StageStatusUpdate{
		WorkflowID: id,
		StageID:    stageID,
		Status:     WorkflowRemoved,
	}
	if err := m.store.UpdateStageStatus(ctx, update); err != nil {
		return err
	}
	m.notifyStageRemoved(ctx, workflowID, stageID, createdBy)
	return nil
}

// StoreExecutionSummary persists the final execution summary and caches it for Waiters.
func (m *StoreManager) StoreExecutionSummary(ctx context.Context, workflowID string, report ExecutionReport) error {
	id := WorkflowID(workflowID)
	summary := resultSummaryFromReport(report)
	if err := m.store.StoreSummary(ctx, id, summary); err != nil {
		return err
	}
	m.mu.Lock()
	if snap, ok := m.workflows[id]; ok {
		copySummary := summary
		snap.summary = &copySummary
		if !report.StartedAt.IsZero() {
			started := report.StartedAt
			snap.record.StartedAt = &started
		}
		if !summary.CompletedAt.IsZero() {
			completed := summary.CompletedAt
			snap.record.CompletedAt = &completed
		}
		if report.Status != "" {
			snap.record.State = report.Status
		}
		snap.record.Success = summary.Success
		snap.record.Error = summary.Error
		snap.record.TerminationReason = summary.Reason
		snap.record.Duration = summary.Duration
		if report.WorkflowName != "" {
			snap.record.Name = report.WorkflowName
		}
		if report.WorkflowType != "" {
			snap.record.Type = report.WorkflowType
		}
		if len(report.WorkflowTags) > 0 {
			snap.record.Tags = append([]string(nil), report.WorkflowTags...)
		}
		for _, stageSummary := range report.Stages {
			if stageSummary.ID == "" {
				continue
			}
			stage := ensureStageRecord(&snap.record, stageSummary.ID)
			if stageSummary.Name != "" {
				stage.Name = stageSummary.Name
			}
			if stageSummary.Description != "" {
				stage.Description = stageSummary.Description
			}
			if len(stageSummary.Tags) > 0 {
				stage.Tags = append([]string(nil), stageSummary.Tags...)
			}
			stage.Dynamic = stageSummary.Dynamic
			if stageSummary.CreatedBy != "" {
				stage.CreatedBy = stageSummary.CreatedBy
			}
			if stageSummary.Status != "" {
				stage.Status = stageSummary.Status
			}
			for _, actionSummary := range stageSummary.Actions {
				action := ensureActionRecord(stage, actionSummary.Name)
				if actionSummary.Description != "" {
					action.Description = actionSummary.Description
				}
				if len(actionSummary.Tags) > 0 {
					action.Tags = append([]string(nil), actionSummary.Tags...)
				}
				action.Dynamic = actionSummary.Dynamic
				if actionSummary.CreatedBy != "" {
					action.CreatedBy = actionSummary.CreatedBy
				}
				if actionSummary.Status != "" {
					action.Status = actionSummary.Status
				}
			}
		}
		finalState := snap.record.State
		reason := summary.Reason
		if reason == "" {
			reason = TerminationReasonUnknown
		}
		m.mu.Unlock()
		if err := m.store.UpdateWorkflowStatus(ctx, WorkflowStatusUpdate{
			ID:     id,
			Status: finalState,
			Reason: &reason,
		}); err != nil {
			return err
		}
		m.notifyWorkflowStatus(ctx, string(id), finalState)
	} else {
		m.mu.Unlock()
	}
	m.notifyExecutionSummary(ctx, workflowID, summary)
	return nil
}

func (m *StoreManager) getSnapshot(id WorkflowID) (*workflowSnapshot, error) {
	m.mu.RLock()
	snap, ok := m.workflows[id]
	m.mu.RUnlock()
	if ok {
		return snap, nil
	}
	return nil, errors.New("state: workflow not registered")
}

func ensureStageRecord(rec *WorkflowRecord, stageID string) *StageRecord {
	if rec.Stages == nil {
		rec.Stages = make(map[string]*StageRecord)
	}
	stage, ok := rec.Stages[stageID]
	if !ok {
		stage = &StageRecord{ID: stageID}
		rec.Stages[stageID] = stage
	}
	return stage
}

func ensureActionRecord(stage *StageRecord, actionName string) *ActionRecord {
	if stage.Actions == nil {
		stage.Actions = make(map[string]*ActionRecord)
	}
	action, ok := stage.Actions[actionName]
	if !ok {
		action = &ActionRecord{Name: actionName}
		stage.Actions[actionName] = action
	}
	return action
}

func cloneWorkflowRecord(rec WorkflowRecord) WorkflowRecord {
	out := rec
	out.Tags = append([]string(nil), rec.Tags...)
	out.Metadata = maps.Clone(rec.Metadata)
	if rec.Definition.Metadata != nil {
		out.Definition.Metadata = maps.Clone(rec.Definition.Metadata)
	}
	if rec.Definition.Payload != nil {
		out.Definition.Payload = maps.Clone(rec.Definition.Payload)
	}
	out.Definition.Tags = append([]string(nil), rec.Definition.Tags...)
	if len(rec.Stages) > 0 {
		out.Stages = make(map[string]*StageRecord, len(rec.Stages))
		for id, stage := range rec.Stages {
			if stage == nil {
				continue
			}
			clone := cloneStageRecord(*stage)
			out.Stages[id] = &clone
		}
	} else {
		out.Stages = make(map[string]*StageRecord)
	}
	return out
}

func cloneStageRecord(rec StageRecord) StageRecord {
	out := rec
	out.Tags = append([]string(nil), rec.Tags...)
	out.StartedAt = cloneTimePointer(rec.StartedAt)
	out.CompletedAt = cloneTimePointer(rec.CompletedAt)
	if len(rec.Actions) > 0 {
		out.Actions = make(map[string]*ActionRecord, len(rec.Actions))
		for id, act := range rec.Actions {
			if act == nil {
				continue
			}
			clone := cloneActionRecord(*act)
			out.Actions[id] = &clone
		}
	} else {
		out.Actions = make(map[string]*ActionRecord)
	}
	return out
}

func cloneActionRecord(rec ActionRecord) ActionRecord {
	out := rec
	out.Ref = rec.Ref
	out.Tags = append([]string(nil), rec.Tags...)
	out.StartedAt = cloneTimePointer(rec.StartedAt)
	out.CompletedAt = cloneTimePointer(rec.CompletedAt)
	return out
}

func resultSummaryFromReport(report ExecutionReport) ResultSummary {
	reason := report.Reason
	if reason == "" {
		reason = TerminationReasonUnknown
	}
	return ResultSummary{
		Success:         report.Success,
		Error:           report.ErrorMessage,
		Attempt:         report.Attempt,
		Output:          maps.Clone(report.FinalStore),
		Duration:        report.Duration,
		CompletedAt:     report.CompletedAt,
		DisabledStages:  maps.Clone(report.DisabledStages),
		DisabledActions: maps.Clone(report.DisabledActions),
		RemovedStages:   maps.Clone(report.RemovedStages),
		RemovedActions:  maps.Clone(report.RemovedActions),
		Reason:          reason,
	}
}

func ptrTime(t time.Time) *time.Time { return &t }

func (m *StoreManager) notifyWorkflowRegistered(ctx context.Context, wf WorkflowRecord) {
	if len(m.observers) == 0 {
		return
	}
	clone := cloneWorkflowRecord(wf)
	for _, obs := range m.observers {
		obs.WorkflowRegistered(ctx, clone)
	}
}

func (m *StoreManager) notifyWorkflowStatus(ctx context.Context, workflowID string, status WorkflowState) {
	if len(m.observers) == 0 {
		return
	}
	for _, obs := range m.observers {
		obs.WorkflowStatus(ctx, workflowID, status)
	}
}

func (m *StoreManager) notifyStageRegistered(ctx context.Context, workflowID string, stage StageRecord) {
	if len(m.observers) == 0 {
		return
	}
	clone := cloneStageRecord(stage)
	for _, obs := range m.observers {
		obs.StageRegistered(ctx, workflowID, clone)
	}
}

func (m *StoreManager) notifyStageStatus(ctx context.Context, workflowID, stageID string, status WorkflowState) {
	if len(m.observers) == 0 {
		return
	}
	for _, obs := range m.observers {
		obs.StageStatus(ctx, workflowID, stageID, status)
	}
}

func (m *StoreManager) notifyActionRegistered(ctx context.Context, workflowID, stageID string, action ActionRecord) {
	if len(m.observers) == 0 {
		return
	}
	clone := cloneActionRecord(action)
	for _, obs := range m.observers {
		obs.ActionRegistered(ctx, workflowID, stageID, clone)
	}
}

func (m *StoreManager) notifyActionStatus(ctx context.Context, workflowID, stageID, actionID string, status WorkflowState) {
	if len(m.observers) == 0 {
		return
	}
	for _, obs := range m.observers {
		obs.ActionStatus(ctx, workflowID, stageID, actionID, status)
	}
}

func (m *StoreManager) notifyActionProgress(ctx context.Context, workflowID, stageID, actionID string, progress int, message string) {
	if len(m.observers) == 0 {
		return
	}
	for _, obs := range m.observers {
		obs.ActionProgress(ctx, workflowID, stageID, actionID, progress, message)
	}
}

func isTerminalState(state WorkflowState) bool {
	switch state {
	case WorkflowCompleted, WorkflowFailed, WorkflowCancelled, WorkflowSkipped, WorkflowRemoved:
		return true
	default:
		return false
	}
}

func (m *StoreManager) notifyActionEvent(ctx context.Context, workflowID, stageID, actionID, kind, message string, metadata map[string]any) {
	if len(m.observers) == 0 {
		return
	}
	var base map[string]any
	if len(metadata) > 0 {
		base = make(map[string]any, len(metadata))
		for k, v := range metadata {
			base[k] = v
		}
	}
	for _, obs := range m.observers {
		var meta map[string]any
		if base != nil {
			meta = make(map[string]any, len(base))
			for k, v := range base {
				meta[k] = v
			}
		}
		obs.ActionEvent(ctx, workflowID, stageID, actionID, kind, message, meta)
	}
}

func (m *StoreManager) notifyActionRemoved(ctx context.Context, workflowID, stageID, actionID, createdBy string) {
	if len(m.observers) == 0 {
		return
	}
	for _, obs := range m.observers {
		obs.ActionRemoved(ctx, workflowID, stageID, actionID, createdBy)
	}
}

func (m *StoreManager) notifyStageRemoved(ctx context.Context, workflowID, stageID, createdBy string) {
	if len(m.observers) == 0 {
		return
	}
	for _, obs := range m.observers {
		obs.StageRemoved(ctx, workflowID, stageID, createdBy)
	}
}

func (m *StoreManager) notifyExecutionSummary(ctx context.Context, workflowID string, summary ResultSummary) {
	if len(m.observers) == 0 {
		return
	}
	clone := summary
	clone.Output = maps.Clone(summary.Output)
	clone.DisabledStages = maps.Clone(summary.DisabledStages)
	clone.DisabledActions = maps.Clone(summary.DisabledActions)
	clone.RemovedStages = maps.Clone(summary.RemovedStages)
	clone.RemovedActions = maps.Clone(summary.RemovedActions)
	for _, obs := range m.observers {
		obs.ExecutionSummary(ctx, workflowID, clone)
	}
}
