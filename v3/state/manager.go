package state

import (
	"context"
	"errors"
	"maps"
	"time"

	"github.com/davidroman0O/gostage/v3/workflow"
	deadlock "github.com/sasha-s/go-deadlock"
)

// StoreManager bridges the legacy state.Manager contract used by the runner
// with the new queue/store implementation backed by sqlc. It keeps an
// in-memory shadow of workflow metadata so incremental status updates can be
// persisted without the runner needing to resend the full record each time.
type StoreManager struct {
	store Store

	mu        deadlock.RWMutex
	workflows map[WorkflowID]*workflowSnapshot

	now func() time.Time
}

type workflowSnapshot struct {
	record WorkflowRecord
}

// NewStoreManager constructs a Manager backed by the provided Store. The
// returned Manager is safe for concurrent use.
func NewStoreManager(store Store) (*StoreManager, error) {
	if store == nil {
		return nil, errors.New("state: store manager requires Store")
	}
	return &StoreManager{
		store:     store,
		workflows: make(map[WorkflowID]*workflowSnapshot),
		now:       time.Now,
	}, nil
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

	return m.store.RecordWorkflow(ctx, clone)
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

	return m.store.RecordWorkflow(ctx, snap.record)
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

	wfStage := workflow.Stage{
		ID:          stageCopy.ID,
		Name:        stageCopy.Name,
		Description: stageCopy.Description,
		Tags:        append([]string(nil), stageCopy.Tags...),
		Dynamic:     stageCopy.Dynamic,
		CreatedBy:   stageCopy.CreatedBy,
	}
	return m.store.RecordStage(ctx, id, wfStage, stageCopy.Dynamic, stageCopy.CreatedBy, stageCopy.Status)
}

// StageStatus updates the persisted stage status.
func (m *StoreManager) StageStatus(ctx context.Context, workflowID, stageID string, status WorkflowState) error {
	id := WorkflowID(workflowID)
	snap, err := m.getSnapshot(id)
	if err != nil {
		return err
	}
	stage := ensureStageRecord(&snap.record, stageID)
	stage.Status = status

	wfStage := workflow.Stage{
		ID:          stage.ID,
		Name:        stage.Name,
		Description: stage.Description,
		Tags:        append([]string(nil), stage.Tags...),
		Dynamic:     stage.Dynamic,
		CreatedBy:   stage.CreatedBy,
	}
	return m.store.RecordStage(ctx, id, wfStage, stage.Dynamic, stage.CreatedBy, status)
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

	wfAction := workflow.Action{
		ID:          actionCopy.Name,
		Description: actionCopy.Description,
		Tags:        append([]string(nil), actionCopy.Tags...),
		Dynamic:     actionCopy.Dynamic,
		CreatedBy:   actionCopy.CreatedBy,
	}
	return m.store.RecordAction(ctx, id, stageID, wfAction, actionCopy.Dynamic, actionCopy.CreatedBy, actionCopy.Status)
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
	action.Status = status

	wfAction := workflow.Action{
		ID:          action.Name,
		Description: action.Description,
		Tags:        append([]string(nil), action.Tags...),
		Dynamic:     action.Dynamic,
		CreatedBy:   action.CreatedBy,
	}
	return m.store.RecordAction(ctx, id, stageID, wfAction, action.Dynamic, action.CreatedBy, status)
}

// ActionProgress is forwarded to the store via ExecutionSummary; no-op here.
func (m *StoreManager) ActionProgress(context.Context, string, string, string, int, string) error {
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

	wfAction := workflow.Action{
		ID:          action.Name,
		Description: action.Description,
		Tags:        append([]string(nil), action.Tags...),
		Dynamic:     action.Dynamic,
		CreatedBy:   action.CreatedBy,
	}
	return m.store.RecordAction(ctx, id, stageID, wfAction, action.Dynamic, action.CreatedBy, WorkflowRemoved)
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

	wfStage := workflow.Stage{
		ID:          stage.ID,
		Name:        stage.Name,
		Description: stage.Description,
		Tags:        append([]string(nil), stage.Tags...),
		Dynamic:     stage.Dynamic,
		CreatedBy:   stage.CreatedBy,
	}
	return m.store.RecordStage(ctx, id, wfStage, stage.Dynamic, stage.CreatedBy, WorkflowRemoved)
}

// StoreExecutionSummary persists the final execution summary and caches it for Waiters.
func (m *StoreManager) StoreExecutionSummary(ctx context.Context, workflowID string, report ExecutionReport) error {
	id := WorkflowID(workflowID)
	summary := resultSummaryFromReport(report)
	return m.store.StoreSummary(ctx, id, summary)
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
	out.Tags = append([]string(nil), rec.Tags...)
	return out
}

func resultSummaryFromReport(report ExecutionReport) ResultSummary {
	return ResultSummary{
		Success:         report.Success,
		Error:           report.ErrorMessage,
		Attempt:         0,
		Output:          maps.Clone(report.FinalStore),
		Duration:        report.Duration,
		CompletedAt:     report.CompletedAt,
		DisabledStages:  maps.Clone(report.DisabledStages),
		DisabledActions: maps.Clone(report.DisabledActions),
		RemovedStages:   maps.Clone(report.RemovedStages),
		RemovedActions:  maps.Clone(report.RemovedActions),
	}
}

func ptrTime(t time.Time) *time.Time { return &t }
