package state

import (
	"context"
	"time"

	deadlock "github.com/sasha-s/go-deadlock"
)

// MemoryStore provides an in-memory Store suitable for tests.
type MemoryStore struct {
	mu        deadlock.Mutex
	workflows map[WorkflowID]WorkflowRecord
	stages    map[WorkflowID]map[string]*StageRecord
	actions   map[WorkflowID]map[string]map[string]*ActionRecord
	summaries map[WorkflowID]ResultSummary
	waiters   map[WorkflowID][]chan ResultSummary
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		workflows: make(map[WorkflowID]WorkflowRecord),
		stages:    make(map[WorkflowID]map[string]*StageRecord),
		actions:   make(map[WorkflowID]map[string]map[string]*ActionRecord),
		summaries: make(map[WorkflowID]ResultSummary),
		waiters:   make(map[WorkflowID][]chan ResultSummary),
	}
}

func (s *MemoryStore) RecordWorkflow(ctx context.Context, rec WorkflowRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workflows[rec.ID] = cloneWorkflowRecord(rec)
	return nil
}

func (s *MemoryStore) UpdateWorkflowStatus(ctx context.Context, update WorkflowStatusUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.workflows[update.ID]
	if rec.ID == "" {
		rec.ID = update.ID
	}
	rec.State = update.Status
	if update.StartedAt != nil {
		rec.StartedAt = cloneTimePtr(update.StartedAt)
	}
	if update.CompletedAt != nil {
		rec.CompletedAt = cloneTimePtr(update.CompletedAt)
	}
	if update.Duration != nil {
		rec.Duration = *update.Duration
	}
	if update.Success != nil {
		rec.Success = *update.Success
	}
	if update.Error != nil {
		rec.Error = *update.Error
	}
	s.workflows[update.ID] = rec
	return nil
}

func (s *MemoryStore) RecordStage(ctx context.Context, workflowID WorkflowID, stage StageRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stages[workflowID] == nil {
		s.stages[workflowID] = make(map[string]*StageRecord)
	}
	stageClone := cloneStageRecord(stage)
	stageClone.Actions = make(map[string]*ActionRecord)
	s.stages[workflowID][stage.ID] = &stageClone
	return nil
}

func (s *MemoryStore) UpdateStageStatus(ctx context.Context, update StageStatusUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stages[update.WorkflowID] == nil {
		s.stages[update.WorkflowID] = make(map[string]*StageRecord)
	}
	var stageClone StageRecord
	if existing := s.stages[update.WorkflowID][update.StageID]; existing != nil {
		stageClone = cloneStageRecord(*existing)
	} else {
		stageClone = StageRecord{ID: update.StageID, Actions: make(map[string]*ActionRecord)}
	}
	stageClone.Status = update.Status
	s.stages[update.WorkflowID][update.StageID] = &stageClone
	return nil
}

func (s *MemoryStore) RecordAction(ctx context.Context, workflowID WorkflowID, stageID string, action ActionRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.actions[workflowID] == nil {
		s.actions[workflowID] = make(map[string]map[string]*ActionRecord)
	}
	if s.actions[workflowID][stageID] == nil {
		s.actions[workflowID][stageID] = make(map[string]*ActionRecord)
	}
	actionClone := cloneActionRecord(action)
	s.actions[workflowID][stageID][action.Name] = &actionClone
	if s.stages[workflowID] != nil {
		if stage := s.stages[workflowID][stageID]; stage != nil {
			if stage.Actions == nil {
				stage.Actions = make(map[string]*ActionRecord)
			}
			stage.Actions[action.Name] = &actionClone
		}
	}
	return nil
}

func (s *MemoryStore) UpdateActionStatus(ctx context.Context, update ActionStatusUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.actions[update.WorkflowID] == nil {
		s.actions[update.WorkflowID] = make(map[string]map[string]*ActionRecord)
	}
	if s.actions[update.WorkflowID][update.StageID] == nil {
		s.actions[update.WorkflowID][update.StageID] = make(map[string]*ActionRecord)
	}
	var actionClone ActionRecord
	if existing := s.actions[update.WorkflowID][update.StageID][update.ActionID]; existing != nil {
		actionClone = cloneActionRecord(*existing)
	} else {
		actionClone = ActionRecord{Name: update.ActionID}
	}
	actionClone.Status = update.Status
	s.actions[update.WorkflowID][update.StageID][update.ActionID] = &actionClone
	if s.stages[update.WorkflowID] != nil {
		if stage := s.stages[update.WorkflowID][update.StageID]; stage != nil {
			if stage.Actions == nil {
				stage.Actions = make(map[string]*ActionRecord)
			}
			stage.Actions[update.ActionID] = &actionClone
		}
	}
	return nil
}

func (s *MemoryStore) StoreSummary(ctx context.Context, id WorkflowID, summary ResultSummary) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.summaries[id] = summary
	for _, ch := range s.waiters[id] {
		ch <- summary
	}
	delete(s.waiters, id)
	return nil
}

func (s *MemoryStore) WaitResult(ctx context.Context, id WorkflowID) (ResultSummary, error) {
	s.mu.Lock()
	if summary, ok := s.summaries[id]; ok {
		s.mu.Unlock()
		return summary, nil
	}
	ch := make(chan ResultSummary, 1)
	s.waiters[id] = append(s.waiters[id], ch)
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		s.mu.Lock()
		defer s.mu.Unlock()
		chs := s.waiters[id]
		for i, waiter := range chs {
			if waiter == ch {
				s.waiters[id] = append(chs[:i], chs[i+1:]...)
				break
			}
		}
		return ResultSummary{}, ctx.Err()
	case res := <-ch:
		return res, nil
	}
}

func (s *MemoryStore) Close() error {
	return nil
}

func cloneTimePtr(ts *time.Time) *time.Time {
	if ts == nil {
		return nil
	}
	v := *ts
	return &v
}
