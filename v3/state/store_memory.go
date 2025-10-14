package state

import (
	"context"

	"github.com/davidroman0O/gostage/v3/workflow"
	deadlock "github.com/sasha-s/go-deadlock"
)

// MemoryStore provides an in-memory Store suitable for tests.
type MemoryStore struct {
	mu        deadlock.Mutex
	workflows map[WorkflowID]WorkflowRecord
	stages    map[WorkflowID]map[string]workflow.Stage
	actions   map[WorkflowID]map[string]workflow.Action
	summaries map[WorkflowID]ResultSummary
	waiters   map[WorkflowID][]chan ResultSummary
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		workflows: make(map[WorkflowID]WorkflowRecord),
		stages:    make(map[WorkflowID]map[string]workflow.Stage),
		actions:   make(map[WorkflowID]map[string]workflow.Action),
		summaries: make(map[WorkflowID]ResultSummary),
		waiters:   make(map[WorkflowID][]chan ResultSummary),
	}
}

func (s *MemoryStore) RecordWorkflow(ctx context.Context, rec WorkflowRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workflows[rec.ID] = rec
	return nil
}

func (s *MemoryStore) RecordStage(ctx context.Context, workflowID WorkflowID, stage workflow.Stage, dynamic bool, createdBy string, state WorkflowState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stages[workflowID] == nil {
		s.stages[workflowID] = make(map[string]workflow.Stage)
	}
	s.stages[workflowID][stage.ID] = stage
	return nil
}

func (s *MemoryStore) RecordAction(ctx context.Context, workflowID WorkflowID, stageID string, action workflow.Action, dynamic bool, createdBy string, state WorkflowState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.actions[workflowID] == nil {
		s.actions[workflowID] = make(map[string]workflow.Action)
	}
	s.actions[workflowID][action.ID] = action
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
