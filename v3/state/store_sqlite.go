package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/davidroman0O/gostage/v3/state/sqlc"
	"github.com/davidroman0O/gostage/v3/workflow"
	deadlock "github.com/sasha-s/go-deadlock"
)

// SQLiteStore persists workflow state using sqlite/sqlc.
type SQLiteStore struct {
	db      *sql.DB
	queries *sqlc.Queries

	mu        deadlock.Mutex
	waiters   map[WorkflowID][]chan ResultSummary
	summaries map[WorkflowID]ResultSummary
}

func NewSQLiteStore(db *sql.DB) (*SQLiteStore, error) {
	if db == nil {
		return nil, errors.New("state: store db is nil")
	}
	return &SQLiteStore{
		db:        db,
		queries:   sqlc.New(db),
		waiters:   make(map[WorkflowID][]chan ResultSummary),
		summaries: make(map[WorkflowID]ResultSummary),
	}, nil
}

func (s *SQLiteStore) RecordWorkflow(ctx context.Context, rec WorkflowRecord) error {
	tags, err := json.Marshal(rec.Tags)
	if err != nil {
		return err
	}
	metadata, err := json.Marshal(rec.Metadata)
	if err != nil {
		return err
	}
	params := sqlc.UpsertWorkflowRunParams{
		ID:          string(rec.ID),
		Name:        toNullString(rec.Name),
		Description: toNullString(rec.Description),
		Type:        toNullString(rec.Type),
		Tags:        tags,
		Metadata:    metadata,
		Column7:     nullableTimeValue(rec.CreatedAt),
		State:       string(rec.State),
		Success:     boolToInt64(rec.Success),
		Error:       toNullString(rec.Error),
		StartedAt:   toNullTime(rec.StartedAt),
		CompletedAt: toNullTime(rec.CompletedAt),
		Duration:    toNullDuration(rec.Duration),
	}
	return s.queries.UpsertWorkflowRun(ctx, params)
}

func (s *SQLiteStore) RecordStage(ctx context.Context, workflowID WorkflowID, stage workflow.Stage, dynamic bool, createdBy string, state WorkflowState) error {
	tags, err := json.Marshal(stage.Tags)
	if err != nil {
		return err
	}
	params := sqlc.InsertStageRunParams{
		WorkflowID:  string(workflowID),
		StageID:     stage.ID,
		Name:        toNullString(stage.Name),
		Tags:        tags,
		Dynamic:     boolToInt64(dynamic),
		CreatedBy:   toNullString(createdBy),
		State:       string(state),
		StartedAt:   sql.NullTime{},
		CompletedAt: sql.NullTime{},
	}
	return s.queries.InsertStageRun(ctx, params)
}

func (s *SQLiteStore) RecordAction(ctx context.Context, workflowID WorkflowID, stageID string, action workflow.Action, dynamic bool, createdBy string, state WorkflowState) error {
	tags, err := json.Marshal(action.Tags)
	if err != nil {
		return err
	}
	params := sqlc.InsertActionRunParams{
		WorkflowID:  string(workflowID),
		StageID:     stageID,
		ActionID:    action.ID,
		Ref:         toNullString(action.Ref),
		Tags:        tags,
		Dynamic:     boolToInt64(dynamic),
		CreatedBy:   toNullString(createdBy),
		State:       string(state),
		StartedAt:   sql.NullTime{},
		CompletedAt: sql.NullTime{},
	}
	return s.queries.InsertActionRun(ctx, params)
}

func (s *SQLiteStore) StoreSummary(ctx context.Context, id WorkflowID, summary ResultSummary) error {
	storeBytes, err := json.Marshal(summary.Output)
	if err != nil {
		return err
	}
	disabledStages, err := json.Marshal(summary.DisabledStages)
	if err != nil {
		return err
	}
	disabledActions, err := json.Marshal(summary.DisabledActions)
	if err != nil {
		return err
	}
	removedStages, err := json.Marshal(summary.RemovedStages)
	if err != nil {
		return err
	}
	removedActions, err := json.Marshal(summary.RemovedActions)
	if err != nil {
		return err
	}
	if err := s.queries.UpsertExecutionSummary(ctx, sqlc.UpsertExecutionSummaryParams{
		WorkflowID:      string(id),
		FinalStore:      storeBytes,
		DisabledStages:  disabledStages,
		DisabledActions: disabledActions,
		RemovedStages:   removedStages,
		RemovedActions:  removedActions,
	}); err != nil {
		return err
	}
	s.mu.Lock()
	s.summaries[id] = summary
	waiters := s.waiters[id]
	delete(s.waiters, id)
	s.mu.Unlock()
	for _, waiter := range waiters {
		waiter <- summary
	}
	return nil
}

func (s *SQLiteStore) WaitResult(ctx context.Context, id WorkflowID) (ResultSummary, error) {
	s.mu.Lock()
	if cached, ok := s.summaries[id]; ok {
		s.mu.Unlock()
		return cached, nil
	}
	s.mu.Unlock()

	summary, err := s.fetchSummary(ctx, id)
	if err == nil {
		return summary, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ResultSummary{}, err
	}
	ch := make(chan ResultSummary, 1)
	s.mu.Lock()
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

func (s *SQLiteStore) Close() error {
	return nil
}

func (s *SQLiteStore) fetchSummary(ctx context.Context, id WorkflowID) (ResultSummary, error) {
	rec, err := s.queries.GetExecutionSummary(ctx, string(id))
	if err != nil {
		return ResultSummary{}, err
	}
	var storeMap map[string]any
	if len(rec.FinalStore) > 0 {
		_ = json.Unmarshal(rec.FinalStore, &storeMap)
	}
	var disabledStages map[string]bool
	if len(rec.DisabledStages) > 0 {
		_ = json.Unmarshal(rec.DisabledStages, &disabledStages)
	}
	var disabledActions map[string]bool
	if len(rec.DisabledActions) > 0 {
		_ = json.Unmarshal(rec.DisabledActions, &disabledActions)
	}
	var removedStages map[string]string
	if len(rec.RemovedStages) > 0 {
		_ = json.Unmarshal(rec.RemovedStages, &removedStages)
	}
	var removedActions map[string]string
	if len(rec.RemovedActions) > 0 {
		_ = json.Unmarshal(rec.RemovedActions, &removedActions)
	}
	return ResultSummary{
		Output:          storeMap,
		DisabledStages:  disabledStages,
		DisabledActions: disabledActions,
		RemovedStages:   removedStages,
		RemovedActions:  removedActions,
	}, nil
}

func (s *SQLiteStore) notifyWaiters(id WorkflowID, summary ResultSummary) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, waiter := range s.waiters[id] {
		waiter <- summary
	}
	delete(s.waiters, id)
}
