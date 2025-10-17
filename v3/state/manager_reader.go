package state

import (
	"context"
	"errors"
	"sort"
	"time"
)

// managerStateReader exposes StoreManager snapshots via the StateReader interface.
type managerStateReader struct {
	manager *StoreManager
}

// NewManagerStateReader builds a StateReader backed by the StoreManager's cached
// snapshots. It returns nil when manager is nil.
func NewManagerStateReader(manager *StoreManager) StateReader {
	if manager == nil {
		return nil
	}
	return &managerStateReader{manager: manager}
}

func (r *managerStateReader) WorkflowSummary(ctx context.Context, id WorkflowID) (WorkflowSummary, error) {
	_ = ctx
	r.manager.mu.RLock()
	snap, ok := r.manager.workflows[id]
	if !ok {
		r.manager.mu.RUnlock()
		return WorkflowSummary{}, errors.New("state: workflow not found")
	}
	record := cloneWorkflowRecord(snap.record)
	if snap.summary != nil {
		summary := *snap.summary
		record.Success = summary.Success
		record.Error = summary.Error
		record.Duration = summary.Duration
		if !summary.CompletedAt.IsZero() {
			completed := summary.CompletedAt
			record.CompletedAt = &completed
		}
	}
	r.manager.mu.RUnlock()

	return WorkflowSummary{WorkflowRecord: record}, nil
}

func (r *managerStateReader) ListWorkflows(ctx context.Context, filter StateFilter) ([]WorkflowSummary, error) {
	_ = ctx
	r.manager.mu.RLock()
	ids := make([]WorkflowID, 0, len(r.manager.workflows))
	for id := range r.manager.workflows {
		ids = append(ids, id)
	}
	// Sort by creation timestamp then ID for stability.
	sort.Slice(ids, func(i, j int) bool {
		a := r.manager.workflows[ids[i]].record.CreatedAt
		b := r.manager.workflows[ids[j]].record.CreatedAt
		if a.Equal(b) {
			return ids[i] < ids[j]
		}
		return a.Before(b)
	})
	records := make([]WorkflowRecord, 0, len(ids))
	for _, id := range ids {
		records = append(records, cloneWorkflowRecord(r.manager.workflows[id].record))
	}
	r.manager.mu.RUnlock()

	matchesState := func(state WorkflowState) bool {
		if len(filter.States) == 0 {
			return true
		}
		for _, candidate := range filter.States {
			if candidate == state {
				return true
			}
		}
		return false
	}

	matchesTags := func(tags []string) bool {
		if len(filter.Tags) == 0 {
			return true
		}
		set := make(map[string]struct{}, len(tags))
		for _, tag := range tags {
			set[tag] = struct{}{}
		}
		for _, tag := range filter.Tags {
			if _, ok := set[tag]; !ok {
				return false
			}
		}
		return true
	}

	matchesType := func(workflowType string) bool {
		if len(filter.Type) == 0 {
			return true
		}
		for _, candidate := range filter.Type {
			if candidate == workflowType {
				return true
			}
		}
		return false
	}

	matchesWindow := func(created time.Time) bool {
		if filter.From != nil && created.Before(*filter.From) {
			return false
		}
		if filter.To != nil && created.After(*filter.To) {
			return false
		}
		return true
	}

	summaries := make([]WorkflowSummary, 0, len(records))
	for _, record := range records {
		if !matchesState(record.State) || !matchesTags(record.Tags) || !matchesType(record.Type) || !matchesWindow(record.CreatedAt) {
			continue
		}
		summary, err := r.WorkflowSummary(ctx, record.ID)
		if err != nil {
			continue
		}
		summaries = append(summaries, summary)
	}

	start := filter.Offset
	if start > len(summaries) {
		return []WorkflowSummary{}, nil
	}
	summaries = summaries[start:]
	if filter.Limit > 0 && filter.Limit < len(summaries) {
		summaries = summaries[:filter.Limit]
	}
	return summaries, nil
}

func (r *managerStateReader) ActionHistory(ctx context.Context, id WorkflowID) ([]ActionHistoryRecord, error) {
	_ = ctx
	r.manager.mu.RLock()
	snap, ok := r.manager.workflows[id]
	if !ok {
		r.manager.mu.RUnlock()
		return nil, errors.New("state: workflow not found")
	}
	record := cloneWorkflowRecord(snap.record)
	r.manager.mu.RUnlock()

	stageIDs := make([]string, 0, len(record.Stages))
	for stageID := range record.Stages {
		stageIDs = append(stageIDs, stageID)
	}
	sort.Strings(stageIDs)

	history := make([]ActionHistoryRecord, 0)
	for _, stageID := range stageIDs {
		stage := record.Stages[stageID]
		if stage == nil {
			continue
		}
		actionIDs := make([]string, 0, len(stage.Actions))
		for actionID := range stage.Actions {
			actionIDs = append(actionIDs, actionID)
		}
		sort.Strings(actionIDs)
		for _, actionID := range actionIDs {
			action := stage.Actions[actionID]
			if action == nil {
				continue
			}
			record := ActionHistoryRecord{
				ActionID:    actionID,
				StageID:     stageID,
				Ref:         action.Ref,
				Description: action.Description,
				Tags:        append([]string(nil), action.Tags...),
				Dynamic:     action.Dynamic,
				CreatedBy:   action.CreatedBy,
				State:       action.Status,
			}
			history = append(history, record)
		}
	}
	return history, nil
}
