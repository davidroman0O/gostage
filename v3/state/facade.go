package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/davidroman0O/gostage/v3/state/sqlc"
)

type StateReader interface {
	WorkflowSummary(ctx context.Context, id WorkflowID) (WorkflowSummary, error)
	ListWorkflows(ctx context.Context, filter StateFilter) ([]WorkflowSummary, error)
	ActionHistory(ctx context.Context, id WorkflowID) ([]ActionHistoryRecord, error)
}

type SQLiteStateReader struct {
	queries *sqlc.Queries
}

func NewSQLiteStateReader(db *sql.DB) (*SQLiteStateReader, error) {
	if db == nil {
		return nil, errors.New("state: reader db is nil")
	}
	return &SQLiteStateReader{queries: sqlc.New(db)}, nil
}

func (r *SQLiteStateReader) WorkflowSummary(ctx context.Context, id WorkflowID) (WorkflowSummary, error) {
	row, err := r.queries.GetWorkflowSummary(ctx, string(id))
	if err != nil {
		return WorkflowSummary{}, err
	}
	return convertWorkflowRow(row)
}

func (r *SQLiteStateReader) ListWorkflows(ctx context.Context, filter StateFilter) ([]WorkflowSummary, error) {
	rows, err := r.queries.ListAllWorkflows(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]WorkflowSummary, 0, len(rows))
	for _, row := range rows {
		summary, err := convertWorkflowRow(row)
		if err != nil {
			return nil, err
		}
		if !matchesFilter(summary, filter) {
			continue
		}
		out = append(out, summary)
	}
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (r *SQLiteStateReader) ActionHistory(ctx context.Context, id WorkflowID) ([]ActionHistoryRecord, error) {
	rows, err := r.queries.ListActionsByWorkflow(ctx, string(id))
	if err != nil {
		return nil, err
	}
	history := make([]ActionHistoryRecord, 0, len(rows))
	for _, row := range rows {
		var tags []string
		if len(row.Tags) > 0 {
			_ = json.Unmarshal(row.Tags, &tags)
		}
		record := ActionHistoryRecord{
			ActionID:  row.ActionID,
			StageID:   row.StageID,
			Ref:       row.Ref.String,
			Tags:      tags,
			Dynamic:   row.Dynamic == 1,
			CreatedBy: row.CreatedBy.String,
			State:     WorkflowState(row.State),
		}
		if row.StartedAt.Valid {
			t := row.StartedAt.Time
			record.StartedAt = &t
		}
		if row.CompletedAt.Valid {
			t := row.CompletedAt.Time
			record.CompletedAt = &t
		}
		history = append(history, record)
	}
	return history, nil
}

func convertWorkflowRow(row sqlc.WorkflowRun) (WorkflowSummary, error) {
	var tags []string
	if len(row.Tags) > 0 {
		_ = json.Unmarshal(row.Tags, &tags)
	}
	metadata := make(map[string]any)
	if len(row.Metadata) > 0 {
		_ = json.Unmarshal(row.Metadata, &metadata)
	}
	var startedAt *time.Time
	if row.StartedAt.Valid {
		t := row.StartedAt.Time
		startedAt = &t
	}
	var completedAt *time.Time
	if row.CompletedAt.Valid {
		t := row.CompletedAt.Time
		completedAt = &t
	}
	duration := time.Duration(0)
	if row.Duration.Valid {
		duration = time.Duration(row.Duration.Int64)
	}
	summary := WorkflowSummary{
		WorkflowRecord: WorkflowRecord{
			ID:          WorkflowID(row.ID),
			Name:        row.Name.String,
			Description: row.Description.String,
			Type:        row.Type.String,
			Tags:        tags,
			Metadata:    metadata,
			CreatedAt:   row.CreatedAt,
			State:       WorkflowState(row.State),
			StartedAt:   startedAt,
			CompletedAt: completedAt,
			Duration:    duration,
			Success:     row.Success == 1,
			Error:       row.Error.String,
		},
	}
	return summary, nil
}

func matchesFilter(summary WorkflowSummary, filter StateFilter) bool {
	if len(filter.States) > 0 {
		match := false
		for _, st := range filter.States {
			if summary.State == st {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	if len(filter.Tags) > 0 {
		if !hasAll(summary.Tags, filter.Tags) {
			return false
		}
	}
	if len(filter.Type) > 0 {
		match := false
		for _, t := range filter.Type {
			if summary.Type == t {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	if filter.From != nil && summary.CreatedAt.Before(*filter.From) {
		return false
	}
	if filter.To != nil && summary.CreatedAt.After(*filter.To) {
		return false
	}
	return true
}

func hasAll(values []string, required []string) bool {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	for _, r := range required {
		if _, ok := set[r]; !ok {
			return false
		}
	}
	return true
}
