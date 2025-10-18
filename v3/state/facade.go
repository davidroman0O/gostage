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
	stateJSON, err := encodeStringArray(workflowStatesToStrings(filter.States))
	if err != nil {
		return nil, err
	}
	typeJSON, err := encodeStringArray(filter.Type)
	if err != nil {
		return nil, err
	}
	tagJSON, err := encodeStringArray(filter.Tags)
	if err != nil {
		return nil, err
	}

	params := sqlc.ListWorkflowsFilteredParams{
		StatesJson: stateJSON,
		TypeJson:   typeJSON,
		TagsJson:   tagJSON,
		FromTime:   filter.From,
		ToTime:     filter.To,
		OffsetRows: int64(filter.Offset),
		LimitRows:  int64(filter.Limit),
	}

	rows, err := r.queries.ListWorkflowsFiltered(ctx, params)
	if err != nil {
		return nil, err
	}
	out := make([]WorkflowSummary, 0, len(rows))
	for _, row := range rows {
		summary, err := convertWorkflowRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, summary)
	}
	return out, nil
}

func (r *SQLiteStateReader) ActionHistory(ctx context.Context, id WorkflowID) ([]ActionHistoryRecord, error) {
	rows, err := r.queries.ListActionsByWorkflow(ctx, string(id))
	if err != nil {
		return nil, err
	}

	progressMap := make(map[string]struct {
		progress int
		message  string
	})
	progressRows, err := r.queries.ListLatestActionProgress(ctx, sqlc.ListLatestActionProgressParams{
		WorkflowID:   sql.NullString{String: string(id), Valid: true},
		WorkflowID_2: sql.NullString{String: string(id), Valid: true},
	})
	if err != nil {
		return nil, err
	}
	for _, row := range progressRows {
		if !row.StageID.Valid || !row.ActionID.Valid {
			continue
		}
		var meta map[string]any
		if len(row.Metadata) > 0 {
			_ = json.Unmarshal(row.Metadata, &meta)
		}
		progress := 0
		if val, ok := meta["progress"]; ok {
			switch v := val.(type) {
			case float64:
				progress = int(v)
			case int:
				progress = v
			case int64:
				progress = int(v)
			}
		}
		msg := ""
		if row.Message.Valid {
			msg = row.Message.String
		}
		key := row.StageID.String + "::" + row.ActionID.String
		progressMap[key] = struct {
			progress int
			message  string
		}{progress: progress, message: msg}
	}
	history := make([]ActionHistoryRecord, 0, len(rows))
	for _, row := range rows {
		var tags []string
		if len(row.Tags) > 0 {
			_ = json.Unmarshal(row.Tags, &tags)
		}
		record := ActionHistoryRecord{
			ActionID:    row.ActionID,
			StageID:     row.StageID,
			Ref:         row.Ref.String,
			Description: row.Description.String,
			Tags:        append([]string(nil), tags...),
			Dynamic:     row.Dynamic == 1,
			CreatedBy:   row.CreatedBy.String,
			State:       WorkflowState(row.State),
		}
		if row.StartedAt.Valid {
			t := row.StartedAt.Time
			record.StartedAt = &t
		}
		if row.CompletedAt.Valid {
			t := row.CompletedAt.Time
			record.CompletedAt = &t
		}
		key := record.StageID + "::" + record.ActionID
		if entry, ok := progressMap[key]; ok {
			record.Progress = entry.progress
			record.Message = entry.message
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
	if row.TerminationReason != "" {
		summary.WorkflowRecord.TerminationReason = TerminationReason(row.TerminationReason)
	}
	return summary, nil
}

func encodeStringArray(values []string) (interface{}, error) {
	if len(values) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	return string(data), nil
}

func workflowStatesToStrings(states []WorkflowState) []string {
	if len(states) == 0 {
		return nil
	}
	out := make([]string, len(states))
	for i, st := range states {
		out[i] = string(st)
	}
	return out
}
