package state

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/davidroman0O/gostage/v3/state/sqlc"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// SQLiteTelemetrySink writes telemetry events into SQLite.
type SQLiteTelemetrySink struct {
	queries *sqlc.Queries
}

func NewSQLiteTelemetrySink(db *sql.DB) (*SQLiteTelemetrySink, error) {
	if db == nil {
		return nil, sql.ErrConnDone
	}
	return &SQLiteTelemetrySink{queries: sqlc.New(db)}, nil
}

func (s *SQLiteTelemetrySink) Record(evt telemetry.Event) {
	meta := cloneMetadata(evt.Metadata)
	if evt.Progress != nil {
		progress := map[string]any{
			"percent": evt.Progress.Percent,
		}
		if evt.Progress.Message != "" {
			progress["message"] = evt.Progress.Message
		}
		if meta == nil {
			meta = make(map[string]any, 1)
		}
		meta["progress"] = progress
	}
	metadata, _ := json.Marshal(meta)
	workflowID := sql.NullString{}
	if evt.WorkflowID != "" {
		workflowID = sql.NullString{String: evt.WorkflowID, Valid: true}
	}
	stageID := sql.NullString{}
	if evt.StageID != "" {
		stageID = sql.NullString{String: evt.StageID, Valid: true}
	}
	actionID := sql.NullString{}
	if evt.ActionID != "" {
		actionID = sql.NullString{String: evt.ActionID, Valid: true}
	}
	message := sql.NullString{}
	if evt.Message != "" {
		message = sql.NullString{String: evt.Message, Valid: true}
	}
	attempt := sql.NullInt64{}
	if evt.Attempt > 0 {
		attempt = sql.NullInt64{Int64: int64(evt.Attempt), Valid: true}
	}
	errMsg := sql.NullString{}
	if evt.Error != "" {
		errMsg = sql.NullString{String: evt.Error, Valid: true}
	}
	_ = s.queries.InsertTelemetryEvent(context.Background(), sqlc.InsertTelemetryEventParams{
		WorkflowID: workflowID,
		StageID:    stageID,
		ActionID:   actionID,
		Kind:       string(evt.Kind),
		Attempt:    attempt,
		OccurredAt: evt.Timestamp,
		Message:    message,
		Metadata:   metadata,
		Error:      errMsg,
	})
}

func cloneMetadata(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
