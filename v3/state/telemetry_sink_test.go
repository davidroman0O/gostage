package state

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/state/sqlc"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestSQLiteTelemetrySinkPersistsEvents(t *testing.T) {
	db := openTestDB(t)
	sink, err := NewSQLiteTelemetrySink(db)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	definition := workflow.Definition{Stages: []workflow.Stage{{Actions: []workflow.Action{{Ref: "noop"}}}}}
	_, assignment, err := workflow.EnsureIDs(definition)
	if err != nil {
		t.Fatalf("ensure ids: %v", err)
	}
	stageID := assignment.Stages[0].ID
	evt := telemetry.Event{
		Kind:       telemetry.EventWorkflowStarted,
		WorkflowID: "wf-123",
		StageID:    stageID,
		Timestamp:  time.Now(),
		Metadata:   map[string]any{"k": "v"},
	}
	sink.Record(evt)

	queries := sqlc.New(db)
	rows, err := queries.ListTelemetryByWorkflow(context.Background(), sql.NullString{String: "wf-123", Valid: true})
	if err != nil {
		t.Fatalf("list telemetry: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one telemetry row, got %d", len(rows))
	}
	if rows[0].Kind != string(evt.Kind) {
		t.Fatalf("unexpected telemetry kind: %s", rows[0].Kind)
	}
}
