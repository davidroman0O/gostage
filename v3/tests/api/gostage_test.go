package gostage_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	gostage "github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/internal/foundation/clock"
	"github.com/davidroman0O/gostage/v3/internal/foundation/gostagetest"
	"github.com/davidroman0O/gostage/v3/internal/domain"
	"github.com/davidroman0O/gostage/v3/internal/domain/queue"
	"github.com/davidroman0O/gostage/v3/internal/domain/store"
	"github.com/davidroman0O/gostage/v3/internal/domain/writer"
	statetest "github.com/davidroman0O/gostage/v3/internal/domain/testkit"
	_ "modernc.org/sqlite"
)

// writerToDomainObserverAdapter adapts writer.ManagerObserver to domain.ManagerObserver
type writerToDomainObserverAdapter struct {
	impl writer.ManagerObserver
}

func (a *writerToDomainObserverAdapter) WorkflowRegistered(ctx context.Context, wf domain.WorkflowRecord) {
	if a.impl == nil {
		return
	}
	writerWf := convertDomainWorkflowRecordToWriter(wf)
	a.impl.WorkflowRegistered(ctx, writerWf)
}

func (a *writerToDomainObserverAdapter) WorkflowStatus(ctx context.Context, workflowID string, status domain.WorkflowState) {
	if a.impl == nil {
		return
	}
	a.impl.WorkflowStatus(ctx, workflowID, writer.WorkflowState(status))
}

func (a *writerToDomainObserverAdapter) StageRegistered(ctx context.Context, workflowID string, stage domain.StageRecord) {
	if a.impl == nil {
		return
	}
	writerStage := convertDomainStageRecordToWriter(stage)
	a.impl.StageRegistered(ctx, workflowID, writerStage)
}

func (a *writerToDomainObserverAdapter) StageStatus(ctx context.Context, workflowID, stageID string, status domain.WorkflowState) {
	if a.impl == nil {
		return
	}
	a.impl.StageStatus(ctx, workflowID, stageID, writer.WorkflowState(status))
}

func (a *writerToDomainObserverAdapter) ActionRegistered(ctx context.Context, workflowID, stageID string, action domain.ActionRecord) {
	if a.impl == nil {
		return
	}
	writerAction := convertDomainActionRecordToWriter(action)
	a.impl.ActionRegistered(ctx, workflowID, stageID, writerAction)
}

func (a *writerToDomainObserverAdapter) ActionStatus(ctx context.Context, workflowID, stageID, actionID string, status domain.WorkflowState) {
	if a.impl == nil {
		return
	}
	a.impl.ActionStatus(ctx, workflowID, stageID, actionID, writer.WorkflowState(status))
}

func (a *writerToDomainObserverAdapter) ActionProgress(ctx context.Context, workflowID, stageID, actionID string, progress int, message string) {
	if a.impl == nil {
		return
	}
	a.impl.ActionProgress(ctx, workflowID, stageID, actionID, progress, message)
}

func (a *writerToDomainObserverAdapter) ActionEvent(ctx context.Context, workflowID, stageID, actionID, kind, message string, metadata map[string]any) {
	if a.impl == nil {
		return
	}
	a.impl.ActionEvent(ctx, workflowID, stageID, actionID, kind, message, metadata)
}

func (a *writerToDomainObserverAdapter) ActionRemoved(ctx context.Context, workflowID, stageID, actionID, createdBy string) {
	if a.impl == nil {
		return
	}
	a.impl.ActionRemoved(ctx, workflowID, stageID, actionID, createdBy)
}

func (a *writerToDomainObserverAdapter) StageRemoved(ctx context.Context, workflowID, stageID, createdBy string) {
	if a.impl == nil {
		return
	}
	a.impl.StageRemoved(ctx, workflowID, stageID, createdBy)
}

func (a *writerToDomainObserverAdapter) ExecutionSummary(ctx context.Context, workflowID string, summary domain.ResultSummary) {
	if a.impl == nil {
		return
	}
	writerSummary := convertDomainResultSummaryToWriter(summary)
	a.impl.ExecutionSummary(ctx, workflowID, writerSummary)
}

// Helper conversion functions
func convertDomainWorkflowRecordToWriter(rec domain.WorkflowRecord) writer.WorkflowRecord {
	return writer.WorkflowRecord{
		ID:                writer.WorkflowID(rec.ID),
		Name:              rec.Name,
		Description:       rec.Description,
		Type:              rec.Type,
		Tags:              rec.Tags,
		Metadata:          rec.Metadata,
		CreatedAt:         rec.CreatedAt,
		State:             writer.WorkflowState(rec.State),
		StartedAt:         rec.StartedAt,
		CompletedAt:      rec.CompletedAt,
		Duration:          rec.Duration,
		Success:           rec.Success,
		Error:             rec.Error,
		TerminationReason: writer.TerminationReason(rec.TerminationReason),
		Definition:        convertDomainSubWorkflowDefToWriter(rec.Definition),
		Stages:            convertDomainStagesMapToWriter(rec.Stages),
	}
}

func convertDomainSubWorkflowDefToWriter(def domain.SubWorkflowDef) writer.SubWorkflowDef {
	return writer.SubWorkflowDef{
		ID:        def.ID,
		Name:      def.Name,
		Type:      def.Type,
		Tags:      def.Tags,
		Metadata:  def.Metadata,
		Payload:   def.Payload,
		Priority:  int(def.Priority),
		CreatedAt: def.CreatedAt,
	}
}

func convertDomainStagesMapToWriter(stages map[string]*domain.StageRecord) map[string]*writer.StageRecord {
	if stages == nil {
		return nil
	}
	result := make(map[string]*writer.StageRecord, len(stages))
	for k, v := range stages {
		if v == nil {
			continue
		}
		writerStage := convertDomainStageRecordToWriter(*v)
		result[k] = &writerStage
	}
	return result
}

func convertDomainStageRecordToWriter(stage domain.StageRecord) writer.StageRecord {
	return writer.StageRecord{
		ID:          stage.ID,
		Name:        stage.Name,
		Description: stage.Description,
		Tags:        stage.Tags,
		Dynamic:     stage.Dynamic,
		CreatedBy:   stage.CreatedBy,
		Status:      writer.WorkflowState(stage.Status),
		Actions:     convertDomainActionsMapToWriter(stage.Actions),
		StartedAt:   stage.StartedAt,
		CompletedAt: stage.CompletedAt,
	}
}

func convertDomainActionsMapToWriter(actions map[string]*domain.ActionRecord) map[string]*writer.ActionRecord {
	if actions == nil {
		return nil
	}
	result := make(map[string]*writer.ActionRecord, len(actions))
	for k, v := range actions {
		if v == nil {
			continue
		}
		writerAction := convertDomainActionRecordToWriter(*v)
		result[k] = &writerAction
	}
	return result
}

func convertDomainActionRecordToWriter(action domain.ActionRecord) writer.ActionRecord {
	return writer.ActionRecord{
		Ref:         action.Ref,
		Name:        action.Name,
		Description: action.Description,
		Tags:        action.Tags,
		Dynamic:     action.Dynamic,
		CreatedBy:   action.CreatedBy,
		Status:      writer.WorkflowState(action.Status),
		StartedAt:   action.StartedAt,
		CompletedAt: action.CompletedAt,
	}
}

func convertDomainResultSummaryToWriter(summary domain.ResultSummary) writer.ResultSummary {
	return writer.ResultSummary{
		Success:         summary.Success,
		Error:           summary.Error,
		Attempt:         summary.Attempt,
		Output:          summary.Output,
		Duration:        summary.Duration,
		CompletedAt:     summary.CompletedAt,
		DisabledStages:  summary.DisabledStages,
		DisabledActions: summary.DisabledActions,
		RemovedStages:   summary.RemovedStages,
		RemovedActions:  summary.RemovedActions,
		Reason:          writer.TerminationReason(summary.Reason),
	}
}

type stringerValue struct{}

func (stringerValue) String() string { return "stringer" }

type badJSON struct{}

func (badJSON) MarshalJSON() ([]byte, error) { return nil, fmt.Errorf("boom") }

func TestPoolMetadataToStrings(t *testing.T) {
	cases := []struct {
		name   string
		input  map[string]any
		expect map[string]string
	}{
		{
			name:   "nil map",
			input:  nil,
			expect: nil,
		},
		{
			name: "string passthrough",
			input: map[string]any{
				"region": "us-central",
			},
			expect: map[string]string{
				"region": "us-central",
			},
		},
		{
			name: "numeric json",
			input: map[string]any{
				"slots": 3,
			},
			expect: map[string]string{
				"slots": "3",
			},
		},
		{
			name: "struct json",
			input: map[string]any{
				"config": map[string]any{"env": "dev", "count": 2},
			},
			expect: map[string]string{
				"config": `{"count":2,"env":"dev"}`,
			},
		},
		{
			name: "stringer",
			input: map[string]any{
				"value": stringerValue{},
			},
			expect: map[string]string{
				"value": "stringer",
			},
		},
		{
			name: "nil values dropped",
			input: map[string]any{
				"keep": "value",
				"drop": nil,
			},
			expect: map[string]string{
				"keep": "value",
			},
		},
	}

	for _, tc := range cases {
		if got := gostagetest.PoolMetadataToStrings(tc.input); !reflect.DeepEqual(got, tc.expect) {
			t.Fatalf("%s: expected %v, got %v", tc.name, tc.expect, got)
		}
	}
}

func TestPoolMetadataToStringsPanicsOnEncodeError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when metadata encoding fails")
		}
	}()

	gostagetest.PoolMetadataToStrings(map[string]any{"bad": badJSON{}})
}

func TestRunWithSQLiteExistingDBPreservesPragmas(t *testing.T) {
	t.Helper()

	tempDir := t.TempDir()
	dbPath := fmt.Sprintf("file:%s/test.db?mode=rwc", tempDir)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatalf("set foreign_keys: %v", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=4321"); err != nil {
		t.Fatalf("set busy_timeout: %v", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=DELETE"); err != nil {
		t.Fatalf("set journal_mode: %v", err)
	}

	memQueue := queue.NewMemoryQueue(clock.DefaultClock())
	memStore := store.NewMemoryStore()
	queueAdapter := domain.NewQueueAdapter(memQueue)
	storeAdapter := domain.NewStoreAdapter(memStore)
	observer := statetest.NewCaptureObserver()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create adapter for observer (from writer.ManagerObserver to domain.ManagerObserver)
	observerAdapter := &writerToDomainObserverAdapter{impl: observer}

	node, diag, err := gostage.Run(ctx,
		gostage.WithQueue(queueAdapter),
		gostage.WithStore(storeAdapter),
		gostage.WithStateObserver(observerAdapter),
		gostage.WithSQLite(gostage.SQLiteConfig{
			DB:              db,
			ApplyMigrations: false,
			DisableWAL:      true,
		}),
	)
	if err != nil {
		t.Fatalf("run with existing db: %v", err)
	}

	done := make(chan struct{})
	go func() {
		for range diag {
			_ = struct{}{} // Drain diagnostics channel
		}
		close(done)
	}()

	cancel()
	if err := node.Close(); err != nil {
		t.Fatalf("close node: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("diagnostics channel not closed in time")
	}

	if val := pragmaInt(t, db, "foreign_keys"); val != 0 {
		t.Fatalf("foreign_keys changed unexpectedly: got %d", val)
	}
	if val := pragmaInt(t, db, "busy_timeout"); val != 4321 {
		t.Fatalf("busy_timeout changed unexpectedly: got %d", val)
	}
	if mode := pragmaText(t, db, "journal_mode"); mode != "delete" {
		t.Fatalf("journal_mode changed unexpectedly: got %s", mode)
	}
}

func pragmaInt(t *testing.T, db *sql.DB, pragma string) int {
	t.Helper()
	row := db.QueryRow("PRAGMA " + pragma)
	var value int
	if err := row.Scan(&value); err != nil {
		t.Fatalf("scan pragma %s: %v", pragma, err)
	}
	return value
}

func pragmaText(t *testing.T, db *sql.DB, pragma string) string {
	t.Helper()
	row := db.QueryRow("PRAGMA " + pragma)
	var value string
	if err := row.Scan(&value); err != nil {
		t.Fatalf("scan pragma %s: %v", pragma, err)
	}
	return value
}
