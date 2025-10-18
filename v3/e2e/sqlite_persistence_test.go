package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/e2e/testkit"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/store"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestSQLitePersistenceEndToEnd(t *testing.T) {
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("sqlite.progress", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			_ = store.Put(ctx, "result", "ok")
			if broker := ctx.Broker(); broker != nil {
				_ = broker.Progress(100, "complete")
			}
			return nil
		}
	})

	def := workflow.Definition{
		Name: "SQLite Persistence",
		Tags: []string{"sqlite"},
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: "sqlite.progress"}},
		}},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "state.db")
	dsn := fmt.Sprintf("file:%s?_busy_timeout=15000&_foreign_keys=on&_journal_mode=WAL", dbPath)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diag, err := gostage.Run(ctx,
		gostage.WithSQLite(gostage.SQLiteConfig{
			DB:              db,
			ApplyMigrations: true,
		}),
		gostage.WithPool(gostage.PoolConfig{Name: "sqlite", Tags: []string{"sqlite"}, Slots: 2}),
	)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}

	collector := testkit.StartDiagnosticsCollector(t, diag)
	t.Cleanup(collector.Close)
	t.Cleanup(func() { _ = node.Close() })

	teleBuf := testkit.StartTelemetryBuffer(ctx, t, node, 128)
	t.Cleanup(teleBuf.Close)
	healthBuf := testkit.StartHealthBuffer(ctx, t, node, 32)
	t.Cleanup(healthBuf.Close)

	runID, err := node.Submit(ctx,
		gostage.WorkflowRef(workflowID),
		gostage.WithTags("sqlite"),
		gostage.WithInitialStore(map[string]any{"input": "value"}),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 10*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runID)
	if err != nil {
		for _, evt := range collector.Events() {
			t.Logf("diagnostic: component=%s severity=%s err=%v", evt.Component, evt.Severity, evt.Err)
		}
		t.Fatalf("wait: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got %+v", result)
	}
	if result.Output["result"] != "ok" {
		t.Fatalf("expected store result 'ok', got %+v", result.Output)
	}

	progressEvt := teleBuf.Next(t, telemetry.EventActionProgressKind, 3*time.Second)
	if progressEvt.Progress == nil || progressEvt.Progress.Percent != 100 {
		t.Fatalf("expected progress 100, got %+v", progressEvt.Progress)
	}
	summaryEvt := teleBuf.Next(t, telemetry.EventWorkflowSummary, 3*time.Second)
	if success, _ := summaryEvt.Metadata["success"].(bool); !success {
		t.Fatalf("expected summary success, got %+v", summaryEvt.Metadata)
	}

	healthy := healthBuf.Next(t, "healthy", 3*time.Second)
	if healthy.Pool != "sqlite" {
		t.Fatalf("expected health pool 'sqlite', got %s", healthy.Pool)
	}

	if node.State == nil {
		t.Fatalf("expected state reader to be initialised")
	}
	summary := testkit.AwaitWorkflowSummary(t, node.State, nil, ctx, runID)
	if summary.State != state.WorkflowCompleted {
		t.Fatalf("expected completed state, got %+v", summary.State)
	}

	assertRowExists := func(query string, args ...any) {
		deadline := time.Now().Add(5 * time.Second)
		for {
			var count int
			qCtx, cancel := context.WithTimeout(ctx, time.Second)
			err := db.QueryRowContext(qCtx, query, args...).Scan(&count)
			cancel()
			if err == nil && count > 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("query %q did not return results: %v", query, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	assertRowExists("SELECT COUNT(*) FROM telemetry_events WHERE workflow_id = ? AND kind = ?", string(runID), string(telemetry.EventWorkflowSummary))
	assertRowExists("SELECT COUNT(*) FROM execution_summaries WHERE workflow_id = ?", string(runID))

	var stateVal string
	var successVal int
	queryCtx, cancelQuery := context.WithTimeout(ctx, time.Second)
	err = db.QueryRowContext(queryCtx, "SELECT state, success FROM workflow_runs WHERE id = ?", string(runID)).Scan(&stateVal, &successVal)
	cancelQuery()
	if err != nil {
		t.Fatalf("workflow_runs query: %v", err)
	}
	if stateVal != string(state.WorkflowCompleted) || successVal != 1 {
		t.Fatalf("unexpected workflow_runs row: state=%s success=%d", stateVal, successVal)
	}
}

func TestSQLiteCancellationPersistsCancelledState(t *testing.T) {
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("sqlite.cancel", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return nil
			}
		}
	})

	def := workflow.Definition{
		Name: "SQLite Cancel",
		Tags: []string{"sqlite-cancel"},
		Stages: []workflow.Stage{{
			Name:    "block",
			Actions: []workflow.Action{{Ref: "sqlite.cancel"}},
		}},
	}
	workflowID, _ := gostage.MustRegisterWorkflow(def)

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "cancel.db")
	dsn := fmt.Sprintf("file:%s?_busy_timeout=15000&_foreign_keys=on&_journal_mode=WAL", dbPath)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diag, err := gostage.Run(ctx,
		gostage.WithSQLite(gostage.SQLiteConfig{
			DB:              db,
			ApplyMigrations: true,
		}),
		gostage.WithPool(gostage.PoolConfig{Name: "sqlite-cancel", Tags: []string{"sqlite-cancel"}, Slots: 1}),
	)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	collector := testkit.StartDiagnosticsCollector(t, diag)
	t.Cleanup(collector.Close)
	t.Cleanup(func() { _ = node.Close() })

	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID), gostage.WithTags("sqlite-cancel"))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	claimDeadline := time.Now().Add(5 * time.Second)
	for {
		stats, serr := node.Stats()
		if serr != nil {
			t.Fatalf("stats during claim wait: %v", serr)
		}
		if stats.InFlight > 0 {
			break
		}
		if time.Now().After(claimDeadline) {
			t.Fatalf("workflow not claimed before deadline")
		}
		time.Sleep(25 * time.Millisecond)
	}

	if err := node.Cancel(ctx, runID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 10*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runID)
	if err != nil {
		for _, evt := range collector.Events() {
			t.Logf("diagnostic: component=%s severity=%s err=%v", evt.Component, evt.Severity, evt.Err)
		}
		t.Fatalf("wait after cancel: %v", err)
	}
	if result.Success {
		t.Fatalf("expected cancellation result, got %+v", result)
	}

	queryCtx, cancelQuery := context.WithTimeout(ctx, time.Second)
	var wfState string
	var wfSuccess int
	if err := db.QueryRowContext(queryCtx, "SELECT state, success FROM workflow_runs WHERE id = ?", string(runID)).Scan(&wfState, &wfSuccess); err != nil {
		cancelQuery()
		t.Fatalf("workflow_runs query: %v", err)
	}
	cancelQuery()
	if wfState != string(state.WorkflowCancelled) || wfSuccess != 0 {
		t.Fatalf("unexpected workflow_runs row: state=%s success=%d", wfState, wfSuccess)
	}

	getLifecycle := func(query string) (string, int, int) {
		deadline := time.Now().Add(5 * time.Second)
		for {
			qCtx, cancel := context.WithTimeout(ctx, time.Second)
			var (
				stateVal      string
				started, done int
			)
			err := db.QueryRowContext(qCtx, query, string(runID)).Scan(&stateVal, &started, &done)
			cancel()
			if err == nil {
				return stateVal, started, done
			}
			if errors.Is(err, sql.ErrNoRows) {
				if time.Now().After(deadline) {
					t.Fatalf("query %q returned no rows before deadline", query)
				}
				time.Sleep(20 * time.Millisecond)
				continue
			}
			t.Fatalf("query %q failed: %v", query, err)
		}
	}

	stageState, stageStarted, stageCompleted := getLifecycle(`
		SELECT state, started_at IS NOT NULL, completed_at IS NOT NULL
		FROM stage_runs
		WHERE workflow_id = ?
		LIMIT 1
	`)
	if stageState != string(state.WorkflowCancelled) || stageStarted == 0 || stageCompleted == 0 {
		t.Fatalf("unexpected stage_runs row: state=%s started=%d completed=%d", stageState, stageStarted, stageCompleted)
	}

	actionState, actionStarted, actionCompleted := getLifecycle(`
		SELECT state, started_at IS NOT NULL, completed_at IS NOT NULL
		FROM action_runs
		WHERE workflow_id = ?
		LIMIT 1
	`)
	if actionState != string(state.WorkflowCancelled) || actionStarted == 0 || actionCompleted == 0 {
		t.Fatalf("unexpected action_runs row: state=%s started=%d completed=%d", actionState, actionStarted, actionCompleted)
	}
}
