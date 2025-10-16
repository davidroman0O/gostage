package e2e

import (
	"context"
	"database/sql"
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

func TestDraftUsageSQLitePersistence(t *testing.T) {
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
		Stages: []workflow.Stage{
			{
				Name: "stage",
				Actions: []workflow.Action{
					{Ref: "sqlite.progress"},
				},
			},
		},
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
	telemetryCh := make(chan telemetry.Event, 128)
	teleCancel := node.StreamTelemetry(ctx, func(evt telemetry.Event) {
		select {
		case telemetryCh <- evt:
		default:
		}
	})

	defer func() {
		teleCancel()
		_ = node.Close()
		collector.Close()
	}()

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
		for {
			select {
			case evt := <-telemetryCh:
				t.Logf("telemetry: kind=%s workflow=%s stage=%s action=%s", evt.Kind, evt.WorkflowID, evt.StageID, evt.ActionID)
			default:
				goto telemetryLogged
			}
		}
	telemetryLogged:
		qCtx, cancelQuery := context.WithTimeout(ctx, time.Second)
		rows, qerr := db.QueryContext(qCtx, "SELECT id, state, attempts FROM queue_entries")
		if qerr == nil {
			for rows.Next() {
				var id string
				var stateVal string
				var attempts int
				_ = rows.Scan(&id, &stateVal, &attempts)
				t.Logf("queue_entry id=%s state=%s attempts=%d", id, stateVal, attempts)
			}
			rows.Close()
		} else {
			t.Logf("queue_entries query error: %v", qerr)
		}
		cancelQuery()
		t.Fatalf("wait: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got %+v", result)
	}
	if result.Output["result"] != "ok" {
		t.Fatalf("expected store result, got %+v", result.Output)
	}
	if node.State == nil {
		t.Fatalf("expected state reader to be initialised")
	}
	summary := testkit.AwaitWorkflowSummary(t, node.State, nil, ctx, runID)
	if summary.State != state.WorkflowCompleted {
		t.Fatalf("expected completed state, got %+v", summary)
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
