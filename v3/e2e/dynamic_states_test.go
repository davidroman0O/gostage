package e2e

import (
    "context"
    "database/sql"
    "fmt"
    "path/filepath"
    "testing"
    "time"

    "github.com/davidroman0O/gostage/v3"
    "github.com/davidroman0O/gostage/v3/diagnostics"
    "github.com/davidroman0O/gostage/v3/e2e/testkit"
    rt "github.com/davidroman0O/gostage/v3/runtime"
    "github.com/davidroman0O/gostage/v3/state"
    storepkg "github.com/davidroman0O/gostage/v3/store"
    "github.com/davidroman0O/gostage/v3/telemetry"
    "github.com/davidroman0O/gostage/v3/workflow"
    _ "modernc.org/sqlite"
)

var dynamicWorkflowDefinition = workflow.Definition{
	Name: "dynamic states",
	Stages: []workflow.Stage{
		{
			ID:   "stage-control",
			Name: "control",
			Actions: []workflow.Action{
				{ID: "action-control", Ref: "control.remove"},
				{ID: "action-removable", Ref: "removable.run"},
			},
		},
		{
			ID:   "stage-skip",
			Name: "skipped stage",
			Actions: []workflow.Action{
				{ID: "action-stage-skip", Ref: "stage.skip.noop"},
			},
		},
		{
			ID:   "stage-action-skip",
			Name: "action skip",
			Actions: []workflow.Action{
				{ID: "action-skip", Ref: "action.skip.noop"},
			},
		},
	},
}

func registerDynamicActions(t *testing.T) {
	t.Helper()

	gostage.MustRegisterAction("control.remove", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			if err := storepkg.Put(ctx, "control_ran", true); err != nil {
				return err
			}
			ctx.Actions().Remove("action-removable")
			dynStage := workflow.NewRuntimeStage("dynamic-cleanup", "dynamic cleanup", "")
			dynStage.AddActions(workflow.MustRuntimeAction("cleanup.run"))
			dynStageID := ctx.Stages().Add(dynStage)
			if err := storepkg.Put(ctx, "dynamic_stage_id", dynStageID); err != nil {
				return err
			}
			ctx.Stages().Remove(dynStageID)
			ctx.Stages().Disable("stage-skip")
			ctx.Actions().Disable("action-skip")
			return nil
		}
	})
	gostage.MustRegisterAction("cleanup.run", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			return storepkg.Put(ctx, "cleanup_ran", true)
		}
	})
	gostage.MustRegisterAction("removable.run", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			return storepkg.Put(ctx, "removable_ran", true)
		}
	})
	gostage.MustRegisterAction("stage.skip.noop", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			return storepkg.Put(ctx, "stage_skip_ran", true)
		}
	})
	gostage.MustRegisterAction("action.skip.noop", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			return storepkg.Put(ctx, "action_skip_ran", true)
		}
	})
}

func TestDynamicRemovalAndSkipPersistence(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(t *testing.T) ([]gostage.Option, *sql.DB)
		verify func(t *testing.T, db *sql.DB, runID gostage.WorkflowID, dynStageID string)
	}{
		{
			name: "memory",
			setup: func(t *testing.T) ([]gostage.Option, *sql.DB) {
				backends := testkit.NewMemoryBackends()
				return testkit.MemoryOptions(backends), nil
			},
			verify: func(t *testing.T, _ *sql.DB, _ gostage.WorkflowID, _ string) {},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) ([]gostage.Option, *sql.DB) {
				tempDir := t.TempDir()
				dbPath := filepath.Join(tempDir, "dynamic.db")
				dsn := fmt.Sprintf("file:%s?_busy_timeout=15000&_foreign_keys=on&_journal_mode=WAL", dbPath)
				db, err := sql.Open("sqlite", dsn)
				if err != nil {
					t.Fatalf("open sqlite: %v", err)
				}
				db.SetMaxOpenConns(1)
				db.SetMaxIdleConns(1)
				t.Cleanup(func() { _ = db.Close() })
				opts := []gostage.Option{
					gostage.WithSQLite(gostage.SQLiteConfig{
						DB:              db,
						ApplyMigrations: true,
					}),
					gostage.WithPool(gostage.PoolConfig{Name: "dynamic", Slots: 2}),
				}
				return opts, db
			},
			verify: verifyDynamicSQLitePersistence,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testkit.ResetRegistry(t)
			registerDynamicActions(t)

			def := dynamicWorkflowDefinition
			workflowID, _ := gostage.MustRegisterWorkflow(def)

			opts, db := tc.setup(t)
			runDynamicScenario(t, opts, workflowID, db, tc.verify)
		})
	}
}

func runDynamicScenario(t *testing.T, opts []gostage.Option, workflowID string, db *sql.DB, verify func(t *testing.T, db *sql.DB, runID gostage.WorkflowID, dynStageID string)) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diag, err := gostage.Run(ctx, opts...)
	if err != nil {
		t.Fatalf("gostage run: %v", err)
	}
	collector := testkit.StartDiagnosticsCollector(t, diag)
	t.Cleanup(collector.Close)
	t.Cleanup(func() { _ = node.Close() })

	telemetryBuf := testkit.StartTelemetryBuffer(ctx, t, node, 256)
	t.Cleanup(telemetryBuf.Close)

	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(ctx, 20*time.Second)
	defer cancelWait()
	result, err := node.Wait(waitCtx, runID)
	if err != nil {
		for _, evt := range collector.Events() {
			t.Logf("diagnostic: component=%s severity=%s err=%v metadata=%v", evt.Component, evt.Severity, evt.Err, evt.Metadata)
		}
		t.Fatalf("wait: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got result %+v", result)
	}

	if _, ok := result.Output["control_ran"]; !ok {
		t.Fatalf("control action did not run, output=%+v", result.Output)
	}
	if _, ok := result.Output["cleanup_ran"]; ok {
		t.Fatalf("cleanup action should have been removed, output=%+v", result.Output)
	}
	if _, ok := result.Output["stage_skip_ran"]; ok {
		t.Fatalf("stage-skip action should not run, output=%+v", result.Output)
	}
	if _, ok := result.Output["action_skip_ran"]; ok {
		t.Fatalf("action-skip action should not run, output=%+v", result.Output)
	}
	if _, ok := result.Output["removable_ran"]; ok {
		t.Fatalf("removable action should not run, output=%+v", result.Output)
	}

	dynStageVal, ok := result.Output["dynamic_stage_id"]
	if !ok {
		t.Fatalf("missing dynamic stage id in output: %+v", result.Output)
	}
	dynStageID, ok := dynStageVal.(string)
	if !ok || dynStageID == "" {
		t.Fatalf("dynamic stage id not a string: %+v", dynStageVal)
	}
	if _, ok := result.RemovedStages[dynStageID]; !ok {
		t.Fatalf("removed stages missing dynamic entry: %+v", result.RemovedStages)
	}
	if _, ok := result.RemovedActions["stage-control::action-removable"]; !ok {
		t.Fatalf("removed actions missing removable entry: %+v", result.RemovedActions)
	}
	if disabled := result.DisabledActions["action-skip"]; !disabled {
		t.Fatalf("expected action-skip to be disabled: %+v", result.DisabledActions)
	}

	events := telemetryBuf.Collect(t, 200*time.Millisecond)
	hasEvent := func(kind telemetry.EventKind, predicate func(telemetry.Event) bool) bool {
		for _, evt := range events {
			if evt.Kind != kind {
				continue
			}
			if predicate == nil || predicate(evt) {
				return true
			}
		}
		return false
	}
	if !hasEvent(telemetry.EventStageRemoved, func(evt telemetry.Event) bool { return evt.StageID == dynStageID }) {
		t.Fatalf("expected dynamic stage removal telemetry, events=%+v", events)
	}
	if !hasEvent(telemetry.EventActionRemoved, func(evt telemetry.Event) bool { return evt.ActionID == "action-removable" }) {
		t.Fatalf("expected action-removable removal telemetry, events=%+v", events)
	}
	if !hasEvent(telemetry.EventStageSkipped, func(evt telemetry.Event) bool { return evt.StageID == "stage-skip" }) {
		t.Fatalf("expected stage-skip skipped telemetry, events=%+v", events)
	}
	if !hasEvent(telemetry.EventActionSkipped, func(evt telemetry.Event) bool { return evt.ActionID == "action-skip" }) {
		t.Fatalf("expected action-skip skipped telemetry, events=%+v", events)
	}

	if node.State == nil {
		t.Fatalf("expected state facade")
	}
	summary, err := node.State.WorkflowSummary(ctx, state.WorkflowID(runID))
	if err != nil {
		t.Fatalf("workflow summary: %v", err)
	}
	stageRecords := summary.Stages
	if len(stageRecords) == 0 {
		if db == nil {
			t.Fatalf("no stage records in summary: %+v", summary)
		}
	} else {
		if rec, ok := stageRecords[dynStageID]; !ok || rec.Status != state.WorkflowRemoved {
			t.Fatalf("expected dynamic stage removed, got %+v", stageRecords[dynStageID])
		}
		if rec, ok := stageRecords["stage-skip"]; !ok || rec.Status != state.WorkflowSkipped {
			t.Fatalf("expected stage-skip skipped, got %+v", stageRecords["stage-skip"])
		}
		if rec, ok := stageRecords["stage-control"]; !ok {
			t.Fatalf("missing stage-control record")
		} else if actionRec, ok := rec.Actions["action-removable"]; !ok || actionRec.Status != state.WorkflowRemoved {
			t.Fatalf("expected action-removable removed, got %+v", rec.Actions["action-removable"])
		}
		if rec, ok := stageRecords["stage-action-skip"]; !ok {
			t.Fatalf("missing stage-action-skip record")
		} else if actionRec, ok := rec.Actions["action-skip"]; !ok || actionRec.Status != state.WorkflowSkipped {
			t.Fatalf("expected action-skip skipped, got %+v", rec.Actions["action-skip"])
		}
	}

	verify(t, db, runID, dynStageID)

	for _, evt := range collector.Events() {
		if evt.Severity == diagnostics.SeverityError {
			t.Fatalf("diagnostic error: %+v", evt)
		}
	}
}

func verifyDynamicSQLitePersistence(t *testing.T, db *sql.DB, runID gostage.WorkflowID, dynStageID string) {
	if db == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	expectStageState := func(stageID string, stateExpected state.WorkflowState) {
		var stateValue string
		err := db.QueryRowContext(ctx,
			`SELECT state FROM stage_runs WHERE workflow_id = ? AND stage_id = ?`,
			runID, stageID,
		).Scan(&stateValue)
		if err != nil {
			t.Fatalf("stage_runs query (%s) failed: %v", stageID, err)
		}
		if stateValue != string(stateExpected) {
			t.Fatalf("expected stage %s state %s, got %s", stageID, stateExpected, stateValue)
		}
	}

	expectActionState := func(stageID, actionID string, stateExpected state.WorkflowState) {
		var stateValue string
		err := db.QueryRowContext(ctx,
			`SELECT state FROM action_runs WHERE workflow_id = ? AND stage_id = ? AND action_id = ?`,
			runID, stageID, actionID,
		).Scan(&stateValue)
		if err != nil {
			t.Fatalf("action_runs query (%s::%s) failed: %v", stageID, actionID, err)
		}
		if stateValue != string(stateExpected) {
			t.Fatalf("expected action %s::%s state %s, got %s", stageID, actionID, stateExpected, stateValue)
		}
	}

	expectStageState("stage-skip", state.WorkflowSkipped)
	expectActionState("stage-control", "action-removable", state.WorkflowRemoved)
	expectActionState("stage-action-skip", "action-skip", state.WorkflowSkipped)
}
