package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"runtime"

	gostage "github.com/davidroman0O/gostage/v3"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	storepkg "github.com/davidroman0O/gostage/v3/store"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbPath := exampleDBPath("dynamic-mutations.db")
	db, err := openSQLite(dbPath)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	workflowID := registerWorkflow()

	node, diag, err := gostage.Run(ctx,
		gostage.WithSQLite(gostage.SQLiteConfig{
			DB:              db,
			ApplyMigrations: true,
		}),
		gostage.WithTelemetrySink(telemetry.NewLoggerSink(log.Default())),
		gostage.WithPool(gostage.PoolConfig{Name: "dynamic", Tags: []string{"dynamic"}, Slots: 1}),
	)
	if err != nil {
		log.Fatalf("gostage.Run: %v", err)
	}
	defer node.Close()

	go drainDiagnostics(diag)

	runID, err := node.Submit(ctx,
		gostage.WorkflowRef(workflowID),
		gostage.WithTags("dynamic"),
		gostage.WithInitialStore(map[string]any{"order_id": "dyn-1"}),
	)
	if err != nil {
		log.Fatalf("submit: %v", err)
	}

	if _, err := node.Wait(ctx, runID); err != nil {
		log.Fatalf("wait: %v", err)
	}

	printSummary(ctx, node, runID)
	log.Printf("SQLite database written to %s", dbPath)
}

func registerWorkflow() string {
	gostage.MustRegisterAction("dynamic.prepare", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			if err := storepkg.Put(ctx, "prepared", true); err != nil {
				return err
			}
			ctx.Logger().Info("prepared", "order", mustGet[string](ctx, "order_id"))
			return nil
		}
	}, gostage.WithActionDescription("Initial preparation"))

	gostage.MustRegisterAction("dynamic.branch", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			stage := workflow.Stage{
				Name: "Manual review",
				Actions: []workflow.Action{
					{Ref: "dynamic.approve"},
				},
			}
			matStage, err := workflow.MaterializeStage(stage, nil)
			if err != nil {
				return err
			}
			id := ctx.Stages().Add(matStage)
			ctx.Logger().Info("added dynamic stage", "stage_id", id)
			ctx.Actions().DisableByTags([]string{"skip"})
			return nil
		}
	}, gostage.WithActionDescription("Adds review stage and disables tagged action"))

	gostage.MustRegisterAction("dynamic.approve", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			ctx.Logger().Info("manual review approved")
			return nil
		}
	}, gostage.WithActionDescription("Handle manual review"))

	gostage.MustRegisterAction("dynamic.skip", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			ctx.Logger().Info("skip action executed")
			return nil
		}
	}, gostage.WithActionDescription("Will be disabled during runtime"))

	def := workflow.Definition{
		Name: "Dynamic Mutations",
		Tags: []string{"dynamic"},
		Stages: []workflow.Stage{
			{
				Name: "Prepare",
				Actions: []workflow.Action{
					{Ref: "dynamic.prepare"},
				},
			},
			{
				Name: "Branch",
				Actions: []workflow.Action{
					{Ref: "dynamic.branch"},
					{Ref: "dynamic.skip", Tags: []string{"skip"}},
				},
			},
		},
	}

	id, assignment := gostage.MustRegisterWorkflow(def)
	log.Printf("registered workflow %q with %d stages", id, len(assignment.Stages))
	return id
}

func printSummary(ctx context.Context, node *gostage.Node, id gostage.WorkflowID) {
	if node.State == nil {
		log.Println("state facade unavailable")
		return
	}
	summary, err := node.State.WorkflowSummary(ctx, id)
	if err != nil {
		log.Printf("summary error: %v", err)
		return
	}
	log.Printf("summary: state=%s success=%t duration=%s", summary.State, summary.Success, summary.Duration)
	history, err := node.State.ActionHistory(ctx, id)
	if err != nil {
		log.Printf("history error: %v", err)
		return
	}
	for _, record := range history {
		log.Printf("  action id=%s stage=%s state=%s dynamic=%t created_by=%s",
			record.ActionID, record.StageID, record.State, record.Dynamic, record.CreatedBy)
	}
}

func openSQLite(path string) (*sql.DB, error) {
	dsn := fmt.Sprintf("file:%s?_busy_timeout=15000&_foreign_keys=on&_journal_mode=WAL", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

func exampleDBPath(filename string) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return filename
	}
	return filepath.Join(filepath.Dir(file), filename)
}

func drainDiagnostics(diag <-chan gostage.DiagnosticEvent) {
	for evt := range diag {
		log.Printf("[diagnostic] component=%s severity=%s err=%v metadata=%v",
			evt.Component, evt.Severity, evt.Err, evt.Metadata)
	}
}

func mustGet[T any](ctx rt.Context, key string) T {
	val, _ := storepkg.Get[T](ctx.Store(), key)
	return val
}
