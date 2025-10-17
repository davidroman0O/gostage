package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"time"

	gostage "github.com/davidroman0O/gostage/v3"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	storepkg "github.com/davidroman0O/gostage/v3/store"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func registerWorkflow() string {
	gostage.MustRegisterAction("example.prepare", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			if err := storepkg.Put(ctx, "prepared_at", time.Now().Format(time.RFC3339)); err != nil {
				return err
			}
			ctx.Logger().Info("prepared workflow state")
			return nil
		}
	}, gostage.WithActionDescription("Prepare request data"))

	gostage.MustRegisterAction("example.finalise", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			ctx.Logger().Info("finalising workflow")
			return nil
		}
	}, gostage.WithActionDescription("Finalise workflow"))

	def := workflow.Definition{
		Name:        "Core Local Example",
		Description: "Demonstrates gostage.Run with SQLite persistence.",
		Tags:        []string{"example", "core"},
		Stages: []workflow.Stage{
			{
				Name: "Preparation",
				Actions: []workflow.Action{
					{Ref: "example.prepare"},
				},
			},
			{
				Name: "Finalise",
				Actions: []workflow.Action{
					{Ref: "example.finalise"},
				},
			},
		},
	}

	var assignment workflow.IDAssignment
	workflowID, assignment := gostage.MustRegisterWorkflow(def)

	log.Printf("registered workflow %q with stages:\n", workflowID)
	for _, stage := range assignment.Stages {
		name := def.Stages[stage.Index].Name
		log.Printf("  stage[%d] id=%q name=%q", stage.Index, stage.ID, name)
		for _, action := range stage.Actions {
			ref := def.Stages[stage.Index].Actions[action.Index].Ref
			log.Printf("    action[%d] id=%q ref=%q", action.Index, action.ID, ref)
		}
	}
	return workflowID
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbPath := exampleDBPath("core-local-node.db")
	db, err := openSQLite(dbPath)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	workflowID := registerWorkflow()

	node, diagnostics, err := gostage.Run(ctx,
		gostage.WithSQLite(gostage.SQLiteConfig{
			DB:              db,
			ApplyMigrations: true,
		}),
		gostage.WithTelemetrySink(telemetry.NewLoggerSink(log.Default())),
	)
	if err != nil {
		log.Fatalf("gostage.Run: %v", err)
	}
	defer node.Close()

	go func() {
		for diag := range diagnostics {
			log.Printf("[diagnostics] component=%s severity=%s err=%v",
				diag.Component, diag.Severity, diag.Err)
		}
	}()

	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID))
	if err != nil {
		log.Fatalf("submit: %v", err)
	}
	log.Printf("submitted workflow %s", runID)

	result, err := node.Wait(ctx, runID)
	if err != nil {
		log.Fatalf("wait: %v", err)
	}
	log.Printf("workflow %s success=%t duration=%s", result.WorkflowID, result.Success, result.Duration)

	if stats, err := node.Stats(); err == nil {
		log.Printf("stats: queue_depth=%d in_flight=%d completed=%d failed=%d cancelled=%d",
			stats.QueueDepth, stats.InFlight, stats.Completed, stats.Failed, stats.Cancelled)
		for _, pool := range stats.Pools {
			log.Printf("  pool %s healthy=%t busy=%d/%d pending=%d status=%s",
				pool.Name, pool.Healthy, pool.Busy, pool.Slots, pool.Pending, pool.Status)
		}
	} else {
		log.Printf("stats error: %v", err)
	}

	printSummary(ctx, node, runID)
	fmt.Printf("\nSQLite database written to %s\n", dbPath)
}

func printSummary(ctx context.Context, node *gostage.Node, id gostage.WorkflowID) {
	if node.State == nil {
		log.Println("state facade unavailable")
		return
	}
	summary, err := node.State.WorkflowSummary(ctx, id)
	if err != nil {
		log.Printf("workflow summary error: %v", err)
		return
	}
	log.Printf("summary: state=%s success=%t started=%s completed=%s duration=%s",
		summary.State, summary.Success, summary.StartedAt, summary.CompletedAt, summary.Duration)
	if history, err := node.State.ActionHistory(ctx, id); err == nil {
		for _, record := range history {
			log.Printf("  action id=%s stage=%s state=%s started=%v completed=%v message=%q progress=%d",
				record.ActionID, record.StageID, record.State, record.StartedAt, record.CompletedAt, record.Message, record.Progress)
		}
	} else {
		log.Printf("action history error: %v", err)
	}
}

func exampleDBPath(filename string) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return filename
	}
	return filepath.Join(filepath.Dir(file), filename)
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
