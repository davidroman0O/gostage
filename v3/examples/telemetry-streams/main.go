package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"time"

	gostage "github.com/davidroman0O/gostage/v3"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	storepkg "github.com/davidroman0O/gostage/v3/store"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbPath := exampleDBPath("telemetry-streams.db")
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
		gostage.WithPool(gostage.PoolConfig{
			Name:  "telemetry",
			Tags:  []string{"telemetry"},
			Slots: 1,
		}),
	)
	if err != nil {
		log.Fatalf("gostage.Run: %v", err)
	}
	defer node.Close()

	go drainDiagnostics(diag)

	var telemetryCount atomic.Int32
	stopTelemetry := node.StreamTelemetry(ctx, func(evt telemetry.Event) {
		telemetryCount.Add(1)
		log.Printf("[telemetry] kind=%s workflow=%s stage=%s action=%s progress=%v msg=%q err=%s",
			evt.Kind, evt.WorkflowID, evt.StageID, evt.ActionID, evt.Progress, evt.Message, evt.Error)
	})
	defer stopTelemetry()

	stopHealth := node.StreamHealth(ctx, func(evt gostage.HealthEvent) {
		log.Printf("[health] pool=%s status=%s detail=%s", evt.Pool, evt.Status, evt.Detail)
	})
	defer stopHealth()

	runID, err := node.Submit(ctx,
		gostage.WorkflowRef(workflowID),
		gostage.WithTags("telemetry"),
		gostage.WithInitialStore(map[string]any{"order_id": "order-abc"}),
	)
	if err != nil {
		log.Fatalf("submit: %v", err)
	}

	if _, err := node.Wait(ctx, runID); err != nil {
		log.Fatalf("wait: %v", err)
	}

	log.Printf("telemetry events captured: %d", telemetryCount.Load())
	log.Printf("SQLite database written to %s", dbPath)
}

func registerWorkflow() string {
	gostage.MustRegisterAction("telemetry.prepare", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			if err := storepkg.Put(ctx, "prepared", true); err != nil {
				return err
			}
			_ = gostage.EmitActionEvent(ctx, "action.prepare", "preparing order", map[string]any{
				"order_id": mustGet[string](ctx, "order_id"),
			})
			return nil
		}
	}, gostage.WithActionDescription("Record preparation telemetry"))

	gostage.MustRegisterAction("telemetry.process", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			broker := ctx.Broker()
			for i := 1; i <= 3; i++ {
				time.Sleep(200 * time.Millisecond)
				if broker != nil {
					_ = broker.Progress(i*33, fmt.Sprintf("step %d/3", i))
				}
			}
			return nil
		}
	}, gostage.WithActionDescription("Emit progress telemetry"))

	def := workflow.Definition{
		Name: "Telemetry Streams",
		Tags: []string{"telemetry"},
		Stages: []workflow.Stage{
			{
				Name: "Prepare",
				Actions: []workflow.Action{
					{Ref: "telemetry.prepare"},
				},
			},
			{
				Name: "Process",
				Actions: []workflow.Action{
					{Ref: "telemetry.process"},
				},
			},
		},
	}

	id, assignment := gostage.MustRegisterWorkflow(def)
	log.Printf("registered workflow %q with %d stages", id, len(assignment.Stages))
	return id
}

func drainDiagnostics(diag <-chan gostage.DiagnosticEvent) {
	for evt := range diag {
		log.Printf("[diagnostic] component=%s severity=%s err=%v metadata=%v",
			evt.Component, evt.Severity, evt.Err, evt.Metadata)
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

func mustGet[T any](ctx rt.Context, key string) T {
	val, _ := storepkg.Get[T](ctx.Store(), key)
	return val
}
