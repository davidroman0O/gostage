package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/store"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

const (
	sqlitePath     = "remote-node.db"
	spawnerName    = "remote-node-spawner"
	remotePoolName = "remote-requests"
)

var remoteWorkflowID string

func init() {
	gostage.MustRegisterAction("example.remote.fetch", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			store.Put(ctx, "fetched_at", time.Now().Format(time.RFC3339))
			ctx.Logger().Info("fetched remote resource")
			return nil
		}
	})

	definition := workflow.Definition{
		Name: "Remote execution pipeline",
		Tags: []string{"remote"},
		Stages: []workflow.Stage{
			{
				Name: "remote-stage",
				Tags: []string{"remote"},
				Actions: []workflow.Action{
					{Ref: "example.remote.fetch"},
				},
			},
		},
	}
	var assignment workflow.IDAssignment
	remoteWorkflowID, assignment = gostage.MustRegisterWorkflow(definition)
	_ = assignment

	gostage.HandleChild(func(ctx context.Context, child gostage.ChildNode) error {
		diag := child.Diagnostics()
		go func(events <-chan diagnostics.Event) {
			for evt := range events {
				log.Printf("[child][%s] %s err=%v meta=%v", evt.Component, evt.Severity, evt.Err, evt.Metadata)
			}
		}(diag)
		log.Println("[child] connected to parent, awaiting work")
		return child.Run(ctx)
	}, gostage.WithChildPool(gostage.PoolConfig{
		Name:  remotePoolName,
		Tags:  []string{"remote"},
		Slots: 1,
	}))
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	dbPath := filepath.Join(os.TempDir(), sqlitePath)
	if !gostage.IsChildProcess() {
		_ = os.Remove(dbPath)
	}

	node, diagnosticsCh, err := gostage.Run(ctx,
		gostage.WithSQLite(gostage.SQLiteConfig{
			Path:            dbPath,
			WAL:             true,
			ApplyMigrations: true,
		}),
		gostage.WithTelemetrySink(telemetry.NewLoggerSink(log.New(os.Stdout, "[telemetry] ", log.LstdFlags))),
		gostage.WithSpawner(buildSpawnerConfig()),
		gostage.WithPool(gostage.PoolConfig{
			Name:    remotePoolName,
			Tags:    []string{"remote"},
			Slots:   1,
			Spawner: spawnerName,
		}),
	)
	if err != nil {
		log.Fatalf("gostage.Run: %v", err)
	}
	if node == nil {
		return
	}
	orchestrator := node
	defer func() {
		_ = orchestrator.Close()
	}()

	go drainDiagnostics(diagnosticsCh)

	stopTelemetry := orchestrator.StreamTelemetry(ctx, func(evt telemetry.Event) {
		log.Printf("[parent telemetry] kind=%s workflow=%s attempt=%d metadata=%v error=%s",
			evt.Kind, evt.WorkflowID, evt.Attempt, evt.Metadata, evt.Error)
	})
	defer stopTelemetry()

	healthCh := make(chan gostage.HealthEvent, 16)
	stopHealth := orchestrator.StreamHealth(ctx, func(evt gostage.HealthEvent) {
		select {
		case healthCh <- evt:
		default:
		}
	})
	defer stopHealth()

	waitForRemoteHealthy(healthCh)

	runID, err := orchestrator.Submit(ctx,
		gostage.WorkflowRef(remoteWorkflowID),
		gostage.WithTags("remote"),
		gostage.WithInitialStore(map[string]any{
			"request_id": "example-remote",
		}),
	)
	if err != nil {
		log.Fatalf("submit: %v", err)
	}
	log.Printf("workflow submitted: %s", runID)

	result, err := orchestrator.Wait(ctx, runID)
	if err != nil {
		log.Fatalf("wait: %v", err)
	}
	log.Printf("workflow complete: success=%t reason=%s output=%v", result.Success, result.Reason, result.Output)

	if summary, err := orchestrator.State.WorkflowSummary(ctx, runID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			log.Printf("summary not yet available (sql.ErrNoRows); inspect %s after the run", dbPath)
		} else {
			log.Printf("workflow summary unavailable: %v", err)
		}
	} else {
		log.Printf("summary state=%s reason=%s completed=%s", summary.State, summary.TerminationReason, summary.CompletedAt)
	}

	log.Printf("inspect %s for persisted state and telemetry rows after the run", dbPath)
}

func buildSpawnerConfig() gostage.SpawnerConfig {
	return gostage.SpawnerConfig{
		Name:           spawnerName,
		BinaryPath:     gostage.CurrentBinary(),
		ChildType:      remotePoolName,
		Metadata:       map[string]string{"role": "example"},
		MaxRestarts:    1,
		RestartBackoff: 2 * time.Second,
		ShutdownGrace:  3 * time.Second,
	}
}

func drainDiagnostics(ch <-chan gostage.DiagnosticEvent) {
	for evt := range ch {
		log.Printf("[parent diag][%s] %s err=%v meta=%v", evt.Component, evt.Severity, evt.Err, evt.Metadata)
	}
}

func waitForRemoteHealthy(events <-chan gostage.HealthEvent) {
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	for {
		select {
		case evt := <-events:
			if evt.Pool == remotePoolName && string(evt.Status) == "healthy" {
				log.Printf("remote pool %s reported healthy", evt.Pool)
				return
			}
		case <-timer.C:
			log.Fatalf("timed out waiting for remote pool %s to become healthy", remotePoolName)
		}
	}
}
