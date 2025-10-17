package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"sync"
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

	dbPath := exampleDBPath("cancel-retry.db")
	db, err := openSQLite(dbPath)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	cancelWorkflowID, retryWorkflowID := registerWorkflows()

	retryPolicy := gostage.FailurePolicyFunc(func(_ context.Context, info gostage.FailureContext) gostage.FailureDecision {
		if info.Attempt < 2 {
			log.Printf("failure policy: retrying workflow %s attempt %d due to %v", info.WorkflowID, info.Attempt, info.Err)
			return gostage.FailureDecisionRetry
		}
		return gostage.FailureDecisionAck
	})

	node, diag, err := gostage.Run(ctx,
		gostage.WithSQLite(gostage.SQLiteConfig{
			DB:              db,
			ApplyMigrations: true,
		}),
		gostage.WithTelemetrySink(telemetry.NewLoggerSink(log.Default())),
		gostage.WithFailurePolicy(retryPolicy),
		gostage.WithPool(gostage.PoolConfig{
			Name:  "control",
			Tags:  []string{"control"},
			Slots: 1,
		}),
	)
	if err != nil {
		log.Fatalf("gostage.Run: %v", err)
	}
	defer node.Close()

	go drainDiagnostics(diag)

	cancelRunID, err := node.Submit(ctx,
		gostage.WorkflowRef(cancelWorkflowID),
		gostage.WithTags("control"),
		gostage.WithInitialStore(map[string]any{"request": "cancel-me"}),
	)
	if err != nil {
		log.Fatalf("submit cancel workflow: %v", err)
	}

	retryRunID, err := node.Submit(ctx,
		gostage.WorkflowRef(retryWorkflowID),
		gostage.WithTags("control"),
	)
	if err != nil {
		log.Fatalf("submit retry workflow: %v", err)
	}

	go func() {
		time.Sleep(300 * time.Millisecond)
		log.Printf("requesting cancellation of %s", cancelRunID)
		if err := node.Cancel(ctx, cancelRunID); err != nil {
			log.Printf("cancel error: %v", err)
		}
	}()

	cancelRes, cancelErr := waitWithTimeout(ctx, node, cancelRunID, 8*time.Second)
	if cancelErr != nil {
		log.Printf("cancel workflow wait error: %v", cancelErr)
	} else {
		log.Printf("cancel workflow result success=%t error=%v", cancelRes.Success, cancelRes.Error)
	}

	retryRes, retryErr := waitWithTimeout(ctx, node, retryRunID, 12*time.Second)
	if retryErr != nil {
		log.Fatalf("retry workflow wait error: %v", retryErr)
	}
	log.Printf("retry workflow success=%t attempts=%d error=%v", retryRes.Success, retryRes.Attempt, retryRes.Error)

	printStats(ctx, node)
	log.Printf("SQLite database written to %s", dbPath)
}

func registerWorkflows() (string, string) {
	cancelWorkflow := workflow.Definition{
		Name: "Cancellation Example",
		Tags: []string{"control"},
		Stages: []workflow.Stage{{
			Name: "Long running stage",
			Actions: []workflow.Action{{
				Ref: "example.sleep",
			}},
		}},
	}

	retryWorkflow := workflow.Definition{
		Name: "Retry Example",
		Tags: []string{"control"},
		Stages: []workflow.Stage{{
			Name: "Unstable action",
			Actions: []workflow.Action{{
				Ref: "example.unstable",
			}},
		}},
	}

	var once sync.Once

	gostage.MustRegisterAction("example.sleep", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			cancelled := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
				case <-time.After(5 * time.Second):
				}
				close(cancelled)
			}()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-cancelled:
				return nil
			}
		}
	}, gostage.WithActionDescription("Sleep until cancelled"))

	gostage.MustRegisterAction("example.unstable", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			var shouldFail bool
			once.Do(func() { shouldFail = true })
			if shouldFail {
				return errors.New("transient failure")
			}
			_ = storepkg.Put(ctx, "status", "recovered")
			return nil
		}
	}, gostage.WithActionDescription("Fails once then succeeds"))

	cancelID, cancelAssign := gostage.MustRegisterWorkflow(cancelWorkflow)
	retryID, retryAssign := gostage.MustRegisterWorkflow(retryWorkflow)

	log.Printf("registered cancel workflow %q with %d stages", cancelID, len(cancelAssign.Stages))
	log.Printf("registered retry workflow %q with %d stages", retryID, len(retryAssign.Stages))
	return cancelID, retryID
}

func waitWithTimeout(ctx context.Context, node *gostage.Node, id gostage.WorkflowID, timeout time.Duration) (gostage.Result, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return node.Wait(waitCtx, id)
}

func printStats(ctx context.Context, node *gostage.Node) {
	stats, err := node.Stats()
	if err != nil {
		log.Printf("stats error: %v", err)
		return
	}
	log.Printf("stats: queue_depth=%d in_flight=%d completed=%d failed=%d cancelled=%d",
		stats.QueueDepth, stats.InFlight, stats.Completed, stats.Failed, stats.Cancelled)
	for _, pool := range stats.Pools {
		log.Printf("  pool %s healthy=%t busy=%d/%d pending=%d status=%s detail=%s",
			pool.Name, pool.Healthy, pool.Busy, pool.Slots, pool.Pending, pool.Status, pool.LastError)
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
