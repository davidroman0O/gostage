// Example 07: Sleep Recovery After Crash
//
// Demonstrates crash recovery for sleeping workflows. A workflow with a
// short Sleep step is started asynchronously. The engine is closed to
// simulate a crash. A new engine is created with WithAutoRecover, which
// detects the sleeping run and resumes it once the sleep timer expires.
//
// Uses SQLite persistence because sleep recovery requires durable state.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	gs.Task("before-sleep", func(ctx *gs.Ctx) error {
		fmt.Println("[before] Running pre-sleep step")
		gs.Set(ctx, "phase", "before")
		return nil
	})

	gs.Task("after-sleep", func(ctx *gs.Ctx) error {
		fmt.Println("[after]  Running post-sleep step")
		gs.Set(ctx, "phase", "after")
		gs.Set(ctx, "recovered", true)
		return nil
	})

	wf, err := gs.NewWorkflow("sleep-recover").
		Step("before-sleep").
		Sleep(500 * time.Millisecond).
		Step("after-sleep").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	dbPath := filepath.Join(os.TempDir(), "gostage-ex07.db")
	defer os.Remove(dbPath)

	// Engine 1: start workflow, let it reach sleep, then "crash"
	engine1, err := gs.New(gs.WithSQLite(dbPath))
	if err != nil {
		log.Fatal(err)
	}

	runID, err := engine1.Run(context.Background(), wf, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Started run %s\n", runID)

	// Wait briefly for the run to enter Sleeping status
	time.Sleep(100 * time.Millisecond)
	run, _ := engine1.GetRun(context.Background(), runID)
	fmt.Printf("Status before crash: %s\n\n", run.Status)

	// Simulate crash
	engine1.Close()
	fmt.Println("--- Engine crashed ---\n")

	// Engine 2: restart with auto-recovery
	engine2, err := gs.New(gs.WithSQLite(dbPath), gs.WithAutoRecover())
	if err != nil {
		log.Fatal(err)
	}
	defer engine2.Close()

	// Poll until the recovered workflow completes
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := engine2.GetRun(context.Background(), runID)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		fmt.Printf("Polling... status=%s\n", run.Status)
		if run.Status == gs.Completed {
			fmt.Println("\nSleeping workflow survived crash and completed.")
			return
		}
		if run.Status == gs.Failed {
			log.Fatal("Run failed after recovery")
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatal("Workflow did not complete within 5 seconds")
}
