package gostage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestE2E_SleepAcrossRestart(t *testing.T) {
	ResetTaskRegistry()

	Task("sar.before", func(ctx *Ctx) error {
		Set(ctx, "phase", "before")
		return nil
	})
	Task("sar.after", func(ctx *Ctx) error {
		Set(ctx, "phase", "after")
		Set(ctx, "restart_survived", true)
		return nil
	})

	wf, err := NewWorkflow("sar-wf").
		Step("sar.before").
		Sleep(100 * time.Millisecond).
		Step("sar.after").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "sar.db")

	// Engine 1: Start workflow, it sleeps
	engine1, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}

	runID, err := engine1.Run(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	run, err := engine1.GetRun(context.Background(), runID)
	if err == nil && run.Status == Sleeping {
		t.Log("confirmed: run is sleeping")
	}

	// "Crash"
	engine1.Close()

	// Engine 2: Restart with auto-recover
	engine2, err := New(WithSQLite(dbPath), WithAutoRecover())
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, getErr := engine2.GetRun(context.Background(), runID)
		if getErr != nil {
			if errors.Is(getErr, ErrRunNotFound) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			t.Fatalf("GetRun: %v", getErr)
		}
		if run.Status == Completed {
			t.Log("sleeping workflow survived restart and completed")
			return
		}
		if run.Status == Failed {
			t.Fatalf("run failed after restart")
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("sleeping workflow did not complete within 5 seconds after restart")
}
