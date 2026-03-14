package gostage

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStress_GCDuringRunCompletion(t *testing.T) {
	ResetTaskRegistry()
	Task("sgc.task", func(ctx *Ctx) error {
		time.Sleep(50 * time.Millisecond)
		Set(ctx, "done", true)
		return nil
	})

	wf, err := NewWorkflow("sgc-wf").Step("sgc.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "sgc.db")
	engine, err := New(
		WithSQLite(dbPath),
		WithRunGC(0, 50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, runErr := engine.RunSync(context.Background(), wf, nil)
			if runErr != nil {
				t.Errorf("RunSync: %v", runErr)
				return
			}
			if result.Status != Completed {
				t.Errorf("expected Completed, got %s", result.Status)
			}
		}()
	}
	wg.Wait()
}

func TestStress_DeleteRunDuringExecution(t *testing.T) {
	ResetTaskRegistry()
	Task("sdr.task", func(ctx *Ctx) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})

	wf, err := NewWorkflow("sdr-wf").Step("sdr.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "sdr.db")
	engine, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	runID, err := engine.Run(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := engine.DeleteRun(ctx, runID); err != nil {
		t.Fatalf("DeleteRun: %v", err)
	}

	_, err = engine.GetRun(context.Background(), runID)
	if err == nil {
		t.Fatal("expected run to be deleted")
	}
}

func TestStress_CacheEvictionDuringWake(t *testing.T) {
	ResetTaskRegistry()
	Task("sce.before", func(ctx *Ctx) error {
		Set(ctx, "phase", "before")
		return nil
	})
	Task("sce.after", func(ctx *Ctx) error {
		Set(ctx, "phase", "after")
		return nil
	})

	dbPath := filepath.Join(t.TempDir(), "sce.db")
	engine, err := New(
		WithSQLite(dbPath),
		WithCacheSize(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	wf1, _ := NewWorkflow("sce-wf1").Step("sce.before").Sleep(100 * time.Millisecond).Step("sce.after").Commit()
	wf2, _ := NewWorkflow("sce-wf2").Step("sce.before").Commit()

	runID1, err := engine.Run(context.Background(), wf1, nil)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	result2, err := engine.RunSync(context.Background(), wf2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result2.Status != Completed {
		t.Fatalf("wf2: expected Completed, got %s", result2.Status)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, getErr := engine.GetRun(context.Background(), runID1)
		if getErr != nil {
			if errors.Is(getErr, ErrRunNotFound) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			t.Fatalf("GetRun: %v", getErr)
		}
		if run.Status == Completed {
			return
		}
		if run.Status == Failed {
			t.Fatalf("wf1 failed — cache eviction killed sleeping workflow")
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("wf1 did not complete within 5 seconds")
}

func TestStress_RapidOpenClose(t *testing.T) {
	ResetTaskRegistry()
	Task("roc.task", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("roc-wf").Step("roc.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		engine, err := New()
		if err != nil {
			t.Fatalf("iteration %d: New: %v", i, err)
		}
		result, err := engine.RunSync(context.Background(), wf, nil)
		if err != nil {
			t.Fatalf("iteration %d: RunSync: %v", i, err)
		}
		if result.Status != Completed {
			t.Fatalf("iteration %d: expected Completed, got %s", i, result.Status)
		}
		engine.Close()
	}
}
