package gostage

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStress_ConcurrentResumeAndCancel(t *testing.T) {
	ResetTaskRegistry()
	Task("src.task", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			return nil
		}
		return Suspend(ctx, Params{"need": "input"})
	})

	wf, err := NewWorkflow("src-wf").Step("src.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "src.db")
	engine, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}
	runID := result.RunID

	// Race: Resume vs Cancel
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		engine.Resume(context.Background(), runID, nil)
	}()
	go func() {
		defer wg.Done()
		engine.Cancel(context.Background(), runID)
	}()
	wg.Wait()

	// Engine must still be functional
	result2, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("engine broken after concurrent Resume+Cancel: %v", err)
	}
	if result2.Status != Suspended {
		t.Logf("status after stress: %s (acceptable)", result2.Status)
	}
}

func TestStress_ConcurrentRunSyncAndClose(t *testing.T) {
	ResetTaskRegistry()
	Task("scc.task", func(ctx *Ctx) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	wf, err := NewWorkflow("scc-wf").Step("scc.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			engine.RunSync(context.Background(), wf, nil)
		}()
	}

	time.Sleep(5 * time.Millisecond)
	engine.Close()

	wg.Wait()
}

func TestStress_ParallelMutationsFromGoroutines(t *testing.T) {
	ResetTaskRegistry()
	Task("spm.insert", func(ctx *Ctx) error {
		InsertAfter(ctx, "spm.noop")
		return nil
	})
	Task("spm.noop", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("spm-wf").
		Parallel(Step("spm.insert"), Step("spm.insert"), Step("spm.insert")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
}

func TestStress_ConcurrentForEachSameKey(t *testing.T) {
	ResetTaskRegistry()
	var writeCount atomic.Int32
	Task("sfk.task", func(ctx *Ctx) error {
		writeCount.Add(1)
		Set(ctx, "shared", writeCount.Load())
		return nil
	})

	wf, err := NewWorkflow("sfk-wf").
		ForEach("items", Step("sfk.task"), WithConcurrency(8)).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, Params{
		"items": []int{1, 2, 3, 4, 5, 6, 7, 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	val, ok := result.Store["shared"]
	if !ok {
		t.Fatal("expected 'shared' key in store")
	}
	t.Logf("shared = %v (last writer wins)", val)
}
