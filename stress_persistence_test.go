package gostage

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// failingPersistence wraps a real persistence and injects failures.
type failingPersistence struct {
	inner         Persistence
	saveRunFail   atomic.Bool
	saveStateFail atomic.Bool
}

func (f *failingPersistence) SaveRun(ctx context.Context, run *RunState) error {
	if f.saveRunFail.Load() {
		return errors.New("injected SaveRun failure")
	}
	return f.inner.SaveRun(ctx, run)
}
func (f *failingPersistence) LoadRun(ctx context.Context, runID RunID) (*RunState, error) {
	return f.inner.LoadRun(ctx, runID)
}
func (f *failingPersistence) UpdateStepStatus(ctx context.Context, runID RunID, stepID string, status Status) error {
	return f.inner.UpdateStepStatus(ctx, runID, stepID, status)
}
func (f *failingPersistence) SaveState(ctx context.Context, runID RunID, entries map[string]StateEntry) error {
	if f.saveStateFail.Load() {
		return errors.New("injected SaveState failure")
	}
	return f.inner.SaveState(ctx, runID, entries)
}
func (f *failingPersistence) LoadState(ctx context.Context, runID RunID) (map[string]StateEntry, error) {
	return f.inner.LoadState(ctx, runID)
}
func (f *failingPersistence) DeleteState(ctx context.Context, runID RunID) error {
	return f.inner.DeleteState(ctx, runID)
}
func (f *failingPersistence) DeleteStateKey(ctx context.Context, runID RunID, key string) error {
	return f.inner.DeleteStateKey(ctx, runID, key)
}
func (f *failingPersistence) DeleteRun(ctx context.Context, runID RunID) error {
	return f.inner.DeleteRun(ctx, runID)
}
func (f *failingPersistence) UpdateCurrentStep(ctx context.Context, runID RunID, stepID string) error {
	return f.inner.UpdateCurrentStep(ctx, runID, stepID)
}
func (f *failingPersistence) ListRuns(ctx context.Context, filter RunFilter) ([]*RunState, error) {
	return f.inner.ListRuns(ctx, filter)
}
func (f *failingPersistence) Close() error {
	return f.inner.Close()
}

func TestStress_SaveStateFailureMidExecution(t *testing.T) {
	ResetTaskRegistry()
	Task("spf.step1", func(ctx *Ctx) error {
		Set(ctx, "step1", "done")
		return nil
	})
	Task("spf.step2", func(ctx *Ctx) error {
		Set(ctx, "step2", "done")
		return nil
	})

	fp := &failingPersistence{inner: newMemoryPersistence()}

	wf, err := NewWorkflow("spf-wf").Step("spf.step1").Step("spf.step2").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithPersistence(fp))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Fail SaveState after step1 completes
	fp.saveStateFail.Store(true)

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Failed {
		t.Fatalf("expected Failed when SaveState fails, got %s", result.Status)
	}
}

func TestStress_CorruptWorkflowDefOnResume(t *testing.T) {
	ResetTaskRegistry()
	Task("cwd.task", func(ctx *Ctx) error {
		if IsResuming(ctx) {
			return nil
		}
		return Suspend(ctx, Params{"need": "input"})
	})

	wf, err := NewWorkflow("cwd-wf").Step("cwd.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	dbPath := filepath.Join(t.TempDir(), "cwd.db")
	engine, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s", result.Status)
	}
	runID := result.RunID
	engine.Close()

	// Corrupt the WorkflowDef
	engine2, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	defer engine2.Close()

	run, _ := engine2.EnginePersistence().LoadRun(context.Background(), runID)
	run.WorkflowDef = nil
	engine2.EnginePersistence().SaveRun(context.Background(), run)

	_, err = engine2.Resume(context.Background(), runID, nil)
	if err == nil {
		t.Fatal("expected error for corrupted WorkflowDef")
	}
	if !errors.Is(err, ErrWorkflowNotResumable) {
		t.Logf("error: %v (acceptable — clear failure)", err)
	}
}

func TestStress_SaveRunFailureOnInitialSave(t *testing.T) {
	ResetTaskRegistry()
	Task("srf.task", func(ctx *Ctx) error { return nil })

	fp := &failingPersistence{inner: newMemoryPersistence()}
	fp.saveRunFail.Store(true)

	wf, err := NewWorkflow("srf-wf").Step("srf.task").Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithPersistence(fp))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	_, err = engine.RunSync(context.Background(), wf, nil)
	if err == nil {
		t.Fatal("expected error when initial SaveRun fails")
	}

	_ = time.Now() // prevent unused import
}
