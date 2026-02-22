package gostage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestSQLite(t *testing.T) *sqlitePersistence {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	p, err := newSQLitePersistence(path)
	if err != nil {
		t.Fatalf("failed to create sqlite persistence: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func TestSQLite_SaveAndLoadRun(t *testing.T) {
	p := newTestSQLite(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)
	run := &RunState{
		RunID:      "run-001",
		WorkflowID: "wf-hello",
		Status:     StatusRunningRun,
		StepStates: map[string]Status{"step1": StatusCompletedRun},
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := p.SaveRun(ctx, run); err != nil {
		t.Fatalf("SaveRun failed: %v", err)
	}

	loaded, err := p.LoadRun(ctx, "run-001")
	if err != nil {
		t.Fatalf("LoadRun failed: %v", err)
	}

	if loaded.RunID != "run-001" {
		t.Fatalf("expected run-001, got %s", loaded.RunID)
	}
	if loaded.WorkflowID != "wf-hello" {
		t.Fatalf("expected wf-hello, got %s", loaded.WorkflowID)
	}
	if loaded.Status != StatusRunningRun {
		t.Fatalf("expected running, got %s", loaded.Status)
	}
	if loaded.StepStates["step1"] != StatusCompletedRun {
		t.Fatalf("expected step1 completed, got %s", loaded.StepStates["step1"])
	}
}

func TestSQLite_SaveRunUpdate(t *testing.T) {
	p := newTestSQLite(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)
	run := &RunState{
		RunID:      "run-002",
		WorkflowID: "wf-test",
		Status:     StatusRunningRun,
		StepStates: make(map[string]Status),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := p.SaveRun(ctx, run); err != nil {
		t.Fatalf("SaveRun failed: %v", err)
	}

	// Update status
	run.Status = StatusCompletedRun
	run.UpdatedAt = time.Now().Truncate(time.Microsecond)
	if err := p.SaveRun(ctx, run); err != nil {
		t.Fatalf("SaveRun update failed: %v", err)
	}

	loaded, err := p.LoadRun(ctx, "run-002")
	if err != nil {
		t.Fatalf("LoadRun failed: %v", err)
	}
	if loaded.Status != StatusCompletedRun {
		t.Fatalf("expected completed, got %s", loaded.Status)
	}
}

func TestSQLite_LoadRunNotFound(t *testing.T) {
	p := newTestSQLite(t)
	ctx := context.Background()

	_, err := p.LoadRun(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent run")
	}
	if _, ok := err.(*RunNotFoundError); !ok {
		t.Fatalf("expected RunNotFoundError, got %T: %v", err, err)
	}
}

func TestSQLite_UpdateStepStatus(t *testing.T) {
	p := newTestSQLite(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)
	run := &RunState{
		RunID:      "run-003",
		WorkflowID: "wf-steps",
		Status:     StatusRunningRun,
		StepStates: make(map[string]Status),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := p.SaveRun(ctx, run); err != nil {
		t.Fatalf("SaveRun failed: %v", err)
	}

	if err := p.UpdateStepStatus(ctx, "run-003", "step-a", StatusCompletedRun); err != nil {
		t.Fatalf("UpdateStepStatus failed: %v", err)
	}

	loaded, err := p.LoadRun(ctx, "run-003")
	if err != nil {
		t.Fatalf("LoadRun failed: %v", err)
	}
	if loaded.StepStates["step-a"] != StatusCompletedRun {
		t.Fatalf("expected step-a completed, got %s", loaded.StepStates["step-a"])
	}
}

func TestSQLite_Checkpoint(t *testing.T) {
	p := newTestSQLite(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)
	run := &RunState{
		RunID:      "run-004",
		WorkflowID: "wf-ckpt",
		Status:     StatusRunningRun,
		StepStates: make(map[string]Status),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := p.SaveRun(ctx, run); err != nil {
		t.Fatalf("SaveRun failed: %v", err)
	}

	storeData := []byte(`{"greeting":"hello","count":42}`)
	if err := p.SaveCheckpoint(ctx, "run-004", storeData); err != nil {
		t.Fatalf("SaveCheckpoint failed: %v", err)
	}

	loaded, err := p.LoadCheckpoint(ctx, "run-004")
	if err != nil {
		t.Fatalf("LoadCheckpoint failed: %v", err)
	}
	if string(loaded) != string(storeData) {
		t.Fatalf("expected %s, got %s", storeData, loaded)
	}

	// Overwrite checkpoint
	storeData2 := []byte(`{"greeting":"updated"}`)
	if err := p.SaveCheckpoint(ctx, "run-004", storeData2); err != nil {
		t.Fatalf("SaveCheckpoint overwrite failed: %v", err)
	}

	loaded2, err := p.LoadCheckpoint(ctx, "run-004")
	if err != nil {
		t.Fatalf("LoadCheckpoint after overwrite failed: %v", err)
	}
	if string(loaded2) != string(storeData2) {
		t.Fatalf("expected %s, got %s", storeData2, loaded2)
	}
}

func TestSQLite_ListRuns(t *testing.T) {
	p := newTestSQLite(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)

	runs := []*RunState{
		{RunID: "run-a", WorkflowID: "wf-1", Status: StatusCompletedRun, StepStates: make(map[string]Status), CreatedAt: now, UpdatedAt: now},
		{RunID: "run-b", WorkflowID: "wf-1", Status: StatusFailedRun, StepStates: make(map[string]Status), CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second)},
		{RunID: "run-c", WorkflowID: "wf-2", Status: StatusCompletedRun, StepStates: make(map[string]Status), CreatedAt: now.Add(2 * time.Second), UpdatedAt: now.Add(2 * time.Second)},
	}

	for _, r := range runs {
		if err := p.SaveRun(ctx, r); err != nil {
			t.Fatalf("SaveRun failed: %v", err)
		}
	}

	// List all
	all, err := p.ListRuns(ctx, RunFilter{})
	if err != nil {
		t.Fatalf("ListRuns all failed: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(all))
	}

	// Filter by workflow
	wf1, err := p.ListRuns(ctx, RunFilter{WorkflowID: "wf-1"})
	if err != nil {
		t.Fatalf("ListRuns wf-1 failed: %v", err)
	}
	if len(wf1) != 2 {
		t.Fatalf("expected 2 runs for wf-1, got %d", len(wf1))
	}

	// Filter by status
	completed, err := p.ListRuns(ctx, RunFilter{Status: StatusCompletedRun})
	if err != nil {
		t.Fatalf("ListRuns completed failed: %v", err)
	}
	if len(completed) != 2 {
		t.Fatalf("expected 2 completed runs, got %d", len(completed))
	}

	// Limit
	limited, err := p.ListRuns(ctx, RunFilter{Limit: 1})
	if err != nil {
		t.Fatalf("ListRuns limit failed: %v", err)
	}
	if len(limited) != 1 {
		t.Fatalf("expected 1 run with limit, got %d", len(limited))
	}
}

func TestSQLite_BailAndSuspendData(t *testing.T) {
	p := newTestSQLite(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)

	// Bail run
	bail := &RunState{
		RunID:      "run-bail",
		WorkflowID: "wf-bail",
		Status:     StatusBailed,
		BailReason: "Must be 18+",
		StepStates: make(map[string]Status),
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := p.SaveRun(ctx, bail); err != nil {
		t.Fatalf("SaveRun bail failed: %v", err)
	}

	loadedBail, err := p.LoadRun(ctx, "run-bail")
	if err != nil {
		t.Fatalf("LoadRun bail failed: %v", err)
	}
	if loadedBail.BailReason != "Must be 18+" {
		t.Fatalf("expected 'Must be 18+', got %q", loadedBail.BailReason)
	}

	// Suspend run
	suspend := &RunState{
		RunID:       "run-suspend",
		WorkflowID:  "wf-suspend",
		Status:      StatusSuspended,
		SuspendData: map[string]any{"reason": "needs approval", "approver": "admin"},
		StepStates:  make(map[string]Status),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := p.SaveRun(ctx, suspend); err != nil {
		t.Fatalf("SaveRun suspend failed: %v", err)
	}

	loadedSuspend, err := p.LoadRun(ctx, "run-suspend")
	if err != nil {
		t.Fatalf("LoadRun suspend failed: %v", err)
	}
	if loadedSuspend.SuspendData["reason"] != "needs approval" {
		t.Fatalf("expected reason 'needs approval', got %v", loadedSuspend.SuspendData["reason"])
	}
	if loadedSuspend.SuspendData["approver"] != "admin" {
		t.Fatalf("expected approver 'admin', got %v", loadedSuspend.SuspendData["approver"])
	}
}

func TestSQLite_Persistence_FileExists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	p, err := newSQLitePersistence(path)
	if err != nil {
		t.Fatalf("failed to create sqlite persistence: %v", err)
	}
	p.Close()

	// File should exist on disk
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("expected database file to exist on disk")
	}
}

func TestEngine_WithSQLite(t *testing.T) {
	ResetTaskRegistry()

	Task("sqlite.task", func(ctx *Ctx) error {
		Set(ctx, "result", "persisted")
		return nil
	})

	wf := NewWorkflowBuilder("sqlite-test").
		Step("sqlite.task").
		Commit()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "engine.db")

	engine, err := New(WithSQLite(dbPath))
	if err != nil {
		t.Fatalf("failed to create engine with SQLite: %v", err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, P{"input": "test"})
	if err != nil {
		t.Fatalf("RunSync failed: %v", err)
	}

	if result.Status != StatusCompletedRun {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	// Verify the run persisted to SQLite
	run, err := engine.persistence.LoadRun(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("LoadRun from SQLite failed: %v", err)
	}
	if run.Status != StatusCompletedRun {
		t.Fatalf("expected Completed in SQLite, got %s", run.Status)
	}

	// Verify checkpoint was saved
	checkpoint, err := engine.persistence.LoadCheckpoint(context.Background(), result.RunID)
	if err != nil {
		t.Fatalf("LoadCheckpoint from SQLite failed: %v", err)
	}
	if len(checkpoint) == 0 {
		t.Fatal("expected non-empty checkpoint data")
	}
}
