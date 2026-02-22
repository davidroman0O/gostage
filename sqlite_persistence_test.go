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
		Status:     Running,
		StepStates: map[string]Status{"step1": Completed},
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
	if loaded.Status != Running {
		t.Fatalf("expected running, got %s", loaded.Status)
	}
	if loaded.StepStates["step1"] != Completed {
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
		Status:     Running,
		StepStates: make(map[string]Status),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := p.SaveRun(ctx, run); err != nil {
		t.Fatalf("SaveRun failed: %v", err)
	}

	// Update status
	run.Status = Completed
	run.UpdatedAt = time.Now().Truncate(time.Microsecond)
	if err := p.SaveRun(ctx, run); err != nil {
		t.Fatalf("SaveRun update failed: %v", err)
	}

	loaded, err := p.LoadRun(ctx, "run-002")
	if err != nil {
		t.Fatalf("LoadRun failed: %v", err)
	}
	if loaded.Status != Completed {
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
		Status:     Running,
		StepStates: make(map[string]Status),
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := p.SaveRun(ctx, run); err != nil {
		t.Fatalf("SaveRun failed: %v", err)
	}

	if err := p.UpdateStepStatus(ctx, "run-003", "step-a", Completed); err != nil {
		t.Fatalf("UpdateStepStatus failed: %v", err)
	}

	loaded, err := p.LoadRun(ctx, "run-003")
	if err != nil {
		t.Fatalf("LoadRun failed: %v", err)
	}
	if loaded.StepStates["step-a"] != Completed {
		t.Fatalf("expected step-a completed, got %s", loaded.StepStates["step-a"])
	}
}

func TestSQLite_SaveLoadState(t *testing.T) {
	p := newTestSQLite(t)
	ctx := context.Background()

	entries := map[string]StateEntry{
		"greeting": {Value: []byte(`"hello"`), TypeName: "string"},
		"count":    {Value: []byte(`42`), TypeName: "int"},
	}

	if err := p.SaveState(ctx, "run-004", entries); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	loaded, err := p.LoadState(ctx, "run-004")
	if err != nil {
		t.Fatalf("LoadState failed: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(loaded))
	}
	if string(loaded["greeting"].Value) != `"hello"` {
		t.Fatalf("expected greeting='hello', got %s", loaded["greeting"].Value)
	}
	if loaded["greeting"].TypeName != "string" {
		t.Fatalf("expected typeName 'string', got %s", loaded["greeting"].TypeName)
	}
	if string(loaded["count"].Value) != `42` {
		t.Fatalf("expected count=42, got %s", loaded["count"].Value)
	}
	if loaded["count"].TypeName != "int" {
		t.Fatalf("expected typeName 'int', got %s", loaded["count"].TypeName)
	}

	// Overwrite one key, add a new one
	entries2 := map[string]StateEntry{
		"greeting": {Value: []byte(`"updated"`), TypeName: "string"},
		"active":   {Value: []byte(`true`), TypeName: "bool"},
	}
	if err := p.SaveState(ctx, "run-004", entries2); err != nil {
		t.Fatalf("SaveState overwrite failed: %v", err)
	}

	loaded2, err := p.LoadState(ctx, "run-004")
	if err != nil {
		t.Fatalf("LoadState after overwrite failed: %v", err)
	}
	if len(loaded2) != 3 {
		t.Fatalf("expected 3 entries after upsert, got %d", len(loaded2))
	}
	if string(loaded2["greeting"].Value) != `"updated"` {
		t.Fatalf("expected greeting='updated', got %s", loaded2["greeting"].Value)
	}
}

func TestSQLite_DeleteState(t *testing.T) {
	p := newTestSQLite(t)
	ctx := context.Background()

	entries := map[string]StateEntry{
		"key1": {Value: []byte(`"value1"`), TypeName: "string"},
		"key2": {Value: []byte(`"value2"`), TypeName: "string"},
	}
	if err := p.SaveState(ctx, "run-del", entries); err != nil {
		t.Fatalf("SaveState failed: %v", err)
	}

	if err := p.DeleteState(ctx, "run-del"); err != nil {
		t.Fatalf("DeleteState failed: %v", err)
	}

	loaded, err := p.LoadState(ctx, "run-del")
	if err != nil {
		t.Fatalf("LoadState after delete failed: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("expected 0 entries after delete, got %d", len(loaded))
	}
}

func TestSQLite_ListRuns(t *testing.T) {
	p := newTestSQLite(t)
	ctx := context.Background()

	now := time.Now().Truncate(time.Microsecond)

	runs := []*RunState{
		{RunID: "run-a", WorkflowID: "wf-1", Status: Completed, StepStates: make(map[string]Status), CreatedAt: now, UpdatedAt: now},
		{RunID: "run-b", WorkflowID: "wf-1", Status: Failed, StepStates: make(map[string]Status), CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(time.Second)},
		{RunID: "run-c", WorkflowID: "wf-2", Status: Completed, StepStates: make(map[string]Status), CreatedAt: now.Add(2 * time.Second), UpdatedAt: now.Add(2 * time.Second)},
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
	completed, err := p.ListRuns(ctx, RunFilter{Status: Completed})
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
		Status:     Bailed,
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
		Status:      Suspended,
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

