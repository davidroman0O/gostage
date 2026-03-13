package gostage

import (
	"context"
	"testing"
)

// === ListRuns pagination with Offset (Issue 14) ===

func TestListRuns_Pagination(t *testing.T) {
	ResetTaskRegistry()

	Task("page.noop", func(ctx *Ctx) error { return nil })

	engine, err := New(WithSQLite(t.TempDir() + "/page.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	wf, err := NewWorkflow("page-wf").Step("page.noop").Commit()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Create 7 runs sequentially (order matters for created_at DESC).
	for i := 0; i < 7; i++ {
		_, runErr := engine.RunSync(ctx, wf, nil)
		if runErr != nil {
			t.Fatal(runErr)
		}
	}

	// Page 1: runs 1-3 (newest)
	page1, err := engine.ListRuns(ctx, RunFilter{Limit: 3, Offset: 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 3 {
		t.Fatalf("page 1: expected 3, got %d", len(page1))
	}

	// Page 2: runs 4-6
	page2, err := engine.ListRuns(ctx, RunFilter{Limit: 3, Offset: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 3 {
		t.Fatalf("page 2: expected 3, got %d", len(page2))
	}

	// Page 3: run 7 (oldest)
	page3, err := engine.ListRuns(ctx, RunFilter{Limit: 3, Offset: 6})
	if err != nil {
		t.Fatal(err)
	}
	if len(page3) != 1 {
		t.Fatalf("page 3: expected 1, got %d", len(page3))
	}

	// All run IDs must be distinct across pages.
	seen := make(map[RunID]bool)
	for _, pages := range [][]*RunState{page1, page2, page3} {
		for _, run := range pages {
			if seen[run.RunID] {
				t.Fatalf("run %s appeared in multiple pages", run.RunID)
			}
			seen[run.RunID] = true
		}
	}
	if len(seen) != 7 {
		t.Fatalf("expected 7 unique runs across all pages, got %d", len(seen))
	}
}

// TestListRuns_OffsetWithoutLimit verifies that ListRuns does not produce a
// SQL syntax error when Offset > 0 and Limit == 0 (meaning "no limit").
func TestListRuns_OffsetWithoutLimit(t *testing.T) {
	ResetTaskRegistry()

	Task("olim.noop", func(ctx *Ctx) error { return nil })

	engine, err := New(WithSQLite(t.TempDir() + "/olim.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	wf, err := NewWorkflow("olim-wf").Step("olim.noop").Commit()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Create 5 runs.
	for i := 0; i < 5; i++ {
		if _, err := engine.RunSync(ctx, wf, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Offset 2 without a Limit must not produce a SQL syntax error.
	// SQLite requires LIMIT before OFFSET; the fix adds LIMIT -1 automatically.
	runs, err := engine.ListRuns(ctx, RunFilter{Offset: 2})
	if err != nil {
		t.Fatalf("ListRuns with Offset but no Limit failed: %v", err)
	}
	// Should return 3 runs (5 total - 2 skipped).
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs after offset 2, got %d", len(runs))
	}

	// Also verify memory persistence handles the same case consistently.
	engineMem, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engineMem.Close()

	for i := 0; i < 5; i++ {
		if _, err := engineMem.RunSync(ctx, wf, nil); err != nil {
			t.Fatal(err)
		}
	}

	runsMem, err := engineMem.ListRuns(ctx, RunFilter{Offset: 2})
	if err != nil {
		t.Fatalf("memoryPersistence ListRuns with Offset but no Limit failed: %v", err)
	}
	if len(runsMem) != 3 {
		t.Fatalf("expected 3 runs from memory after offset 2, got %d", len(runsMem))
	}
}
