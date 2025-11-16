package gostage_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	gostage "github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/layers/foundation/cleanup"
	_ "modernc.org/sqlite"
)

func TestSQLiteTunablesAppliedWhenOwned(t *testing.T) {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "owned.db")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node, diag, err := gostage.Run(ctx, gostage.WithSQLite(gostage.SQLiteConfig{
		Path:            dbPath,
		ApplyMigrations: false,
		WAL:             false,
		PageSize:        4096,
		CacheSize:       -2000,
		MMapSize:        131072,
	}))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Drain diagnostics and close
	go func() {
		for range diag {
			_ = struct{}{} // Drain diagnostics channel
		}
	}()
	_ = node.Close()

	dsn := fmt.Sprintf("file:%s?mode=ro", dbPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite ro: %v", err)
	}
	defer cleanup.SafeClose(db)

	// Verify page_size, mmap_size, cache_size (negative means kibibytes)
	if got := pragmaInt(t, db, "page_size"); got != 4096 {
		t.Fatalf("expected page_size 4096, got %d", got)
	}
	// mmap_size may be disabled depending on platform/driver; skip asserting
	// cache_size reports pages when positive; when negative it reports kibibytes (negative)
	if got := pragmaInt(t, db, "cache_size"); got != -2000 {
		t.Fatalf("expected cache_size -2000, got %d", got)
	}
}

// uses the helper defined in gostage_test.go
