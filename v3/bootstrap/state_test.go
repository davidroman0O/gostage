package bootstrap_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
	_ "modernc.org/sqlite"
)

func TestPrepareStateMemoryFallback(t *testing.T) {
	cfg := bootstrap.NewConfig()
	res, err := bootstrap.PrepareState(cfg, telemetry.NoopLogger{})
	if err != nil {
		t.Fatalf("PrepareState returned error: %v", err)
	}
	if res.Queue == nil || res.Store == nil {
		t.Fatalf("expected memory queue/store")
	}
	if !res.QueueOwned || !res.StoreOwned {
		t.Fatalf("expected queue/store to be owned when using memory fallback")
	}
	if res.SQLiteDB != nil {
		t.Fatalf("expected no sqlite DB")
	}
}

func TestPrepareStateRespectsInjectedComponents(t *testing.T) {
	cfg := bootstrap.NewConfig()
	cfg.Queue = state.NewMemoryQueue()
	cfg.Store = state.NewMemoryStore()

	res, err := bootstrap.PrepareState(cfg, telemetry.NoopLogger{})
	if err != nil {
		t.Fatalf("PrepareState returned error: %v", err)
	}
	if res.Queue != cfg.Queue || res.Store != cfg.Store {
		t.Fatalf("expected existing queue/store to be reused")
	}
	if res.QueueOwned || res.StoreOwned {
		t.Fatalf("expected ownership flags to remain false for injected components")
	}
}

func TestPrepareStateExistingSQLiteSkipsPragmas(t *testing.T) {
	cfg := bootstrap.NewConfig()

	tempDir := t.TempDir()
	dsn := fmt.Sprintf("file:%s/test.db?mode=rwc", tempDir)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatalf("set foreign_keys: %v", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=4321"); err != nil {
		t.Fatalf("set busy_timeout: %v", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=DELETE"); err != nil {
		t.Fatalf("set journal_mode: %v", err)
	}

	cfg.SQLite = &bootstrap.SQLiteConfig{
		DB:              db,
		ApplyMigrations: false,
		DisableWAL:      true,
	}

	res, err := bootstrap.PrepareState(cfg, telemetry.NoopLogger{})
	if err != nil {
		t.Fatalf("PrepareState returned error: %v", err)
	}
	if res.SQLiteOwned {
		t.Fatalf("expected sqlite ownership flag to remain false for injected DB")
	}

	assertPragmaInt(t, db, "foreign_keys", 0)
	assertPragmaInt(t, db, "busy_timeout", 4321)
	assertPragmaText(t, db, "journal_mode", "delete")
}

func assertPragmaInt(t *testing.T, db *sql.DB, pragma string, want int) {
	t.Helper()
	row := db.QueryRow("PRAGMA " + pragma)
	var got int
	if err := row.Scan(&got); err != nil {
		t.Fatalf("scan pragma %s: %v", pragma, err)
	}
	if got != want {
		t.Fatalf("pragma %s mismatch: got %d want %d", pragma, got, want)
	}
}

func assertPragmaText(t *testing.T, db *sql.DB, pragma, want string) {
	t.Helper()
	row := db.QueryRow("PRAGMA " + pragma)
	var got string
	if err := row.Scan(&got); err != nil {
		t.Fatalf("scan pragma %s: %v", pragma, err)
	}
	if got != want {
		t.Fatalf("pragma %s mismatch: got %s want %s", pragma, got, want)
	}
}
