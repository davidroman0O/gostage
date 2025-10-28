package state

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

// openTestDB returns an in-memory SQLite database with migrations applied.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:state_test?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := ApplyMigrations(db); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return db
}
