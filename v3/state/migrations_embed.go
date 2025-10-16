package state

import (
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
)

// embeddedMigrations contains the SQL migration files required to bootstrap the
// SQLite schema used by the queue, store, and telemetry sink.
//
//go:embed migrations/*.up.sql
var embeddedMigrations embed.FS

// ApplyMigrations executes the embedded SQLite migrations against the provided
// database connection. It is safe to call multiple times; migrations are
// idempotent through CREATE IF NOT EXISTS statements.
func ApplyMigrations(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("state: apply migrations requires database")
	}

	files, err := fs.Glob(embeddedMigrations, "migrations/*.up.sql")
	if err != nil {
		return fmt.Errorf("state: list migrations: %w", err)
	}
	sort.Strings(files)

	for _, file := range files {
		content, err := embeddedMigrations.ReadFile(file)
		if err != nil {
			return fmt.Errorf("state: read migration %s: %w", file, err)
		}
		if _, err := db.Exec(string(content)); err != nil {
			return fmt.Errorf("state: execute migration %s: %w", file, err)
		}
	}
	return nil
}
