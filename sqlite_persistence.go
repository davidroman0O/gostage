package gostage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// sqlitePersistence implements Persistence using SQLite.
type sqlitePersistence struct {
	db *sql.DB
}

func newSQLitePersistence(path string) (*sqlitePersistence, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// SQLite serializes writes at the file level; a single connection prevents
	// SQLITE_BUSY errors under concurrent flush calls.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	// Enable WAL mode for better concurrent read performance.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set WAL mode: %w", err)
	}

	p := &sqlitePersistence{db: db}
	if err := p.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return p, nil
}

// migrations is the ordered list of schema migrations.
// Each function receives a transaction and applies one version's changes.
// New migrations are appended to the end; existing entries must never be modified.
var migrations = []func(tx *sql.Tx) error{
	migrateV1,
}

func (p *sqlitePersistence) migrate() error {
	// Ensure the version tracking table exists.
	if _, err := p.db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("create schema_version table: %w", err)
	}

	// Read current schema version.
	var current int
	row := p.db.QueryRow(`SELECT version FROM schema_version LIMIT 1`)
	if err := row.Scan(&current); err == sql.ErrNoRows {
		current = 0
	} else if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	// Apply pending migrations.
	for i := current; i < len(migrations); i++ {
		tx, err := p.db.Begin()
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", i+1, err)
		}

		if err := migrations[i](tx); err != nil {
			tx.Rollback()
			return fmt.Errorf("migration %d: %w", i+1, err)
		}

		// Update version within the same transaction.
		if current == 0 && i == 0 {
			if _, err := tx.Exec(`INSERT INTO schema_version (version) VALUES (?)`, i+1); err != nil {
				tx.Rollback()
				return fmt.Errorf("insert schema version: %w", err)
			}
		} else {
			if _, err := tx.Exec(`UPDATE schema_version SET version = ?`, i+1); err != nil {
				tx.Rollback()
				return fmt.Errorf("update schema version: %w", err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", i+1, err)
		}
	}

	return nil
}

// migrateV1 creates the initial schema (runs, checkpoints, run_state tables).
func migrateV1(tx *sql.Tx) error {
	const schema = `
	CREATE TABLE IF NOT EXISTS runs (
		run_id       TEXT PRIMARY KEY,
		workflow_id  TEXT NOT NULL,
		status       TEXT NOT NULL,
		current_step TEXT NOT NULL DEFAULT '',
		step_states  TEXT NOT NULL DEFAULT '{}',
		bail_reason  TEXT NOT NULL DEFAULT '',
		suspend_data TEXT NOT NULL DEFAULT '{}',
		wake_at      TEXT NOT NULL DEFAULT '',
		mutations    TEXT NOT NULL DEFAULT '[]',
		created_at   TEXT NOT NULL,
		updated_at   TEXT NOT NULL
	);

	CREATE INDEX IF NOT EXISTS idx_runs_workflow_id ON runs(workflow_id);
	CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);

	CREATE TABLE IF NOT EXISTS checkpoints (
		run_id     TEXT PRIMARY KEY,
		store_data BLOB NOT NULL,
		FOREIGN KEY (run_id) REFERENCES runs(run_id)
	);

	CREATE TABLE IF NOT EXISTS run_state (
		run_id    TEXT NOT NULL,
		key       TEXT NOT NULL,
		value     BLOB NOT NULL,
		type_name TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (run_id, key)
	);
	`
	_, err := tx.Exec(schema)
	return err
}

func (p *sqlitePersistence) SaveRun(ctx context.Context, run *RunState) error {
	stepStatesJSON, err := json.Marshal(run.StepStates)
	if err != nil {
		return fmt.Errorf("marshal step states: %w", err)
	}

	suspendDataJSON, err := json.Marshal(run.SuspendData)
	if err != nil {
		return fmt.Errorf("marshal suspend data: %w", err)
	}

	mutationsJSON, err := json.Marshal(run.Mutations)
	if err != nil {
		return fmt.Errorf("marshal mutations: %w", err)
	}

	wakeAtStr := ""
	if !run.WakeAt.IsZero() {
		wakeAtStr = run.WakeAt.Format(time.RFC3339Nano)
	}

	const query = `
	INSERT INTO runs (run_id, workflow_id, status, current_step, step_states, bail_reason, suspend_data, wake_at, mutations, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(run_id) DO UPDATE SET
		status       = excluded.status,
		current_step = excluded.current_step,
		step_states  = excluded.step_states,
		bail_reason  = excluded.bail_reason,
		suspend_data = excluded.suspend_data,
		wake_at      = excluded.wake_at,
		mutations    = excluded.mutations,
		updated_at   = excluded.updated_at
	`

	_, err = p.db.ExecContext(ctx, query,
		string(run.RunID),
		run.WorkflowID,
		string(run.Status),
		run.CurrentStep,
		string(stepStatesJSON),
		run.BailReason,
		string(suspendDataJSON),
		wakeAtStr,
		string(mutationsJSON),
		run.CreatedAt.Format(time.RFC3339Nano),
		run.UpdatedAt.Format(time.RFC3339Nano),
	)
	return err
}

func (p *sqlitePersistence) LoadRun(ctx context.Context, runID RunID) (*RunState, error) {
	const query = `
	SELECT run_id, workflow_id, status, current_step, step_states, bail_reason, suspend_data, wake_at, mutations, created_at, updated_at
	FROM runs WHERE run_id = ?
	`

	var (
		run             RunState
		rid, st         string
		stepStatesJSON  string
		suspendDataJSON string
		wakeAtStr       string
		mutationsJSON   string
		createdAtStr    string
		updatedAtStr    string
	)

	err := p.db.QueryRowContext(ctx, query, string(runID)).Scan(
		&rid,
		&run.WorkflowID,
		&st,
		&run.CurrentStep,
		&stepStatesJSON,
		&run.BailReason,
		&suspendDataJSON,
		&wakeAtStr,
		&mutationsJSON,
		&createdAtStr,
		&updatedAtStr,
	)
	if err == sql.ErrNoRows {
		return nil, &RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return nil, fmt.Errorf("query run: %w", err)
	}

	run.RunID = RunID(rid)
	run.Status = Status(st)

	if err := json.Unmarshal([]byte(stepStatesJSON), &run.StepStates); err != nil {
		return nil, fmt.Errorf("unmarshal step states: %w", err)
	}

	if suspendDataJSON != "{}" && suspendDataJSON != "" {
		if err := json.Unmarshal([]byte(suspendDataJSON), &run.SuspendData); err != nil {
			return nil, fmt.Errorf("unmarshal suspend data: %w", err)
		}
	}

	if wakeAtStr != "" {
		if run.WakeAt, err = time.Parse(time.RFC3339Nano, wakeAtStr); err != nil {
			return nil, fmt.Errorf("parse wake_at: %w", err)
		}
	}

	if mutationsJSON != "[]" && mutationsJSON != "" {
		if err := json.Unmarshal([]byte(mutationsJSON), &run.Mutations); err != nil {
			return nil, fmt.Errorf("unmarshal mutations: %w", err)
		}
	}

	if run.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAtStr); err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	if run.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAtStr); err != nil {
		return nil, fmt.Errorf("parse updated_at: %w", err)
	}

	return &run, nil
}

func (p *sqlitePersistence) UpdateStepStatus(ctx context.Context, runID RunID, stepID string, status Status) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	var statesJSON string
	err = tx.QueryRowContext(ctx,
		`SELECT step_states FROM runs WHERE run_id = ?`, string(runID)).Scan(&statesJSON)
	if err == sql.ErrNoRows {
		return &RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return fmt.Errorf("load step states: %w", err)
	}

	var states map[string]Status
	if statesJSON == "" || statesJSON == "{}" {
		states = make(map[string]Status)
	} else {
		if err := json.Unmarshal([]byte(statesJSON), &states); err != nil {
			return fmt.Errorf("unmarshal step states: %w", err)
		}
	}
	states[stepID] = status

	newJSON, err := json.Marshal(states)
	if err != nil {
		return fmt.Errorf("marshal step states: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`UPDATE runs SET step_states = ?, updated_at = ? WHERE run_id = ?`,
		string(newJSON), time.Now().Format(time.RFC3339Nano), string(runID))
	if err != nil {
		return fmt.Errorf("update step states: %w", err)
	}

	return tx.Commit()
}

func (p *sqlitePersistence) SaveState(ctx context.Context, runID RunID, entries map[string]StateEntry) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO run_state (run_id, key, value, type_name)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(run_id, key) DO UPDATE SET
			value     = excluded.value,
			type_name = excluded.type_name
	`)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for k, e := range entries {
		if _, err := stmt.ExecContext(ctx, string(runID), k, e.Value, e.TypeName); err != nil {
			return fmt.Errorf("upsert key %q: %w", k, err)
		}
	}

	return tx.Commit()
}

func (p *sqlitePersistence) LoadState(ctx context.Context, runID RunID) (map[string]StateEntry, error) {
	const query = `SELECT key, value, type_name FROM run_state WHERE run_id = ?`

	rows, err := p.db.QueryContext(ctx, query, string(runID))
	if err != nil {
		return nil, fmt.Errorf("query state: %w", err)
	}
	defer rows.Close()

	entries := make(map[string]StateEntry)
	for rows.Next() {
		var key, typeName string
		var value []byte
		if err := rows.Scan(&key, &value, &typeName); err != nil {
			return nil, fmt.Errorf("scan state row: %w", err)
		}
		entries[key] = StateEntry{Value: value, TypeName: typeName}
	}

	return entries, rows.Err()
}

func (p *sqlitePersistence) DeleteState(ctx context.Context, runID RunID) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM run_state WHERE run_id = ?`, string(runID))
	return err
}

func (p *sqlitePersistence) DeleteStateKey(ctx context.Context, runID RunID, key string) error {
	_, err := p.db.ExecContext(ctx,
		`DELETE FROM run_state WHERE run_id = ? AND key = ?`,
		string(runID), key)
	return err
}

func (p *sqlitePersistence) ListRuns(ctx context.Context, filter RunFilter) ([]*RunState, error) {
	query := "SELECT run_id, workflow_id, status, current_step, step_states, bail_reason, suspend_data, wake_at, mutations, created_at, updated_at FROM runs WHERE 1=1"
	var args []any

	if filter.WorkflowID != "" {
		query += " AND workflow_id = ?"
		args = append(args, filter.WorkflowID)
	}
	if filter.Status != "" {
		query += " AND status = ?"
		args = append(args, string(filter.Status))
	}

	query += " ORDER BY created_at DESC"

	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}

	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()

	var results []*RunState
	for rows.Next() {
		var (
			run             RunState
			rid, st         string
			stepStatesJSON  string
			suspendDataJSON string
			wakeAtStr       string
			mutationsJSON   string
			createdAtStr    string
			updatedAtStr    string
		)

		if err := rows.Scan(
			&rid, &run.WorkflowID, &st, &run.CurrentStep,
			&stepStatesJSON, &run.BailReason, &suspendDataJSON,
			&wakeAtStr, &mutationsJSON,
			&createdAtStr, &updatedAtStr,
		); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}

		run.RunID = RunID(rid)
		run.Status = Status(st)

		if err := json.Unmarshal([]byte(stepStatesJSON), &run.StepStates); err != nil {
			return nil, fmt.Errorf("unmarshal step states: %w", err)
		}

		if suspendDataJSON != "{}" && suspendDataJSON != "" {
			if err := json.Unmarshal([]byte(suspendDataJSON), &run.SuspendData); err != nil {
				return nil, fmt.Errorf("unmarshal suspend data: %w", err)
			}
		}

		if wakeAtStr != "" {
			if run.WakeAt, err = time.Parse(time.RFC3339Nano, wakeAtStr); err != nil {
				return nil, fmt.Errorf("parse wake_at: %w", err)
			}
		}

		if mutationsJSON != "[]" && mutationsJSON != "" {
			if err := json.Unmarshal([]byte(mutationsJSON), &run.Mutations); err != nil {
				return nil, fmt.Errorf("unmarshal mutations: %w", err)
			}
		}

		if run.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAtStr); err != nil {
			return nil, fmt.Errorf("parse created_at: %w", err)
		}
		if run.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAtStr); err != nil {
			return nil, fmt.Errorf("parse updated_at: %w", err)
		}

		results = append(results, &run)
	}

	return results, rows.Err()
}

func (p *sqlitePersistence) Close() error {
	return p.db.Close()
}
