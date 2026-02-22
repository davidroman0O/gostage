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

// WithSQLite configures the engine to use SQLite for persistence.
//
//	engine, _ := gostage.New(gostage.WithSQLite("app.db"))
//	engine, _ := gostage.New(gostage.WithSQLite(":memory:"))
func WithSQLite(path string) EngineOption {
	return func(e *Engine) error {
		p, err := newSQLitePersistence(path)
		if err != nil {
			return fmt.Errorf("sqlite persistence: %w", err)
		}
		e.persistence = p
		return nil
	}
}

func newSQLitePersistence(path string) (*sqlitePersistence, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

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

func (p *sqlitePersistence) migrate() error {
	const schema = `
	CREATE TABLE IF NOT EXISTS runs (
		run_id       TEXT PRIMARY KEY,
		workflow_id  TEXT NOT NULL,
		status       TEXT NOT NULL,
		current_step TEXT NOT NULL DEFAULT '',
		step_states  TEXT NOT NULL DEFAULT '{}',
		bail_reason  TEXT NOT NULL DEFAULT '',
		suspend_data TEXT NOT NULL DEFAULT '{}',
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
	`
	_, err := p.db.Exec(schema)
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

	const query = `
	INSERT INTO runs (run_id, workflow_id, status, current_step, step_states, bail_reason, suspend_data, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(run_id) DO UPDATE SET
		status       = excluded.status,
		current_step = excluded.current_step,
		step_states  = excluded.step_states,
		bail_reason  = excluded.bail_reason,
		suspend_data = excluded.suspend_data,
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
		run.CreatedAt.Format(time.RFC3339Nano),
		run.UpdatedAt.Format(time.RFC3339Nano),
	)
	return err
}

func (p *sqlitePersistence) LoadRun(ctx context.Context, runID RunID) (*RunState, error) {
	const query = `
	SELECT run_id, workflow_id, status, current_step, step_states, bail_reason, suspend_data, created_at, updated_at
	FROM runs WHERE run_id = ?
	`

	var (
		run             RunState
		rid, st         string
		stepStatesJSON  string
		suspendDataJSON string
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

	if run.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAtStr); err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	if run.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAtStr); err != nil {
		return nil, fmt.Errorf("parse updated_at: %w", err)
	}

	return &run, nil
}

func (p *sqlitePersistence) UpdateStepStatus(ctx context.Context, runID RunID, stepID string, status Status) error {
	run, err := p.LoadRun(ctx, runID)
	if err != nil {
		return err
	}

	if run.StepStates == nil {
		run.StepStates = make(map[string]Status)
	}
	run.StepStates[stepID] = status
	run.UpdatedAt = time.Now()

	return p.SaveRun(ctx, run)
}

func (p *sqlitePersistence) SaveCheckpoint(ctx context.Context, runID RunID, storeData []byte) error {
	const query = `
	INSERT INTO checkpoints (run_id, store_data)
	VALUES (?, ?)
	ON CONFLICT(run_id) DO UPDATE SET
		store_data = excluded.store_data
	`
	_, err := p.db.ExecContext(ctx, query, string(runID), storeData)
	return err
}

func (p *sqlitePersistence) LoadCheckpoint(ctx context.Context, runID RunID) ([]byte, error) {
	const query = `SELECT store_data FROM checkpoints WHERE run_id = ?`

	var data []byte
	err := p.db.QueryRowContext(ctx, query, string(runID)).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, &RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return nil, fmt.Errorf("query checkpoint: %w", err)
	}
	return data, nil
}

func (p *sqlitePersistence) ListRuns(ctx context.Context, filter RunFilter) ([]*RunState, error) {
	query := "SELECT run_id, workflow_id, status, current_step, step_states, bail_reason, suspend_data, created_at, updated_at FROM runs WHERE 1=1"
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
			createdAtStr    string
			updatedAtStr    string
		)

		if err := rows.Scan(
			&rid, &run.WorkflowID, &st, &run.CurrentStep,
			&stepStatesJSON, &run.BailReason, &suspendDataJSON,
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
