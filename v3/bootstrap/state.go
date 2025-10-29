package bootstrap

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// StateInitResult contains persistence components prepared during bootstrap.
type StateInitResult struct {
	Queue          state.Queue
	QueueOwned     bool
	Store          state.Store
	StoreOwned     bool
	StateReader    state.StateReader
	SQLiteDB       *sql.DB
	SQLiteOwned    bool
	TelemetrySinks []telemetry.Sink
}

// PrepareState resolves queue/store/state reader and optional SQLite backing according to the provided configuration.
func PrepareState(cfg *Config, logger telemetry.Logger) (*StateInitResult, error) {
	res := &StateInitResult{}
	queue := cfg.Queue
	store := cfg.Store
	stateReader := cfg.StateReader

	var (
		sqliteDB    *sql.DB
		sqliteOwned bool
	)

	if sqliteCfg := cfg.SQLite; sqliteCfg != nil {
		var err error
		busyTimeout := sqliteCfg.BusyTimeout
		if busyTimeout <= 0 {
			busyTimeout = 15 * time.Second
		}
		busyTimeoutMS := int(busyTimeout / time.Millisecond)
		if busyTimeoutMS <= 0 {
			busyTimeoutMS = 1
		}
		enableWAL := !sqliteCfg.DisableWAL
		if sqliteCfg.WAL {
			enableWAL = true
		}
		sqliteDB = sqliteCfg.DB
		if sqliteDB == nil {
			if sqliteCfg.Path == "" {
				return nil, fmt.Errorf("gostage: sqlite path required when no *sql.DB supplied")
			}
			dsn := fmt.Sprintf("file:%s?mode=rwc&_busy_timeout=%d&_foreign_keys=on", sqliteCfg.Path, busyTimeoutMS)
			if enableWAL {
				dsn += "&_journal_mode=WAL"
			}
			sqliteDB, err = sql.Open("sqlite", dsn)
			if err != nil {
				return nil, fmt.Errorf("gostage: open sqlite: %w", err)
			}
			sqliteOwned = true
		}
		if sqliteOwned {
			if _, err := sqliteDB.Exec("PRAGMA foreign_keys=ON"); err != nil {
				_ = sqliteDB.Close()
				return nil, fmt.Errorf("gostage: enable foreign keys: %w", err)
			}
			if _, err := sqliteDB.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeoutMS)); err != nil {
				_ = sqliteDB.Close()
				return nil, fmt.Errorf("gostage: set busy timeout: %w", err)
			}
			if enableWAL {
				if _, err := sqliteDB.Exec("PRAGMA journal_mode=WAL"); err != nil {
					_ = sqliteDB.Close()
					return nil, fmt.Errorf("gostage: enable WAL: %w", err)
				}
			}
		}
		if sqliteCfg.ApplyMigrations {
			if err := state.ApplyMigrations(sqliteDB); err != nil {
				if sqliteOwned {
					_ = sqliteDB.Close()
				}
				return nil, fmt.Errorf("gostage: apply migrations: %w", err)
			}
		}
		if queue == nil {
			queue, err = state.NewSQLiteQueue(sqliteDB)
			if err != nil {
				if sqliteOwned {
					_ = sqliteDB.Close()
				}
				return nil, fmt.Errorf("gostage: sqlite queue: %w", err)
			}
			res.QueueOwned = true
		}
		if store == nil {
			store, err = state.NewSQLiteStore(sqliteDB)
			if err != nil {
				if sqliteOwned {
					_ = sqliteDB.Close()
				}
				return nil, fmt.Errorf("gostage: sqlite store: %w", err)
			}
			res.StoreOwned = true
		}
		if stateReader == nil {
			stateReader, err = state.NewSQLiteStateReader(sqliteDB)
			if err != nil {
				if sqliteOwned {
					_ = sqliteDB.Close()
				}
				return nil, fmt.Errorf("gostage: sqlite state reader: %w", err)
			}
		}
		if sink, err := state.NewSQLiteTelemetrySink(sqliteDB); err != nil {
			if sqliteOwned {
				_ = sqliteDB.Close()
			}
			return nil, fmt.Errorf("gostage: sqlite telemetry sink: %w", err)
		} else {
			cfg.TelemetrySinks = append(cfg.TelemetrySinks, sink)
			res.TelemetrySinks = append(res.TelemetrySinks, sink)
		}
	}

	if queue == nil {
		queue = state.NewMemoryQueue()
		res.QueueOwned = true
	}
	if store == nil {
		store = state.NewMemoryStore()
		res.StoreOwned = true
	}
	if stateReader == nil {
		// state reader provided later once manager is created; leave nil for now
	}

	res.Queue = queue
	res.Store = store
	res.StateReader = stateReader
	res.SQLiteDB = sqliteDB
	res.SQLiteOwned = sqliteOwned
	return res, nil
}
