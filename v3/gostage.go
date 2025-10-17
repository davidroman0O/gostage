package gostage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/davidroman0O/gostage/v3/broker"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/runner"
	"github.com/davidroman0O/gostage/v3/runtime/local"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// Public state aliases so callers don't reach into v3/state.
type (
	WorkflowID          = state.WorkflowID
	StateReader         = state.StateReader
	StateFilter         = state.StateFilter
	WorkflowSummary     = state.WorkflowSummary
	ActionHistoryRecord = state.ActionHistoryRecord
	WorkflowState       = state.WorkflowState
)

const (
	WorkflowPending   = state.WorkflowPending
	WorkflowClaimed   = state.WorkflowClaimed
	WorkflowRunning   = state.WorkflowRunning
	WorkflowCompleted = state.WorkflowCompleted
	WorkflowFailed    = state.WorkflowFailed
	WorkflowCancelled = state.WorkflowCancelled
	WorkflowSkipped   = state.WorkflowSkipped
	WorkflowRemoved   = state.WorkflowRemoved
)

var (
	// ErrNodeClosed signals the node has already been shut down.
	ErrNodeClosed = errors.New("gostage: node closed")
	// ErrNoMatchingPool indicates that no configured pool can accept a submission.
	ErrNoMatchingPool = errors.New("gostage: no matching pool")
	// ErrDuplicatePool indicates two pools share the same resolved name.
	ErrDuplicatePool = errors.New("gostage: duplicate pool name")
	// ErrInvalidPoolConfig indicates pool configuration is incomplete or invalid.
	ErrInvalidPoolConfig = errors.New("gostage: invalid pool config")
	// ErrDuplicateSpawner indicates two spawners share the same name.
	ErrDuplicateSpawner = errors.New("gostage: duplicate spawner name")
	// ErrUnknownSpawner indicates a pool references a spawner that was not configured.
	ErrUnknownSpawner = errors.New("gostage: unknown spawner")
	// ErrInvalidSpawnerConfig indicates a spawner configuration is incomplete or invalid.
	ErrInvalidSpawnerConfig = errors.New("gostage: invalid spawner config")
	// ErrSubmissionRejected indicates the node rejected a submission prior to enqueueing.
	ErrSubmissionRejected = errors.New("gostage: submission rejected")
)

// Node is the public handle returned by Run. It embeds the runtime behaviour
// while exposing the read-only State facade as a field for ergonomic access.
type Node struct {
	*parentNode
	State StateReader
}

// Stats returns scheduler metrics using a background context.
func (n *Node) Stats() (Snapshot, error) {
	if n == nil || n.parentNode == nil {
		return Snapshot{}, fmt.Errorf("gostage: node not initialised")
	}
	return n.parentNode.stats(context.Background())
}

// StatsWithContext collects scheduler metrics using the provided context.
func (n *Node) StatsWithContext(ctx context.Context) (Snapshot, error) {
	if n == nil || n.parentNode == nil {
		return Snapshot{}, fmt.Errorf("gostage: node not initialised")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return n.parentNode.stats(ctx)
}

// State exposes the read-only state facade for compatibility with earlier API.
// ChildNode is the restricted interface handed to child handlers.
type ChildNode interface {
	Run(ctx context.Context) error
	Diagnostics() <-chan diagnostics.Event
	StreamTelemetry(ctx context.Context, fn TelemetryHandler) CancelFunc
	Close() error
}

// DiagnosticEvent aliases the diagnostics package type so callers can drain the stream.
type DiagnosticEvent = diagnostics.Event

// TelemetryHandler handles telemetry events streamed from the node.
type TelemetryHandler func(telemetry.Event)

// HealthEvent represents pool health events exposed by StreamHealth.
type HealthEvent = node.HealthEvent

// HealthHandler consumes HealthEvent notifications.
type HealthHandler func(HealthEvent)

// CancelFunc cancels a previously registered stream.
type CancelFunc func()

// Result captures workflow execution results returned by Wait.
type Result struct {
	WorkflowID      WorkflowID
	Success         bool
	Error           error
	Duration        time.Duration
	Attempt         int
	Output          map[string]any
	DisabledStages  map[string]bool
	DisabledActions map[string]bool
	RemovedStages   map[string]string
	RemovedActions  map[string]string
	CompletedAt     time.Time
}

// Snapshot provides point-in-time scheduler metrics.
type Snapshot struct {
	UpdatedAt  time.Time
	QueueDepth int
	InFlight   int
	Completed  int
	Failed     int
	Cancelled  int
	Pools      []PoolSnapshot
}

// PoolSnapshot reports utilisation metrics for a pool.
type PoolSnapshot struct {
	Name             string
	Slots            int
	Busy             int
	Available        int
	Pending          int
	Healthy          bool
	Status           node.HealthStatus
	LastError        error
	LastErrorAt      time.Time
	LastHealthChange time.Time
}

// Selector matches workflows to pools by tag sets.
type Selector struct {
	All  []string
	Any  []string
	None []string
}

// PoolConfig declares execution capacity owned by a process.
type PoolConfig struct {
	Name     string
	Tags     []string
	Selector Selector
	Slots    int
	Spawner  string
	Metadata map[string]any
}

// TLSFiles references file paths for TLS materials supplied to spawners.
type TLSFiles struct {
	CertPath string
	KeyPath  string
	CAPath   string
}

// SpawnerConfig configures remote capacity provisioning.
type SpawnerConfig struct {
	Name           string
	BinaryPath     string
	Args           []string
	Env            []string
	WorkingDir     string
	AuthToken      string
	Tags           []string
	TLS            TLSFiles
	MaxRestarts    int
	RestartBackoff time.Duration
	ShutdownGrace  time.Duration
}

// DispatcherConfig tunes the scheduler loop.
type DispatcherConfig struct {
	ClaimInterval time.Duration
	MaxInFlight   int
	Jitter        time.Duration
}

// SQLiteConfig controls the embedded SQLite/sqlc backend.
type SQLiteConfig struct {
	Path            string
	WAL             bool
	ApplyMigrations bool
	DB              *sql.DB
}

// Run bootstraps the orchestrator in parent mode.
func Run(ctx context.Context, opts ...Option) (*Node, <-chan DiagnosticEvent, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("gostage: context is required")
	}
	cfg := new(options)
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt.apply(cfg)
	}

	logger := cfg.logger
	if logger == nil {
		logger = telemetry.NoopLogger{}
	}

	queue := cfg.queue
	store := cfg.store
	stateReader := cfg.stateReader
	queueOwned := false
	storeOwned := false

	var (
		sqliteDB    *sql.DB
		sqliteOwned bool
	)

	if sqliteCfg := cfg.sqlite; sqliteCfg != nil {
		var err error
		sqliteDB = sqliteCfg.DB
		if sqliteDB == nil {
			if sqliteCfg.Path == "" {
				return nil, nil, fmt.Errorf("gostage: sqlite path required when no *sql.DB supplied")
			}
			dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_foreign_keys=on", sqliteCfg.Path)
			sqliteDB, err = sql.Open("sqlite", dsn)
			if err != nil {
				return nil, nil, fmt.Errorf("gostage: open sqlite: %w", err)
			}
			sqliteOwned = true
		}
		if _, err := sqliteDB.Exec("PRAGMA foreign_keys=ON"); err != nil {
			if sqliteOwned {
				_ = sqliteDB.Close()
			}
			return nil, nil, fmt.Errorf("gostage: enable foreign keys: %w", err)
		}
		if sqliteCfg.WAL {
			if _, err := sqliteDB.Exec("PRAGMA journal_mode=WAL"); err != nil {
				if sqliteOwned {
					_ = sqliteDB.Close()
				}
				return nil, nil, fmt.Errorf("gostage: enable WAL: %w", err)
			}
		}
		if sqliteCfg.ApplyMigrations {
			if err := state.ApplyMigrations(sqliteDB); err != nil {
				if sqliteOwned {
					_ = sqliteDB.Close()
				}
				return nil, nil, fmt.Errorf("gostage: apply migrations: %w", err)
			}
		}
		if queue == nil {
			queue, err = state.NewSQLiteQueue(sqliteDB)
			if err != nil {
				if sqliteOwned {
					_ = sqliteDB.Close()
				}
				return nil, nil, fmt.Errorf("gostage: sqlite queue: %w", err)
			}
			queueOwned = true
		}
		if store == nil {
			store, err = state.NewSQLiteStore(sqliteDB)
			if err != nil {
				if sqliteOwned {
					_ = sqliteDB.Close()
				}
				return nil, nil, fmt.Errorf("gostage: sqlite store: %w", err)
			}
			storeOwned = true
		}
		if stateReader == nil {
			stateReader, err = state.NewSQLiteStateReader(sqliteDB)
			if err != nil {
				if sqliteOwned {
					_ = sqliteDB.Close()
				}
				return nil, nil, fmt.Errorf("gostage: sqlite state reader: %w", err)
			}
		}
		if sink, err := state.NewSQLiteTelemetrySink(sqliteDB); err != nil {
			if sqliteOwned {
				_ = sqliteDB.Close()
			}
			return nil, nil, fmt.Errorf("gostage: sqlite telemetry sink: %w", err)
		} else {
			cfg.telemetrySinks = append(cfg.telemetrySinks, sink)
		}
	}

	if queue == nil {
		queue = state.NewMemoryQueue()
		queueOwned = true
	}

	if store == nil {
		store = state.NewMemoryStore()
		storeOwned = true
	}

	managerOpts := []state.ManagerOption{}
	if len(cfg.observers) > 0 {
		managerOpts = append(managerOpts, state.WithManagerObservers(cfg.observers...))
	}

	manager, err := state.NewStoreManager(store, managerOpts...)
	if err != nil {
		if sqliteOwned {
			_ = sqliteDB.Close()
		}
		return nil, nil, fmt.Errorf("gostage: state manager: %w", err)
	}
	if stateReader == nil {
		stateReader = state.NewManagerStateReader(manager)
	}

	base := node.New(ctx, cfg.telemetrySinks)
	health := node.NewHealthDispatcher()
	if diagWriter := base.DiagnosticsWriter(); diagWriter != nil {
		_ = health.Subscribe(func(evt node.HealthEvent) {
			if evt.Status == node.HealthDegraded || evt.Status == node.HealthUnavailable {
				diagWriter.Write(diagnostics.Event{
					OccurredAt: time.Now(),
					Component:  "node.health",
					Severity:   diagnostics.SeverityWarning,
					Metadata: map[string]any{
						"pool":   evt.Pool,
						"status": evt.Status,
						"detail": evt.Detail,
					},
				})
			}
		})
	}

	managerWithTelemetry := wrapWithTelemetry(manager, base.TelemetryDispatcher())

	broker := broker.NewLocal(managerWithTelemetry)
	runOpts := []runner.Option{runner.WithDefaultLogger(logger)}
	r := runner.New(local.Factory{}, registry.Default(), broker, runOpts...)

	spawnerIndex, err := indexSpawners(cfg.spawners)
	if err != nil {
		return nil, nil, err
	}

	poolBindings, err := buildPools(cfg.pools, spawnerIndex)
	if err != nil {
		return nil, nil, err
	}

	dispatcher := newDispatcher(ctx, queue, store, managerWithTelemetry, r, base.TelemetryDispatcher(), base.DiagnosticsWriter(), health, logger, cfg.dispatcher.ClaimInterval, cfg.dispatcher.Jitter, cfg.dispatcher.MaxInFlight, cfg.failurePolicy, poolBindings)
	dispatcher.start()

	impl := &parentNode{
		base:       base,
		dispatcher: dispatcher,
		queue:      queue,
		queueOwned: queueOwned,
		store:      store,
		storeOwned: storeOwned,
		pools:      poolBindings,
		logger:     logger,
		sqliteDB:   sqliteDB,
		dbOwned:    sqliteOwned,
	}

	return &Node{
		parentNode: impl,
		State:      stateReader,
	}, base.Diagnostics(), nil
}

func indexSpawners(cfgs []SpawnerConfig) (map[string]SpawnerConfig, error) {
	if len(cfgs) == 0 {
		return map[string]SpawnerConfig{}, nil
	}
	index := make(map[string]SpawnerConfig, len(cfgs))
	for _, cfg := range cfgs {
		if cfg.Name == "" || cfg.BinaryPath == "" {
			return nil, errors.Join(ErrInvalidSpawnerConfig, fmt.Errorf("spawner %q requires name and binary path", cfg.Name))
		}
		if _, exists := index[cfg.Name]; exists {
			return nil, errors.Join(ErrDuplicateSpawner, fmt.Errorf("spawner %q defined multiple times", cfg.Name))
		}
		index[cfg.Name] = cfg
	}
	return index, nil
}

func buildPools(cfgs []PoolConfig, spawners map[string]SpawnerConfig) ([]*poolBinding, error) {
	if len(cfgs) == 0 {
		cfgs = []PoolConfig{{Slots: 1}}
	}
	bindings := make([]*poolBinding, 0, len(cfgs))
	seenNames := make(map[string]struct{}, len(cfgs))
	for idx, cfg := range cfgs {
		name := cfg.Name
		if name == "" {
			name = fmt.Sprintf("pool-%d", idx+1)
		}
		if _, exists := seenNames[name]; exists {
			return nil, errors.Join(ErrDuplicatePool, fmt.Errorf("pool %q defined multiple times", name))
		}
		seenNames[name] = struct{}{}

		slots := cfg.Slots
		if slots <= 0 {
			return nil, errors.Join(ErrInvalidPoolConfig, fmt.Errorf("pool %q must specify Slots > 0", name))
		}

		if cfg.Spawner != "" {
			if _, ok := spawners[cfg.Spawner]; !ok {
				return nil, errors.Join(ErrUnknownSpawner, fmt.Errorf("pool %q references unknown spawner %q", name, cfg.Spawner))
			}
		}

		selector := state.Selector{
			All:  append([]string(nil), cfg.Selector.All...),
			Any:  append([]string(nil), cfg.Selector.Any...),
			None: append([]string(nil), cfg.Selector.None...),
		}
		if len(selector.All) == 0 && len(cfg.Tags) > 0 {
			selector.All = append([]string(nil), cfg.Tags...)
		}
		pool := pools.NewLocal(name, selector, slots)
		bindings = append(bindings, &poolBinding{pool: pool})
	}
	return bindings, nil
}

// ChildHandler represents the entrypoint executed when the binary re-enters as a child.
type ChildHandler func(context.Context, ChildNode) error

var registeredChildHandlers []ChildHandler

// HandleChild registers a child-mode handler. Child execution wiring arrives in later phases.
func HandleChild(handler ChildHandler, opts ...ChildOption) {
	if handler == nil {
		return
	}
	registeredChildHandlers = append(registeredChildHandlers, handler)
}
