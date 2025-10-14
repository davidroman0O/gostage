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

// ErrNodeClosed signals the node has already been shut down.
var ErrNodeClosed = errors.New("gostage: node closed")

// Node is the public handle returned by Run.
type Node interface {
	Submit(ctx context.Context, ref WorkflowReference, opts ...SubmitOption) (state.WorkflowID, error)
	Wait(ctx context.Context, id state.WorkflowID) (Result, error)
	Cancel(ctx context.Context, id state.WorkflowID) error
	Stats(ctx context.Context) (Snapshot, error)
	StreamTelemetry(ctx context.Context, fn TelemetryHandler) CancelFunc
	StreamHealth(ctx context.Context, fn HealthHandler) CancelFunc
	State() state.StateReader
	Close() error
}

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
	WorkflowID      state.WorkflowID
	Success         bool
	Error           error
	Duration        time.Duration
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
	Pools      []PoolSnapshot
}

// PoolSnapshot reports utilisation metrics for a pool.
type PoolSnapshot struct {
	Name      string
	Slots     int
	Busy      int
	Available int
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
func Run(ctx context.Context, opts ...Option) (Node, <-chan DiagnosticEvent, error) {
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
	queueOwned := false
	if queue == nil {
		queue = state.NewMemoryQueue()
		queueOwned = true
	}

	store := cfg.store
	storeOwned := false
	if store == nil {
		store = state.NewMemoryStore()
		storeOwned = true
	}

	manager, err := state.NewStoreManager(store)
	if err != nil {
		return nil, nil, fmt.Errorf("gostage: state manager: %w", err)
	}

	base := node.New(ctx, cfg.telemetrySinks)
	health := node.NewHealthDispatcher()

	broker := broker.NewLocal(manager)
	runOpts := []runner.Option{runner.WithDefaultLogger(logger)}
	r := runner.New(local.Factory{}, registry.Default(), broker, runOpts...)

	poolBindings := buildPools(cfg.pools)
	if len(poolBindings) == 0 {
		defaultPool := pools.NewLocal("default", state.Selector{All: []string{}}, 1)
		poolBindings = []*poolBinding{{pool: defaultPool}}
	}

	dispatcher := newDispatcher(ctx, queue, store, r, base.TelemetryDispatcher(), base.DiagnosticsWriter(), health, logger, cfg.dispatcher.ClaimInterval, cfg.dispatcher.Jitter, poolBindings)
	dispatcher.start()

	node := &parentNode{
		base:        base,
		dispatcher:  dispatcher,
		queue:       queue,
		queueOwned:  queueOwned,
		store:       store,
		storeOwned:  storeOwned,
		stateReader: cfg.stateReader,
		pools:       poolBindings,
		logger:      logger,
	}

	return node, base.Diagnostics(), nil
}

func buildPools(cfgs []PoolConfig) []*poolBinding {
	bindings := make([]*poolBinding, 0, len(cfgs))
	for idx, cfg := range cfgs {
		name := cfg.Name
		if name == "" {
			name = fmt.Sprintf("pool-%d", idx+1)
		}
		selector := state.Selector{
			All:  append([]string(nil), cfg.Selector.All...),
			Any:  append([]string(nil), cfg.Selector.Any...),
			None: append([]string(nil), cfg.Selector.None...),
		}
		if len(selector.All) == 0 && len(cfg.Tags) > 0 {
			selector.All = append([]string(nil), cfg.Tags...)
		}
		slots := cfg.Slots
		if slots <= 0 {
			slots = 1
		}
		pool := pools.NewLocal(name, selector, slots)
		bindings = append(bindings, &poolBinding{pool: pool})
	}
	return bindings
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
