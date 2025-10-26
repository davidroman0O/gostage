package gostage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/davidroman0O/gostage/v3/broker"
	"github.com/davidroman0O/gostage/v3/child"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/internal/locks"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/runner"
	"github.com/davidroman0O/gostage/v3/runtime/local"
	"github.com/davidroman0O/gostage/v3/spawner"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// Public state aliases so callers don't reach into v3/state.
type (
	WorkflowID                = state.WorkflowID
	StateReader               = state.StateReader
	StateFilter               = state.StateFilter
	WorkflowSummary           = state.WorkflowSummary
	ActionHistoryRecord       = state.ActionHistoryRecord
	WorkflowState             = state.WorkflowState
	TelemetryDispatcherConfig = node.TelemetryDispatcherConfig
	TelemetryStats            = node.TelemetryStats
	OverflowStrategy          = node.OverflowStrategy
	TerminationReason         = state.TerminationReason
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

	OverflowStrategyDropOldest = node.OverflowStrategyDropOldest
	OverflowStrategyBlock      = node.OverflowStrategyBlock
	OverflowStrategyFailFast   = node.OverflowStrategyFailFast

	TerminationReasonUnknown      = state.TerminationReasonUnknown
	TerminationReasonSuccess      = state.TerminationReasonSuccess
	TerminationReasonFailure      = state.TerminationReasonFailure
	TerminationReasonUserCancel   = state.TerminationReasonUserCancel
	TerminationReasonPolicyCancel = state.TerminationReasonPolicyCancel
	TerminationReasonTimeout      = state.TerminationReasonTimeout
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

var (
	childDetect  = child.Detect
	newChildNode = child.NewNode
)

type childHandlerRegistration struct {
	childType string
	handler   ChildHandler
	options   childOptions
}

type childNodeWrapper struct {
	node *child.Node
}

func (w childNodeWrapper) Run(ctx context.Context) error {
	return w.node.Run(ctx)
}

func (w childNodeWrapper) Diagnostics() <-chan diagnostics.Event {
	return w.node.Diagnostics()
}

func (w childNodeWrapper) StreamTelemetry(ctx context.Context, fn TelemetryHandler) CancelFunc {
	cancel := w.node.StreamTelemetry(ctx, func(evt telemetry.Event) {
		if fn != nil {
			fn(evt)
		}
	})
	return CancelFunc(cancel)
}

func (w childNodeWrapper) Close() error {
	return w.node.Close()
}

var (
	childHandlersMu     locks.RWMutex
	defaultChildHandler *childHandlerRegistration
	namedChildHandlers  = make(map[string]*childHandlerRegistration)
)

// IsChildProcess reports whether the current process is running in child mode.
// It mirrors the detection path used by Run so callers can guard parent-only setup.
func IsChildProcess() bool {
	_, childMode, _ := childDetect(os.Args[1:], os.Getenv)
	return childMode
}

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
	Reason          TerminationReason
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
	Metadata       map[string]string
	ChildType      string
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
	DisableWAL      bool
	BusyTimeout     time.Duration
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

	childCfg, childMode, err := childDetect(os.Args[1:], os.Getenv)
	if err != nil {
		return nil, nil, fmt.Errorf("gostage: detect child mode: %w", err)
	}
	if childMode {
		return runChildProcess(ctx, childCfg)
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
				return nil, nil, fmt.Errorf("gostage: sqlite path required when no *sql.DB supplied")
			}
			dsn := fmt.Sprintf("file:%s?mode=rwc&_busy_timeout=%d&_foreign_keys=on", sqliteCfg.Path, busyTimeoutMS)
			if enableWAL {
				dsn += "&_journal_mode=WAL"
			}
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
		if _, err := sqliteDB.Exec(fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeoutMS)); err != nil {
			if sqliteOwned {
				_ = sqliteDB.Close()
			}
			return nil, nil, fmt.Errorf("gostage: set busy timeout: %w", err)
		}
		if enableWAL {
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

	base := node.New(ctx, cfg.telemetrySinks, cfg.telemetryCfg)
	health := node.NewHealthDispatcher()
	if diagWriter := base.DiagnosticsWriter(); diagWriter != nil {
		writer := node.NewDiagnosticsHealthWriter(diagWriter)
		_ = health.Subscribe(writer.Handle)
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
	remoteCoord, err := newRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), base.DiagnosticsWriter(), health, logger, poolBindings)
	if err != nil {
		if sqliteOwned {
			_ = sqliteDB.Close()
		}
		return nil, nil, err
	}
	dispatcher.start()

	impl := &parentNode{
		base:       base,
		dispatcher: dispatcher,
		remote:     remoteCoord,
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

func indexSpawners(cfgs []SpawnerConfig) (map[string]*spawnerBinding, error) {
	if len(cfgs) == 0 {
		return map[string]*spawnerBinding{}, nil
	}
	index := make(map[string]*spawnerBinding, len(cfgs))
	for _, cfg := range cfgs {
		if cfg.Name == "" || cfg.BinaryPath == "" {
			return nil, errors.Join(ErrInvalidSpawnerConfig, fmt.Errorf("spawner %q requires name and binary path", cfg.Name))
		}
		if _, exists := index[cfg.Name]; exists {
			return nil, errors.Join(ErrDuplicateSpawner, fmt.Errorf("spawner %q defined multiple times", cfg.Name))
		}
		copyCfg := copySpawnerConfig(cfg)
		procCfg := spawner.Config{
			BinaryPath:     copyCfg.BinaryPath,
			Args:           append([]string(nil), copyCfg.Args...),
			Env:            append([]string(nil), copyCfg.Env...),
			WorkingDir:     copyCfg.WorkingDir,
			MaxRestarts:    copyCfg.MaxRestarts,
			RestartBackoff: copyCfg.RestartBackoff,
			ShutdownGrace:  copyCfg.ShutdownGrace,
		}
		binding := &spawnerBinding{
			name:    copyCfg.Name,
			cfg:     copyCfg,
			process: spawner.NewProcessSpawner(procCfg),
		}
		index[copyCfg.Name] = binding
	}
	return index, nil
}

func buildPools(cfgs []PoolConfig, spawners map[string]*spawnerBinding) ([]*poolBinding, error) {
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

		selector := state.Selector{
			All:  append([]string(nil), cfg.Selector.All...),
			Any:  append([]string(nil), cfg.Selector.Any...),
			None: append([]string(nil), cfg.Selector.None...),
		}
		if len(selector.All) == 0 && len(cfg.Tags) > 0 {
			selector.All = append([]string(nil), cfg.Tags...)
		}
		pool := pools.NewLocal(name, selector, slots)
		binding := &poolBinding{pool: pool}
		if cfg.Spawner != "" {
			spBinding, ok := spawners[cfg.Spawner]
			if !ok {
				return nil, errors.Join(ErrUnknownSpawner, fmt.Errorf("pool %q references unknown spawner %q", name, cfg.Spawner))
			}
			binding.remote = &remoteBinding{
				spawner: spBinding,
				poolCfg: copyPoolConfig(cfg),
			}
		}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func copyPoolConfig(cfg PoolConfig) PoolConfig {
	copyCfg := cfg
	if len(cfg.Tags) > 0 {
		copyCfg.Tags = append([]string(nil), cfg.Tags...)
	}
	copyCfg.Selector = Selector{
		All:  append([]string(nil), cfg.Selector.All...),
		Any:  append([]string(nil), cfg.Selector.Any...),
		None: append([]string(nil), cfg.Selector.None...),
	}
	if len(cfg.Metadata) > 0 {
		meta := make(map[string]any, len(cfg.Metadata))
		for k, v := range cfg.Metadata {
			meta[k] = v
		}
		copyCfg.Metadata = meta
	}
	return copyCfg
}

// ChildHandler represents the entrypoint executed when the binary re-enters as a child.
type ChildHandler func(context.Context, ChildNode) error

// HandleChild registers the default child-mode handler executed when the binary
// starts in child mode without a specific child type.
func HandleChild(handler ChildHandler, opts ...ChildOption) {
	registerChildHandler("", handler, opts...)
}

// HandleChildNamed registers a child handler for a specific childType. The
// spawner selects the handler by providing the same child type via
// SpawnerConfig.ChildType or process flags.
func HandleChildNamed(childType string, handler ChildHandler, opts ...ChildOption) {
	registerChildHandler(childType, handler, opts...)
}

func runChildProcess(ctx context.Context, detected child.Config) (*Node, <-chan DiagnosticEvent, error) {
	reg := resolveChildHandler(detected.ChildType)
	if reg == nil {
		return nil, nil, fmt.Errorf("gostage: no child handler registered for type %q", detected.ChildType)
	}
	cfg := mergeChildConfig(detected, reg)
	childNode := newChildNode(cfg)
	wrapper := childNodeWrapper{node: childNode}
	err := reg.handler(ctx, wrapper)
	if closeErr := wrapper.Close(); closeErr != nil {
		if err == nil {
			err = closeErr
		} else {
			err = errors.Join(err, closeErr)
		}
	}
	return nil, nil, err
}

func registerChildHandler(childType string, handler ChildHandler, opts ...ChildOption) {
	if handler == nil {
		return
	}
	reg := &childHandlerRegistration{
		childType: childType,
		handler:   handler,
		options:   childOptions{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&reg.options)
		}
	}
	childHandlersMu.Lock()
	defer childHandlersMu.Unlock()
	if childType == "" {
		defaultChildHandler = reg
		return
	}
	if namedChildHandlers == nil {
		namedChildHandlers = make(map[string]*childHandlerRegistration)
	}
	namedChildHandlers[childType] = reg
}

func resolveChildHandler(childType string) *childHandlerRegistration {
	childHandlersMu.RLock()
	defer childHandlersMu.RUnlock()
	if childType != "" {
		if reg, ok := namedChildHandlers[childType]; ok {
			return reg.clone()
		}
	}
	if defaultChildHandler == nil {
		return nil
	}
	return defaultChildHandler.clone()
}

func (reg *childHandlerRegistration) clone() *childHandlerRegistration {
	if reg == nil {
		return nil
	}
	clone := &childHandlerRegistration{
		childType: reg.childType,
		handler:   reg.handler,
		options:   reg.options.clone(),
	}
	return clone
}

func (opts childOptions) clone() childOptions {
	clone := childOptions{}
	if len(opts.pools) > 0 {
		clone.pools = make([]PoolConfig, len(opts.pools))
		for i, pool := range opts.pools {
			clone.pools[i] = copyPoolConfig(pool)
		}
	}
	if len(opts.metadata) > 0 {
		clone.metadata = copyStringStringMap(opts.metadata)
	}
	return clone
}

func copyStringStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func mergeChildConfig(base child.Config, reg *childHandlerRegistration) child.Config {
	if reg == nil {
		return base
	}
	cfg := base
	if len(cfg.Pools) == 0 && len(reg.options.pools) > 0 {
		cfg.Pools = childPoolSpecsFromOptions(reg.options.pools)
	}
	if len(cfg.Metadata) == 0 && len(reg.options.metadata) > 0 {
		cfg.Metadata = copyStringStringMap(reg.options.metadata)
	}
	return cfg
}

func childPoolSpecsFromOptions(pools []PoolConfig) []child.PoolSpec {
	if len(pools) == 0 {
		return nil
	}
	specs := make([]child.PoolSpec, 0, len(pools))
	for _, pool := range pools {
		slots := pool.Slots
		if slots <= 0 {
			slots = 1
		}
		spec := child.PoolSpec{
			Name:     pool.Name,
			Slots:    uint32(slots),
			Tags:     append([]string(nil), pool.Tags...),
			Metadata: poolMetadataToStrings(pool.Metadata),
		}
		specs = append(specs, spec)
	}
	return specs
}

func poolMetadataToStrings(values map[string]any) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for k, v := range values {
		if v == nil {
			continue
		}
		out[k] = fmt.Sprint(v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
