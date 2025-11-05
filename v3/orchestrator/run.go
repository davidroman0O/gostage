package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/broker"
	"github.com/davidroman0O/gostage/v3/child"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/internal/locks"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/runner"
	"github.com/davidroman0O/gostage/v3/runtime/local"
	"github.com/davidroman0O/gostage/v3/scheduler"
	"github.com/davidroman0O/gostage/v3/spawner"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// Public state aliases so callers don't reach into v3/state.
type (
	WorkflowID                = state.WorkflowID
	WorkflowRecord            = state.WorkflowRecord
	StateReader               = state.StateReader
	StateFilter               = state.StateFilter
	WorkflowSummary           = state.WorkflowSummary
	ActionHistoryRecord       = state.ActionHistoryRecord
	WorkflowState             = state.WorkflowState
	StageRecord               = state.StageRecord
	ActionRecord              = state.ActionRecord
	ResultSummary             = state.ResultSummary
	StateObserver             = state.ManagerObserver
	StateObserverFuncs        = state.ObserverFuncs
	TelemetryDispatcherConfig = node.TelemetryDispatcherConfig
	TelemetryStats            = node.TelemetryStats
	OverflowStrategy          = node.OverflowStrategy
	TerminationReason         = state.TerminationReason
	Option                    = bootstrap.Option
	ChildOption               = bootstrap.ChildOption
	Selector                  = bootstrap.Selector
	PoolConfig                = bootstrap.PoolConfig
	DispatcherConfig          = bootstrap.DispatcherConfig
	SQLiteConfig              = bootstrap.SQLiteConfig
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

type ChildHandlerRegistration struct {
	childType string
	handler   ChildHandler
	options   bootstrap.ChildOptions
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
	defaultChildHandler *ChildHandlerRegistration
	namedChildHandlers  = make(map[string]*ChildHandlerRegistration)
)

// IsChildProcess reports whether the current process is running in child mode.
// It mirrors the detection path used by Run so callers can guard parent-only setup.
func IsChildProcess() bool {
	_, childMode, _ := childDetect(os.Args[1:], os.Getenv)
	return childMode
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
	Skipped    int
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

// Run bootstraps the orchestrator in parent mode.
func Run(ctx context.Context, opts ...Option) (*Node, <-chan DiagnosticEvent, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("gostage: context is required")
	}
	cfg := bootstrap.NewConfig()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt.Apply(cfg)
	}

	childCfg, childMode, err := childDetect(os.Args[1:], os.Getenv)
	if err != nil {
		return nil, nil, fmt.Errorf("gostage: detect child mode: %w", err)
	}
	if childMode {
		return runChildProcess(ctx, childCfg)
	}

	logger := cfg.Logger
	if logger == nil {
		logger = telemetry.NoopLogger{}
	}

	stateInit, err := bootstrap.PrepareState(cfg, logger)
	if err != nil {
		return nil, nil, err
	}
	queue := stateInit.Queue
	store := stateInit.Store
	stateReader := stateInit.StateReader
	queueOwned := stateInit.QueueOwned
	storeOwned := stateInit.StoreOwned
	sqliteDB := stateInit.SQLiteDB
	sqliteOwned := stateInit.SQLiteOwned

	managerOpts := []state.ManagerOption{}
	if len(cfg.Observers) > 0 {
		managerOpts = append(managerOpts, state.WithManagerObservers(cfg.Observers...))
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

	base := node.New(ctx, cfg.TelemetrySinks, cfg.TelemetryCfg)
	health := node.NewHealthDispatcher()
	if diagWriter := base.DiagnosticsWriter(); diagWriter != nil {
		writer := node.NewDiagnosticsHealthWriter(diagWriter)
		_ = health.Subscribe(writer.Handle)
	}

	clock := manager.Clock()

	managerWithTelemetry := WrapWithTelemetry(manager, base.TelemetryDispatcher())

	broker := broker.NewLocal(managerWithTelemetry)
	runOpts := []runner.Option{runner.WithDefaultLogger(logger)}
	r := runner.New(local.Factory{}, registry.Default(), broker, runOpts...)

	if err := bootstrap.ValidateConfig(cfg); err != nil {
		return nil, nil, err
	}

	spawnerIndex, err := indexSpawners(cfg.Spawners)
	if err != nil {
		return nil, nil, err
	}

	poolBindings, err := buildPools(cfg.Pools, spawnerIndex)
	if err != nil {
		return nil, nil, err
	}

	schedBindings := make([]*scheduler.Binding, 0, len(poolBindings))
	for _, binding := range poolBindings {
		if schedBinding := binding.SchedulerBinding(); schedBinding != nil {
			schedBindings = append(schedBindings, schedBinding)
		}
	}
	dispatcher := scheduler.New(ctx, queue, store, managerWithTelemetry, r, base.TelemetryDispatcher(), base.DiagnosticsWriter(), health, logger, schedBindings, scheduler.Options{
		ClaimInterval: cfg.Dispatcher.ClaimInterval,
		Jitter:        cfg.Dispatcher.Jitter,
		MaxInFlight:   cfg.Dispatcher.MaxInFlight,
		FailurePolicy: cfg.FailurePolicy,
		Clock:         clock,
	})
	remoteCoord, err := NewRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), base.DiagnosticsWriter(), health, logger, poolBindings, clock, cfg.RemoteBridge)
	if err != nil {
		if sqliteOwned {
			_ = sqliteDB.Close()
		}
		return nil, nil, err
	}
	dispatcher.Start()

	node := &Node{
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
		State:      stateReader,
	}

	return node, base.Diagnostics(), nil
}

func indexSpawners(cfgs []SpawnerConfig) (map[string]*SpawnerBinding, error) {
	if len(cfgs) == 0 {
		return map[string]*SpawnerBinding{}, nil
	}
	index := make(map[string]*SpawnerBinding, len(cfgs))
	for _, cfg := range cfgs {
		if cfg.Name == "" || cfg.BinaryPath == "" {
			return nil, errors.Join(ErrInvalidSpawnerConfig, fmt.Errorf("spawner %q requires name and binary path", cfg.Name))
		}
		if _, exists := index[cfg.Name]; exists {
			return nil, errors.Join(ErrDuplicateSpawner, fmt.Errorf("spawner %q defined multiple times", cfg.Name))
		}
		copyCfg := copySpawnerConfig(cfg)
		procCfg := spawner.Config{
			BinaryPath:           copyCfg.BinaryPath,
			Args:                 append([]string(nil), copyCfg.Args...),
			Env:                  append([]string(nil), copyCfg.Env...),
			WorkingDir:           copyCfg.WorkingDir,
			MaxRestarts:          copyCfg.MaxRestarts,
			RestartBackoff:       copyCfg.RestartBackoff,
			ShutdownGrace:        copyCfg.ShutdownGrace,
			DisableOutputCapture: copyCfg.DisableOutputCapture,
		}
		binding := &SpawnerBinding{
			Name:    copyCfg.Name,
			Cfg:     copyCfg,
			Process: spawner.NewProcessSpawner(procCfg),
		}
		index[copyCfg.Name] = binding
	}
	return index, nil
}

func buildPools(cfgs []PoolConfig, spawners map[string]*SpawnerBinding) ([]*PoolBinding, error) {
	if len(cfgs) == 0 {
		cfgs = []PoolConfig{{Slots: 1}}
	}
	bindings := make([]*PoolBinding, 0, len(cfgs))
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
		binding := &PoolBinding{Pool: pool}
		if cfg.Spawner != "" {
			spBinding, ok := spawners[cfg.Spawner]
			if !ok {
				return nil, errors.Join(ErrUnknownSpawner, fmt.Errorf("pool %q references unknown spawner %q", name, cfg.Spawner))
			}
			binding.Remote = &RemoteBinding{
				Parent:  binding,
				Spawner: spBinding,
				PoolCfg: copyPoolConfig(cfg),
			}
		}
		binding.Sched = &scheduler.Binding{
			Pool:   pool,
			Remote: binding.Remote,
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

// ChildMetadataConflict captures a conflicting metadata key supplied by both the
// parent spawner environment and a child registration option.
type ChildMetadataConflict struct {
	Key      string
	Existing string
	Proposed string
}

// ChildMetadataConflictError is returned when child metadata merges would
// override parent-provided values.
type ChildMetadataConflictError struct {
	conflicts []ChildMetadataConflict
}

// Error implements error.
func (e *ChildMetadataConflictError) Error() string {
	if e == nil || len(e.conflicts) == 0 {
		return bootstrap.ErrChildMetadataConflict.Error()
	}
	conflictKeys := make([]string, 0, len(e.conflicts))
	for _, conflict := range e.conflicts {
		conflictKeys = append(conflictKeys, conflict.Key)
	}
	return fmt.Sprintf("%s: %s", bootstrap.ErrChildMetadataConflict.Error(), strings.Join(conflictKeys, ", "))
}

// Unwrap exposes the sentinel error for callers using errors.Is/As.
func (e *ChildMetadataConflictError) Unwrap() error {
	return bootstrap.ErrChildMetadataConflict
}

// Conflicts returns a copy of the conflict list for inspection.
func (e *ChildMetadataConflictError) Conflicts() []ChildMetadataConflict {
	if e == nil || len(e.conflicts) == 0 {
		return nil
	}
	out := make([]ChildMetadataConflict, len(e.conflicts))
	copy(out, e.conflicts)
	return out
}

func runChildProcess(ctx context.Context, detected child.Config) (*Node, <-chan DiagnosticEvent, error) {
	reg := resolveChildHandler(detected.ChildType)
	if reg == nil {
		return nil, nil, fmt.Errorf("gostage: no child handler registered for type %q", detected.ChildType)
	}
	cfg, err := mergeChildConfig(detected, reg)
	if err != nil {
		return nil, nil, fmt.Errorf("gostage: child configuration invalid: %w", err)
	}
	childNode := newChildNode(cfg)
	wrapper := childNodeWrapper{node: childNode}
	err = reg.handler(ctx, wrapper)
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
	reg := &ChildHandlerRegistration{
		childType: childType,
		handler:   handler,
		options:   bootstrap.ChildOptions{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt.Apply(&reg.options)
		}
	}
	childHandlersMu.Lock()
	defer childHandlersMu.Unlock()
	if childType == "" {
		defaultChildHandler = reg
		return
	}
	if namedChildHandlers == nil {
		namedChildHandlers = make(map[string]*ChildHandlerRegistration)
	}
	namedChildHandlers[childType] = reg
}

func resolveChildHandler(childType string) *ChildHandlerRegistration {
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

func (reg *ChildHandlerRegistration) clone() *ChildHandlerRegistration {
	if reg == nil {
		return nil
	}
	clone := &ChildHandlerRegistration{
		childType: reg.childType,
		handler:   reg.handler,
		options:   cloneChildOptions(reg.options),
	}
	return clone
}

func cloneChildOptions(opts bootstrap.ChildOptions) bootstrap.ChildOptions {
	clone := bootstrap.ChildOptions{}
	if len(opts.Pools) > 0 {
		clone.Pools = make([]PoolConfig, len(opts.Pools))
		for i, pool := range opts.Pools {
			clone.Pools[i] = copyPoolConfig(pool)
		}
	}
	if len(opts.Metadata) > 0 {
		clone.Metadata = copyStringStringMap(opts.Metadata)
	}
	if opts.TLS != nil {
		tlsCopy := *opts.TLS
		clone.TLS = &tlsCopy
	}
	if opts.AuthToken != nil {
		tokenCopy := *opts.AuthToken
		clone.AuthToken = &tokenCopy
	}
	if opts.Logger != nil {
		clone.Logger = opts.Logger
	}
	if len(opts.Tags) > 0 {
		clone.Tags = append([]string(nil), opts.Tags...)
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

func mergeChildConfig(base child.Config, reg *ChildHandlerRegistration) (child.Config, error) {
	if reg == nil {
		return base, nil
	}
	cfg := base
	if len(cfg.Pools) == 0 && len(reg.options.Pools) > 0 {
		cfg.Pools = childPoolSpecsFromOptions(reg.options.Pools)
	}

	if len(reg.options.Metadata) > 0 {
		merged := copyStringStringMap(cfg.Metadata)
		if merged == nil {
			merged = make(map[string]string, len(reg.options.Metadata))
		}

		conflicts := make([]ChildMetadataConflict, 0)
		for k, v := range reg.options.Metadata {
			if current, exists := merged[k]; exists && current != v {
				conflicts = append(conflicts, ChildMetadataConflict{Key: k, Existing: current, Proposed: v})
			}
		}

		if len(conflicts) > 0 {
			return child.Config{}, &ChildMetadataConflictError{conflicts: conflicts}
		}

		for k, v := range reg.options.Metadata {
			merged[k] = v
		}
		cfg.Metadata = merged
	}

	if reg.options.TLS != nil {
		cfg.TLS = child.TLSConfig{
			CertPath: reg.options.TLS.CertPath,
			KeyPath:  reg.options.TLS.KeyPath,
			CAPath:   reg.options.TLS.CAPath,
		}
	}
	if reg.options.AuthToken != nil {
		cfg.AuthToken = *reg.options.AuthToken
	}
	if reg.options.Logger != nil && cfg.Logger == nil {
		cfg.Logger = reg.options.Logger
	}

	if len(reg.options.Tags) > 0 {
		existing := append([]string(nil), cfg.Tags...)
		existing = appendUniqueStrings(existing, reg.options.Tags...)
		cfg.Tags = existing
	}

	return cfg, nil
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
			Metadata: PoolMetadataToStrings(pool.Metadata),
		}
		specs = append(specs, spec)
	}
	return specs
}
