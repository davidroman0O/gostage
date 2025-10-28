package gostage

import (
	"time"

	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// Option configures parent node behaviour.
type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (fn optionFunc) apply(opts *options) {
	fn(opts)
}

type options struct {
	telemetrySinks []telemetry.Sink
	queue          state.Queue
	store          state.Store
	stateReader    StateReader
	observers      []StateObserver
	sqlite         *SQLiteConfig
	pools          []PoolConfig
	childPools     []PoolConfig
	spawners       []SpawnerConfig
	remoteBridge   RemoteBridgeConfig
	dispatcher     DispatcherConfig
	logger         telemetry.Logger
	failurePolicy  FailurePolicy
	telemetryCfg   TelemetryDispatcherConfig
}

// ChildOption configures child node bootstrap via HandleChild.
type ChildOption interface {
	apply(*childOptions)
}

type childOptionFunc func(*childOptions)

func (fn childOptionFunc) apply(opts *childOptions) {
	fn(opts)
}

type childOptions struct {
	pools     []PoolConfig
	metadata  map[string]string
	tls       *TLSFiles
	authToken *string
	logger    telemetry.Logger
	tags      []string
}

const (
	defaultClaimInterval = 50 * time.Millisecond
)

// WithTelemetrySink registers telemetry sinks that will receive events emitted by the node.
func WithTelemetrySink(sink telemetry.Sink) Option {
	if sink == nil {
		return optionFunc(func(*options) {})
	}
	return optionFunc(func(o *options) {
		o.telemetrySinks = append(o.telemetrySinks, sink)
	})
}

// WithTelemetryDispatcherConfig customises telemetry buffering/backpressure behaviour.
func WithTelemetryDispatcherConfig(cfg TelemetryDispatcherConfig) Option {
	return optionFunc(func(o *options) {
		o.telemetryCfg = cfg
	})
}

// WithQueue overrides the state queue implementation used by the node.
func WithQueue(queue state.Queue) Option {
	return optionFunc(func(o *options) {
		o.queue = queue
	})
}

// WithStore overrides the persistence store implementation used by the node.
func WithStore(store state.Store) Option {
	return optionFunc(func(o *options) {
		o.store = store
	})
}

// WithSQLite configures the node to use an embedded SQLite backend.
func WithSQLite(cfg SQLiteConfig) Option {
	copy := cfg
	return optionFunc(func(o *options) {
		o.sqlite = &copy
	})
}

// WithStateReader overrides the read-only state facade exposed via Node.State.
func WithStateReader(reader StateReader) Option {
	return optionFunc(func(o *options) {
		o.stateReader = reader
	})
}

// WithStateObserver registers a StoreManager observer hook chain.
func WithStateObserver(observer StateObserver) Option {
	if observer == nil {
		return optionFunc(func(*options) {})
	}
	return optionFunc(func(o *options) {
		o.observers = append(o.observers, observer)
	})
}

// WithPool registers a parent-side execution pool.
func WithPool(cfg PoolConfig) Option {
	return optionFunc(func(o *options) {
		o.pools = append(o.pools, cfg)
	})
}

// WithChildPool registers a pool that will be exposed when running in child mode.
func WithChildPool(cfg PoolConfig) ChildOption {
	return childOptionFunc(func(o *childOptions) {
		o.pools = append(o.pools, copyPoolConfig(cfg))
	})
}

// WithChildMetadata attaches metadata that will be injected into the child
// registration if the spawner does not supply its own metadata payload.
func WithChildMetadata(values map[string]string) ChildOption {
	return childOptionFunc(func(o *childOptions) {
		if len(values) == 0 {
			return
		}
		if o.metadata == nil {
			o.metadata = make(map[string]string, len(values))
		}
		for k, v := range values {
			o.metadata[k] = v
		}
	})
}

// ChildWithTLS overrides the TLS materials supplied to the child bootstrap.
// All fields must reference readable files; empty values are ignored.
func ChildWithTLS(files TLSFiles) ChildOption {
	return childOptionFunc(func(o *childOptions) {
		o.tls = &TLSFiles{
			CertPath: files.CertPath,
			KeyPath:  files.KeyPath,
			CAPath:   files.CAPath,
		}
	})
}

// ChildWithAuth overrides the authentication token provided to the child.
// An empty token clears any previously configured value.
func ChildWithAuth(token string) ChildOption {
	return childOptionFunc(func(o *childOptions) {
		copied := token
		o.authToken = &copied
	})
}

// ChildWithLogger assigns a logger used by the child runtime when no logger is supplied by the parent spawner.
func ChildWithLogger(logger telemetry.Logger) ChildOption {
	return childOptionFunc(func(o *childOptions) {
		o.logger = logger
	})
}

// WithChildLogger is an alias for ChildWithLogger to match documentation naming.
func WithChildLogger(logger telemetry.Logger) ChildOption { return ChildWithLogger(logger) }

// ChildWithTags appends process-level tags associated with the child process.
func ChildWithTags(tags ...string) ChildOption {
	return childOptionFunc(func(o *childOptions) {
		if len(tags) == 0 {
			return
		}
		o.tags = appendUniqueStrings(o.tags, tags...)
	})
}

// WithSpawner records a spawner configuration. Spawner wiring arrives in later phases.
func WithSpawner(cfg SpawnerConfig) Option {
	return optionFunc(func(o *options) {
		o.spawners = append(o.spawners, copySpawnerConfig(cfg))
	})
}

// WithRemoteBridge configures the parent-side listener used by remote children.
func WithRemoteBridge(cfg RemoteBridgeConfig) Option {
	copy := cfg
	return optionFunc(func(o *options) {
		o.remoteBridge = copy
	})
}

// WithDispatcher sets dispatcher tuning parameters.
func WithDispatcher(cfg DispatcherConfig) Option {
	return optionFunc(func(o *options) {
		o.dispatcher = cfg
	})
}

// WithLogger overrides the default logger used by runners.
func WithLogger(logger telemetry.Logger) Option {
	return optionFunc(func(o *options) {
		o.logger = logger
	})
}

// WithFailurePolicy sets the failure policy for workflow execution.
func WithFailurePolicy(policy FailurePolicy) Option {
	return optionFunc(func(o *options) {
		o.failurePolicy = policy
	})
}

func copySpawnerConfig(cfg SpawnerConfig) SpawnerConfig {
	copyCfg := cfg
	if len(cfg.Args) > 0 {
		copyCfg.Args = append([]string(nil), cfg.Args...)
	}
	if len(cfg.Env) > 0 {
		copyCfg.Env = append([]string(nil), cfg.Env...)
	}
	if len(cfg.Tags) > 0 {
		copyCfg.Tags = append([]string(nil), cfg.Tags...)
	}
	if len(cfg.Metadata) > 0 {
		meta := make(map[string]string, len(cfg.Metadata))
		for k, v := range cfg.Metadata {
			meta[k] = v
		}
		copyCfg.Metadata = meta
	}
	return copyCfg
}
