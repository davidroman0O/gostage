package gostage

import (
	"time"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

const defaultClaimInterval = 50 * time.Millisecond

type (
	Option          = bootstrap.Option
	OptionFunc      = bootstrap.OptionFunc
	ChildOption     = bootstrap.ChildOption
	ChildOptionFunc = bootstrap.ChildOptionFunc
)

// WithTelemetrySink registers telemetry sinks that will receive events emitted by the node.
func WithTelemetrySink(sink telemetry.Sink) Option {
	if sink == nil {
		return bootstrap.OptionFunc(func(*bootstrap.Config) {})
	}
	return bootstrap.OptionFunc(func(cfg *bootstrap.Config) {
		cfg.TelemetrySinks = append(cfg.TelemetrySinks, sink)
	})
}

// WithTelemetryDispatcherConfig customises telemetry buffering/backpressure behaviour.
func WithTelemetryDispatcherConfig(cfg TelemetryDispatcherConfig) Option {
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.TelemetryCfg = cfg
	})
}

// WithQueue overrides the state queue implementation used by the node.
func WithQueue(queue state.Queue) Option {
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.Queue = queue
	})
}

// WithStore overrides the persistence store implementation used by the node.
func WithStore(store state.Store) Option {
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.Store = store
	})
}

// WithSQLite configures the node to use an embedded SQLite backend.
func WithSQLite(cfg SQLiteConfig) Option {
	copyCfg := cfg
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.SQLite = &copyCfg
	})
}

// WithStateReader overrides the read-only state facade exposed via Node.State.
func WithStateReader(reader StateReader) Option {
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.StateReader = reader
	})
}

// WithStateObserver registers a StoreManager observer hook chain.
func WithStateObserver(observer StateObserver) Option {
	if observer == nil {
		return bootstrap.OptionFunc(func(*bootstrap.Config) {})
	}
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.Observers = append(o.Observers, observer)
	})
}

// WithPool registers a parent-side execution pool.
func WithPool(cfg PoolConfig) Option {
	copyCfg := copyPoolConfig(cfg)
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.Pools = append(o.Pools, copyCfg)
	})
}

// WithChildPool registers a pool that will be exposed when running in child mode.
func WithChildPool(cfg PoolConfig) ChildOption {
	copyCfg := copyPoolConfig(cfg)
	return bootstrap.ChildOptionFunc(func(o *bootstrap.ChildOptions) {
		o.Pools = append(o.Pools, copyCfg)
	})
}

// WithChildMetadata attaches metadata that will be injected into the child registration if the spawner does not supply its own metadata payload.
func WithChildMetadata(values map[string]string) ChildOption {
	return bootstrap.ChildOptionFunc(func(o *bootstrap.ChildOptions) {
		if len(values) == 0 {
			return
		}
		if o.Metadata == nil {
			o.Metadata = make(map[string]string, len(values))
		}
		for k, v := range values {
			o.Metadata[k] = v
		}
	})
}

// ChildWithTLS overrides the TLS materials supplied to the child bootstrap.
func ChildWithTLS(files TLSFiles) ChildOption {
	copyFiles := files
	return bootstrap.ChildOptionFunc(func(o *bootstrap.ChildOptions) {
		o.TLS = &copyFiles
	})
}

// ChildWithAuth overrides the authentication token provided to the child.
func ChildWithAuth(token string) ChildOption {
	return bootstrap.ChildOptionFunc(func(o *bootstrap.ChildOptions) {
		copied := token
		o.AuthToken = &copied
	})
}

// ChildWithLogger assigns a logger used by the child runtime when no logger is supplied by the parent spawner.
func ChildWithLogger(logger telemetry.Logger) ChildOption {
	return bootstrap.ChildOptionFunc(func(o *bootstrap.ChildOptions) {
		o.Logger = logger
	})
}

// WithChildLogger is an alias for ChildWithLogger to match documentation naming.
func WithChildLogger(logger telemetry.Logger) ChildOption { return ChildWithLogger(logger) }

// ChildWithTags appends process-level tags associated with the child process.
func ChildWithTags(tags ...string) ChildOption {
	return bootstrap.ChildOptionFunc(func(o *bootstrap.ChildOptions) {
		if len(tags) == 0 {
			return
		}
		o.Tags = appendUniqueStrings(o.Tags, tags...)
	})
}

// WithSpawner records a spawner configuration.
func WithSpawner(cfg SpawnerConfig) Option {
	copyCfg := copySpawnerConfig(cfg)
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.Spawners = append(o.Spawners, copyCfg)
	})
}

// WithRemoteBridge configures the parent-side listener used by remote children.
func WithRemoteBridge(cfg RemoteBridgeConfig) Option {
	copyCfg := cfg
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.RemoteBridge = copyCfg
	})
}

// WithDispatcher sets dispatcher tuning parameters.
func WithDispatcher(cfg DispatcherConfig) Option {
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.Dispatcher = cfg
	})
}

// WithLogger overrides the default logger used by runners.
func WithLogger(logger telemetry.Logger) Option {
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.Logger = logger
	})
}

// WithFailurePolicy sets the failure policy for workflow execution.
func WithFailurePolicy(policy FailurePolicy) Option {
	return bootstrap.OptionFunc(func(o *bootstrap.Config) {
		o.FailurePolicy = policy
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

func appendUniqueStrings(dst []string, values ...string) []string {
	seen := make(map[string]struct{}, len(dst))
	for _, v := range dst {
		seen[v] = struct{}{}
	}
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, exists := seen[v]; exists {
			continue
		}
		dst = append(dst, v)
		seen[v] = struct{}{}
	}
	return dst
}
