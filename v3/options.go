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
	stateReader    state.StateReader
	observers      []state.ManagerObserver
	sqlite         *SQLiteConfig
	pools          []PoolConfig
	childPools     []PoolConfig
	spawners       []SpawnerConfig
	dispatcher     DispatcherConfig
	logger         telemetry.Logger
	failurePolicy  FailurePolicy
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
	pools []PoolConfig
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

// WithTelemetrySinks registers multiple telemetry sinks at once.
func WithTelemetrySinks(sinks ...telemetry.Sink) Option {
	return optionFunc(func(o *options) {
		for _, sink := range sinks {
			if sink != nil {
				o.telemetrySinks = append(o.telemetrySinks, sink)
			}
		}
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
func WithStateReader(reader state.StateReader) Option {
	return optionFunc(func(o *options) {
		o.stateReader = reader
	})
}

// WithStateObserver registers a StoreManager observer hook chain.
func WithStateObserver(observer state.ManagerObserver) Option {
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
		o.pools = append(o.pools, cfg)
	})
}

// WithSpawner records a spawner configuration. Spawner wiring arrives in later phases.
func WithSpawner(cfg SpawnerConfig) Option {
	return optionFunc(func(o *options) {
		o.spawners = append(o.spawners, cfg)
	})
}

// WithDispatcherConfig sets dispatcher tuning parameters.
func WithDispatcherConfig(cfg DispatcherConfig) Option {
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
