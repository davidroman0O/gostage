package bootstrap

import (
	"database/sql"
	"time"

	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

// Selector mirrors the queue selector used by pools.
type Selector struct {
	All  []string
	Any  []string
	None []string
}

// PoolConfig describes a pool exposed by the parent or child process.
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

// RemoteBridgeConfig controls the parent-side gRPC bridge used by remote children.
type RemoteBridgeConfig struct {
	BindAddress       string
	AdvertiseAddress  string
	TLS               TLSFiles
	RequireClientCert bool
}

// SpawnerConfig configures remote capacity provisioning.
type SpawnerConfig struct {
	Name                 string
	BinaryPath           string
	Args                 []string
	Env                  []string
	WorkingDir           string
	AuthToken            string
	Tags                 []string
	Metadata             map[string]string
	ChildType            string
	TLS                  TLSFiles
	MaxRestarts          int
	RestartBackoff       time.Duration
	ShutdownGrace        time.Duration
	DisableOutputCapture bool
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

// Config collects bootstrap options resolved from gostage.Option values.
type Config struct {
	TelemetrySinks []telemetry.Sink
	Queue          state.Queue
	Store          state.Store
	StateReader    state.StateReader
	Observers      []state.ManagerObserver
	SQLite         *SQLiteConfig
	Pools          []PoolConfig
	ChildPools     []PoolConfig
	Spawners       []SpawnerConfig
	RemoteBridge   RemoteBridgeConfig
	Dispatcher     DispatcherConfig
	Logger         telemetry.Logger
	FailurePolicy  FailurePolicy
	TelemetryCfg   node.TelemetryDispatcherConfig
}

// ChildOptions holds child-specific configuration assembled from ChildOption values.
type ChildOptions struct {
	Pools     []PoolConfig
	Metadata  map[string]string
	TLS       *TLSFiles
	AuthToken *string
	Logger    telemetry.Logger
	Tags      []string
}

// Option configures parent node behaviour.
type Option interface {
	Apply(*Config)
}

type OptionFunc func(*Config)

func (fn OptionFunc) Apply(cfg *Config) {
	if fn != nil {
		fn(cfg)
	}
}

// ChildOption configures child bootstrap behaviour when registering handlers.
type ChildOption interface {
	Apply(*ChildOptions)
}

type ChildOptionFunc func(*ChildOptions)

func (fn ChildOptionFunc) Apply(opts *ChildOptions) {
	if fn != nil {
		fn(opts)
	}
}

// NewConfig constructs an empty bootstrap configuration.
func NewConfig() *Config {
	return &Config{}
}
