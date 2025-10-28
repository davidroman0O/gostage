package child

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/davidroman0O/gostage/v3/telemetry"
)

const (
	EnvMode          = "GOSTAGE_MODE"
	EnvParentAddress = "GOSTAGE_PARENT_ADDRESS"
	EnvParentToken   = "GOSTAGE_PARENT_TOKEN"
	EnvChildType     = "GOSTAGE_CHILD_TYPE"
	EnvChildMetadata = "GOSTAGE_CHILD_METADATA"
	EnvChildPools    = "GOSTAGE_CHILD_POOLS"
	EnvTLSCert       = "GOSTAGE_TLS_CERT"
	EnvTLSKey        = "GOSTAGE_TLS_KEY"
	EnvTLSCA         = "GOSTAGE_TLS_CA"

	ModeChild = "child"

	defaultDialTimeout       = 10 * time.Second
	defaultHeartbeatInterval = 15 * time.Second
)

// PoolSpec captures pool information advertised by a child process during registration.
type PoolSpec struct {
	Name     string
	Slots    uint32
	Tags     []string
	Metadata map[string]string
}

// TLSConfig references TLS materials for client authentication.
type TLSConfig struct {
	CertPath string
	KeyPath  string
	CAPath   string
}

// Config contains the parameters required to bootstrap a child node connection.
type Config struct {
	Address           string
	AuthToken         string
	ChildType         string
	Metadata          map[string]string
	TLS               TLSConfig
	Pools             []PoolSpec
	DialTimeout       time.Duration
	HeartbeatInterval time.Duration
	Logger            telemetry.Logger
	Tags              []string
}

// GetenvFunc allows overriding environment lookups for testing.
type GetenvFunc func(string) string

// Detect parses CLI arguments and environment variables to determine whether the
// current process should run in child mode. When child mode is not requested,
// Detect returns (Config{}, false, nil).
func Detect(args []string, getenv GetenvFunc) (Config, bool, error) {
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	fs := flag.NewFlagSet("gostage-child", flag.ContinueOnError)
	fs.String("mode", "", "gostage execution mode")
	addrFlag := fs.String("parent-address", "", "parent process gRPC address")
	tokenFlag := fs.String("parent-token", "", "parent authentication token")
	childTypeFlag := fs.String("child-type", "", "child type identifier")
	metadataFlag := fs.String("child-metadata", "", "child metadata JSON")
	certFlag := fs.String("tls-cert", "", "client certificate path")
	keyFlag := fs.String("tls-key", "", "client key path")
	caFlag := fs.String("tls-ca", "", "CA certificate path")
	filteredArgs := make([]string, 0, len(args))
	for _, arg := range args {
		if strings.HasPrefix(arg, "-test.") || strings.HasPrefix(arg, "--test.") {
			continue
		}
		filteredArgs = append(filteredArgs, arg)
	}
	_ = fs.Parse(filteredArgs)

	mode := strings.ToLower(firstNonEmpty(getenv(EnvMode), fs.Lookup("mode").Value.String()))
	if mode != ModeChild {
		return Config{}, false, nil
	}

	cfg := Config{
		Address:           firstNonEmpty(getenv(EnvParentAddress), *addrFlag),
		AuthToken:         firstNonEmpty(getenv(EnvParentToken), *tokenFlag),
		ChildType:         firstNonEmpty(getenv(EnvChildType), *childTypeFlag),
		DialTimeout:       defaultDialTimeout,
		HeartbeatInterval: defaultHeartbeatInterval,
		Metadata:          make(map[string]string),
	}

	if cfg.Address == "" {
		return Config{}, false, errors.New("child mode requires parent address")
	}
	if metaStr := firstNonEmpty(getenv(EnvChildMetadata), *metadataFlag); metaStr != "" {
		if err := json.Unmarshal([]byte(metaStr), &cfg.Metadata); err != nil {
			return Config{}, false, fmt.Errorf("parse child metadata: %w", err)
		}
	}
	if poolsJSON := firstNonEmpty(getenv(EnvChildPools)); poolsJSON != "" {
		var pools []PoolSpec
		if err := json.Unmarshal([]byte(poolsJSON), &pools); err != nil {
			return Config{}, false, fmt.Errorf("parse child pools: %w", err)
		}
		cfg.Pools = append(cfg.Pools, pools...)
	}

	cfg.TLS = TLSConfig{
		CertPath: firstNonEmpty(getenv(EnvTLSCert), *certFlag),
		KeyPath:  firstNonEmpty(getenv(EnvTLSKey), *keyFlag),
		CAPath:   firstNonEmpty(getenv(EnvTLSCA), *caFlag),
	}

	return cfg, true, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
