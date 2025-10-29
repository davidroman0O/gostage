package coordinator

import (
	"errors"

	"github.com/davidroman0O/gostage/v3/bootstrap"
)

// RemoteBridgeConfig controls the parent-side listener used by remote children.
type RemoteBridgeConfig = bootstrap.RemoteBridgeConfig

// TLSFiles references TLS material paths supplied to the remote bridge/spawner.
type TLSFiles = bootstrap.TLSFiles

// SpawnerConfig captures remote spawner configuration.
type SpawnerConfig = bootstrap.SpawnerConfig

var (
	ErrMissingTLSPair   = errors.New("coordinator: TLS cert/key pair required when enabling TLS")
	ErrMissingTLSCA     = errors.New("coordinator: TLS CA required when enabling TLS")
	ErrMissingAuthToken = errors.New("coordinator: auth token required for remote spawner")
)
