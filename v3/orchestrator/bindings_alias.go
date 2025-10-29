package orchestrator

import "github.com/davidroman0O/gostage/v3/scheduler/coordinator"

type PoolBinding = coordinator.Binding
type RemoteBinding = coordinator.RemoteBinding
type SpawnerBinding = coordinator.SpawnerBinding

type RemoteCoordinator = coordinator.RemoteCoordinator
type RemotePool = coordinator.RemotePool
type RemoteWorker = coordinator.RemoteWorker
type RemoteJob = coordinator.RemoteJob
type RemoteBridgeConfig = coordinator.RemoteBridgeConfig
type TLSFiles = coordinator.TLSFiles
type SpawnerConfig = coordinator.SpawnerConfig

var (
	ErrMissingTLSPair   = coordinator.ErrMissingTLSPair
	ErrMissingTLSCA     = coordinator.ErrMissingTLSCA
	ErrMissingAuthToken = coordinator.ErrMissingAuthToken
)

var (
	NewRemoteCoordinator            = coordinator.NewRemoteCoordinator
	NewRemoteCoordinatorBareForTest = coordinator.NewRemoteCoordinatorBareForTest
	NewRemoteWorkerForTest          = coordinator.NewRemoteWorkerForTest
	PoolMetadataToStrings           = coordinator.PoolMetadataToStrings
	ValidateRemoteBinding           = coordinator.ValidateRemoteBinding
)
