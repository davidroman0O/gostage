package coordinator

import (
	"github.com/davidroman0O/gostage/v3/child"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/process"
	"github.com/davidroman0O/gostage/v3/scheduler"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

func (rc *RemoteCoordinator) PoolsForTest() map[string]*RemotePool { return rc.pools }

func (rc *RemoteCoordinator) JobsForTest() map[string]*RemoteJob { return rc.jobs }

func (rc *RemoteCoordinator) DispatcherForTest() *scheduler.Dispatcher { return rc.dispatcher }

func (rc *RemoteCoordinator) ShutdownForTest() { rc.shutdown() }

func (rc *RemoteCoordinator) BindAddressForTest() string { return rc.bindAddress }

func (rc *RemoteCoordinator) AddressForTest() string { return rc.address }

func (rc *RemoteCoordinator) LaunchSpawnerForTest(binding *Binding) error {
	return rc.launchSpawner(binding)
}

func (rc *RemoteCoordinator) PoolSpecsForSpawnerForTest(sp *SpawnerBinding) []child.PoolSpec {
	return rc.poolSpecsForSpawner(sp)
}

func (rc *RemoteCoordinator) ForwardChildLogForTest(pool string, evt diagnostics.Event) {
	rc.forwardChildLog(pool, evt)
}

func NewRemoteCoordinatorBareForTest(diag node.DiagnosticsWriter, logger telemetry.Logger) *RemoteCoordinator {
	return &RemoteCoordinator{
		diagnostics: diag,
		logger:      logger,
		pools:       make(map[string]*RemotePool),
	}
}

func NewRemoteWorkerForTest(busy bool, metadata map[string]string) *RemoteWorker {
	return &RemoteWorker{busy: busy, metadata: metadata}
}

func (w *RemoteWorker) SetBusyForTest(b bool) {
	if w != nil {
		w.busy = b
	}
}

func (w *RemoteWorker) BusyForTest() bool {
	return w != nil && w.busy
}

func (w *RemoteWorker) MetadataForTest() map[string]string {
	if w == nil {
		return nil
	}
	return w.metadata
}

func (w *RemoteWorker) SetConnectionForTest(conn *process.Connection) {
	if w != nil {
		w.conn = conn
	}
}
