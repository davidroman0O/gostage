package coordinator

import (
	"errors"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/scheduler"
	"github.com/davidroman0O/gostage/v3/spawner"
	"github.com/davidroman0O/gostage/v3/state"
)

// Binding associates a local pool with an optional remote executor.
type Binding struct {
	Pool   *pools.Local
	Remote *RemoteBinding
	Sched  *scheduler.Binding
}

// RemoteBinding contains remote execution configuration for a pool.
type RemoteBinding struct {
	Parent      *Binding
	Spawner     *SpawnerBinding
	PoolCfg     bootstrap.PoolConfig
	Coordinator *RemoteCoordinator
	Pool        *RemotePool
}

// SpawnerBinding represents a remote spawner instance and its runtime state.
type SpawnerBinding struct {
	Name       string
	Cfg        bootstrap.SpawnerConfig
	Process    *spawner.ProcessSpawner
	Supervisor *processSupervisor
}

// SchedulerBinding exposes the scheduler binding for dispatcher wiring.
func (b *Binding) SchedulerBinding() *scheduler.Binding {
	if b == nil {
		return nil
	}
	return b.Sched
}

// Ready reports whether the remote binding has an attached coordinator.
func (r *RemoteBinding) Ready() bool {
	return r != nil && r.Coordinator != nil
}

// Dispatch forwards a claimed workflow to the remote coordinator.
func (r *RemoteBinding) Dispatch(claimed *state.ClaimedWorkflow, release func()) error {
	if r == nil || r.Coordinator == nil {
		return errors.New("gostage: remote coordinator unavailable")
	}
	if r.Parent == nil {
		return errors.New("gostage: remote binding missing parent configuration")
	}
	return r.Coordinator.dispatch(r.Parent, claimed, release)
}
