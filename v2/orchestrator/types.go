package orchestrator

import (
	"context"

	"github.com/davidroman0O/gostage/v2/broker"
	"github.com/davidroman0O/gostage/v2/coordinator"
	"github.com/davidroman0O/gostage/v2/registry"
	"github.com/davidroman0O/gostage/v2/runtime/core"
	"github.com/davidroman0O/gostage/v2/state"
	"github.com/davidroman0O/gostage/v2/workerhost"
)

// WorkflowFactory describes how to materialise a workflow definition at runtime.
type WorkflowFactory = coordinator.WorkflowFactory

// Config describes how to construct an orchestrator instance.
type Config struct {
	Manager       state.Manager
	Registry      registry.Registry
	Factory       core.Factory
	BrokerBuilder func() (broker.Broker, error)

	CoordinatorConfig coordinator.Config
	LocalHosts        int
}

// Stats exposes high-level orchestrator telemetry.
type Stats struct {
	Coordinator coordinator.Snapshot
}

// Orchestrator represents the façade exposed to applications.
type Orchestrator interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	Submit(def state.SubWorkflowDef) (string, error)

	Stats() Stats
	HostStats() map[coordinator.HostID]coordinator.HostSnapshot

	ScaleHost(id coordinator.HostID, delta int) error
	RegisterExternalHost(id coordinator.HostID, host workerhost.Host) error
}
