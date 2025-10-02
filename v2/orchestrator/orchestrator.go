package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/davidroman0O/gostage/v2/broker"
	"github.com/davidroman0O/gostage/v2/coordinator"
	"github.com/davidroman0O/gostage/v2/state"
	"github.com/davidroman0O/gostage/v2/workerhost"
)

var (
	errManagerRequired       = fmt.Errorf("orchestrator: manager is required")
	errRegistryRequired      = fmt.Errorf("orchestrator: registry is required")
	errFactoryRequired       = fmt.Errorf("orchestrator: runtime factory is required")
	errBrokerBuilderRequired = fmt.Errorf("orchestrator: broker builder is required")
)

type orchestratorImpl struct {
	cfg Config

	coordinator coordinator.Coordinator

	mu    sync.Mutex
	hosts map[coordinator.HostID]*hostRecord

	startOnce sync.Once
	stopOnce  sync.Once

	ctx    context.Context
	cancel context.CancelFunc
}

type hostRecord struct {
	host      workerhost.Host
	owned     bool
	stopped   bool
	stopMutex sync.Mutex
}

// New constructs an orchestrator from the provided configuration.
func New(cfg Config) (Orchestrator, error) {
	if cfg.Manager == nil {
		return nil, errManagerRequired
	}
	if cfg.Registry == nil {
		return nil, errRegistryRequired
	}
	if cfg.Factory == nil {
		return nil, errFactoryRequired
	}
	if cfg.BrokerBuilder == nil {
		return nil, errBrokerBuilderRequired
	}

	coordCfg := cfg.CoordinatorConfig
	coordCfg.Manager = cfg.Manager
	coordCfg.Logger = coordCfg.Logger
	if coordCfg.WorkflowFactory == nil {
		return nil, fmt.Errorf("orchestrator: coordinator workflow factory required")
	}

	coord, err := coordinator.New(coordCfg)
	if err != nil {
		return nil, err
	}

	return &orchestratorImpl{
		cfg:         cfg,
		coordinator: coord,
		hosts:       make(map[coordinator.HostID]*hostRecord),
	}, nil
}

func (o *orchestratorImpl) Start(ctx context.Context) error {
	var err error
	o.startOnce.Do(func() {
		if ctx == nil {
			err = fmt.Errorf("orchestrator: start context required")
			return
		}
		o.ctx, o.cancel = context.WithCancel(ctx)

		if err = o.startLocalHosts(); err != nil {
			return
		}

		err = o.coordinator.Start(o.ctx)
	})
	return err
}

func (o *orchestratorImpl) Stop(ctx context.Context) error {
	var err error
	o.stopOnce.Do(func() {
		if o.cancel != nil {
			o.cancel()
		}

		o.mu.Lock()
		hosts := make(map[coordinator.HostID]*hostRecord, len(o.hosts))
		for id, rec := range o.hosts {
			hosts[id] = rec
		}
		o.mu.Unlock()

		for id := range hosts {
			_ = o.coordinator.UnregisterHost(id)
		}

		if cerr := o.coordinator.Stop(ctx); cerr != nil {
			err = cerr
		}

		for id, rec := range hosts {
			if rec.owned {
				rec.stopMutex.Lock()
				if !rec.stopped {
					_ = rec.host.Stop(context.Background())
					rec.stopped = true
				}
				rec.stopMutex.Unlock()
			}
			o.mu.Lock()
			delete(o.hosts, id)
			o.mu.Unlock()
		}
	})
	return err
}

func (o *orchestratorImpl) Submit(def state.SubWorkflowDef) (string, error) {
	if def.ID == "" {
		def.ID = fmt.Sprintf("wf-%d", time.Now().UnixNano())
	}
	if def.CreatedAt.IsZero() {
		def.CreatedAt = time.Now()
	}
	id := o.cfg.Manager.StoreWithPriority(def, def.Priority)
	if id == "" {
		return "", fmt.Errorf("orchestrator: failed to store workflow")
	}
	return id, nil
}

func (o *orchestratorImpl) Stats() Stats {
	return Stats{Coordinator: o.coordinator.Stats()}
}

func (o *orchestratorImpl) HostStats() map[coordinator.HostID]coordinator.HostSnapshot {
	return o.coordinator.HostStats()
}

func (o *orchestratorImpl) ScaleHost(id coordinator.HostID, delta int) error {
	return o.coordinator.ScaleHost(id, delta)
}

func (o *orchestratorImpl) RegisterExternalHost(id coordinator.HostID, host workerhost.Host) error {
	if host == nil {
		return fmt.Errorf("orchestrator: host is nil")
	}
	if err := o.coordinator.RegisterHost(id, host); err != nil {
		return err
	}
	o.mu.Lock()
	o.hosts[id] = &hostRecord{host: host, owned: false}
	o.mu.Unlock()
	return nil
}

func (o *orchestratorImpl) startLocalHosts() error {
	local := o.cfg.LocalHosts
	if local <= 0 {
		local = 1
	}

	for i := 0; i < local; i++ {
		hostID := coordinator.HostID(fmt.Sprintf("local-%d", i+1))
		host, err := o.newWorkerHost()
		if err != nil {
			return err
		}
		if err := o.coordinator.RegisterHost(hostID, host); err != nil {
			return err
		}
		o.mu.Lock()
		o.hosts[hostID] = &hostRecord{host: host, owned: true}
		o.mu.Unlock()
	}
	return nil
}

func (o *orchestratorImpl) newWorkerHost() (workerhost.Host, error) {
	cfg := workerhost.Config{
		InitialSlots: 1,
		Factory:      o.cfg.Factory,
		Registry:     o.cfg.Registry,
		BrokerBuilder: func() (broker.Broker, error) {
			return o.cfg.BrokerBuilder()
		},
	}
	return workerhost.New(cfg)
}
