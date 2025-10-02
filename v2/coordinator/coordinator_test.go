package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v2/broker"
	"github.com/davidroman0O/gostage/v2/registry"
	"github.com/davidroman0O/gostage/v2/runtime/local"
	"github.com/davidroman0O/gostage/v2/state"
	"github.com/davidroman0O/gostage/v2/state/memory"
	"github.com/davidroman0O/gostage/v2/types"
	"github.com/davidroman0O/gostage/v2/types/defaults"
	"github.com/davidroman0O/gostage/v2/workerhost"
)

func TestCoordinatorEndToEndSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	host, reg, manager := newTestHost(t, 1)

	factory := &workflowFactory{mu: &sync.RWMutex{}, workflows: make(map[string]types.Workflow)}

	coord, err := New(Config{
		Manager:         manager,
		Logger:          nil,
		WorkerID:        "worker-1",
		WorkerType:      "local",
		ClaimInterval:   10 * time.Millisecond,
		WorkflowFactory: factory.Load,
	})
	if err != nil {
		t.Fatalf("New coordinator: %v", err)
	}
	if err := coord.RegisterHost(HostID("host-1"), host); err != nil {
		t.Fatalf("RegisterHost: %v", err)
	}

	if err := coord.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	enqueueWorkflow(t, reg, manager, factory, "wf-success", func(types.Context) error { return nil })

	res, err := manager.WaitForCompletion("wf-success", time.Second)
	if err != nil {
		t.Fatalf("WaitForCompletion: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success, got %+v", res)
	}

	if err := coord.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatalf("host stop: %v", err)
	}
}

func TestCoordinatorFailurePolicyRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	host, reg, manager := newTestHost(t, 1)
	factory := &workflowFactory{mu: &sync.RWMutex{}, workflows: make(map[string]types.Workflow)}

	policyCalls := 0
	policy := func(info FailureInfo) FailureAction {
		policyCalls++
		return FailureActionRelease
	}

	coord, err := New(Config{
		Manager:         manager,
		WorkerID:        "worker-1",
		WorkerType:      "local",
		ClaimInterval:   10 * time.Millisecond,
		WorkflowFactory: factory.Load,
		FailurePolicy:   policy,
	})
	if err != nil {
		t.Fatalf("New coordinator: %v", err)
	}
	if err := coord.RegisterHost(HostID("host-1"), host); err != nil {
		t.Fatalf("RegisterHost: %v", err)
	}
	if err := coord.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var attempts int
	enqueueWorkflow(t, reg, manager, factory, "wf-fail", func(types.Context) error {
		if attempts == 0 {
			attempts++
			return errors.New("boom")
		}
		return nil
	})

	res, err := manager.WaitForCompletion("wf-fail", 2*time.Second)
	if err != nil {
		t.Fatalf("WaitForCompletion: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success after retry, got %+v", res)
	}
	if policyCalls == 0 {
		t.Fatalf("expected failure policy to be called")
	}

	if err := coord.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := host.Stop(context.Background()); err != nil {
		t.Fatalf("host stop: %v", err)
	}
}

func TestCoordinatorMultiHostDistribution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hostA, reg, manager := newTestHost(t, 1)
	hostB := newWorkerHost(t, 1, reg, manager)

	factory := &workflowFactory{mu: &sync.RWMutex{}, workflows: make(map[string]types.Workflow)}

	coord, err := New(Config{
		Manager:         manager,
		WorkerID:        "worker-main",
		WorkerType:      "local",
		ClaimInterval:   10 * time.Millisecond,
		WorkflowFactory: factory.Load,
	})
	if err != nil {
		t.Fatalf("New coordinator: %v", err)
	}
	if err := coord.RegisterHost(HostID("host-A"), hostA); err != nil {
		t.Fatalf("register host A: %v", err)
	}
	if err := coord.RegisterHost(HostID("host-B"), hostB); err != nil {
		t.Fatalf("register host B: %v", err)
	}

	if err := coord.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	enqueueWorkflow(t, reg, manager, factory, "wf-A", func(types.Context) error { return nil })
	enqueueWorkflow(t, reg, manager, factory, "wf-B", func(types.Context) error { return nil })

	completed := map[string]bool{}
	usedHosts := map[HostID]bool{}
	done := make(chan struct{})
	go func() {
		for evt := range coord.Events() {
			if evt.Type == EventSucceeded {
				completed[evt.Workflow] = true
				usedHosts[evt.HostID] = true
				if len(completed) == 2 {
					close(done)
					return
				}
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for coordinator completions")
	}

	if len(usedHosts) < 2 {
		t.Fatalf("expected workflows to span two hosts, got %v", usedHosts)
	}

	if err := coord.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := hostA.Stop(context.Background()); err != nil {
		t.Fatalf("hostA stop: %v", err)
	}
	if err := hostB.Stop(context.Background()); err != nil {
		t.Fatalf("hostB stop: %v", err)
	}
}

func TestCoordinatorStopWithAcquireFailure(t *testing.T) {
	mgr := memory.New()
	stubHost := &fakeHost{
		eventsCh: make(chan workerhost.Event),
		stats:    workerhost.Stats{TotalSlots: 1},
	}
	stubHost.acquire = func(ctx context.Context) (workerhost.Lease, error) {
		return nil, workerhost.ErrHostStopped
	}

	coord, err := New(Config{
		Manager:    mgr,
		WorkerID:   "worker-err",
		WorkerType: "stub",
		WorkflowFactory: func(def state.SubWorkflowDef) (types.Workflow, error) {
			return nil, fmt.Errorf("unused")
		},
	})
	if err != nil {
		t.Fatalf("New coordinator: %v", err)
	}
	if err := coord.RegisterHost(HostID("stub"), stubHost); err != nil {
		t.Fatalf("RegisterHost: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := coord.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := coord.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

// Helpers ----------------------------------------------------------------

type workflowFactory struct {
	mu        *sync.RWMutex
	workflows map[string]types.Workflow
}

func (wf *workflowFactory) Load(def state.SubWorkflowDef) (types.Workflow, error) {
	wf.mu.RLock()
	defer wf.mu.RUnlock()
	w, ok := wf.workflows[def.ID]
	if !ok {
		return nil, fmt.Errorf("workflow %s not registered", def.ID)
	}
	return w, nil
}

func newTestHost(t *testing.T, slots int) (workerhost.Host, registry.Registry, *memory.Manager) {
	t.Helper()
	reg := registry.NewSafeRegistry()
	prev := registry.Default()
	registry.SetDefault(reg)
	t.Cleanup(func() { registry.SetDefault(prev) })

	mgr := memory.New()

	host, err := workerhost.New(workerhost.Config{
		InitialSlots: slots,
		Factory:      local.Factory{},
		Registry:     reg,
		BrokerBuilder: func() (broker.Broker, error) {
			return broker.NewLocal(mgr), nil
		},
	})
	if err != nil {
		t.Fatalf("workerhost.New: %v", err)
	}
	return host, reg, mgr
}

func enqueueWorkflow(t *testing.T, reg registry.Registry, mgr *memory.Manager, factory *workflowFactory, id string, fn func(types.Context) error) {
	t.Helper()
	wf := buildWorkflow(t, reg, id, fn)
	factory.mu.Lock()
	factory.workflows[id] = wf
	factory.mu.Unlock()

	def := state.SubWorkflowDef{
		ID:        id,
		Name:      id,
		Type:      "test",
		Tags:      []string{"test"},
		CreatedAt: time.Now(),
	}
	if stored := mgr.StoreWithPriority(def, state.PriorityDefault); stored == "" {
		t.Fatalf("StoreWithPriority returned empty id")
	}
}

func buildWorkflow(t *testing.T, reg registry.Registry, id string, fn func(types.Context) error) types.Workflow {
	t.Helper()
	actionID := id + "-action"
	if err := reg.RegisterAction(actionID, func(ctx types.Context) error { return fn(ctx) }, registry.ActionMetadata{}); err != nil {
		t.Fatalf("register action: %v", err)
	}

	action := defaults.NewAction(actionID)
	stage := defaults.NewStage(id+"-stage", "stage", "")
	stage.AddActions(action)

	wf := defaults.NewWorkflow(id, "workflow", "")
	wf.AddStage(stage)
	return wf
}

type fakeLease struct{}

func (fakeLease) ID() string                 { return "fake" }
func (fakeLease) Start(workerhost.Job) error { return nil }
func (fakeLease) Release() error             { return nil }

type fakeHost struct {
	acquire  func(ctx context.Context) (workerhost.Lease, error)
	eventsCh chan workerhost.Event
	stats    workerhost.Stats
}

func (f *fakeHost) Acquire(ctx context.Context) (workerhost.Lease, error) {
	if f.acquire != nil {
		return f.acquire(ctx)
	}
	return fakeLease{}, nil
}

func (f *fakeHost) Submit(workerhost.Job) error { return nil }

func (f *fakeHost) Events() <-chan workerhost.Event { return f.eventsCh }

func (f *fakeHost) Stats() workerhost.Stats { return f.stats }

func (f *fakeHost) Scale(delta int) error { return nil }

func (f *fakeHost) Stop(ctx context.Context) error { return nil }

func newWorkerHost(t *testing.T, slots int, reg registry.Registry, mgr *memory.Manager) workerhost.Host {
	t.Helper()
	host, err := workerhost.New(workerhost.Config{
		InitialSlots: slots,
		Factory:      local.Factory{},
		Registry:     reg,
		BrokerBuilder: func() (broker.Broker, error) {
			return broker.NewLocal(mgr), nil
		},
	})
	if err != nil {
		t.Fatalf("workerhost.New: %v", err)
	}
	return host
}
