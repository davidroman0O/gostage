package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v2/broker"
	"github.com/davidroman0O/gostage/v2/coordinator"
	"github.com/davidroman0O/gostage/v2/registry"
	"github.com/davidroman0O/gostage/v2/runtime/local"
	"github.com/davidroman0O/gostage/v2/state"
	"github.com/davidroman0O/gostage/v2/state/memory"
	"github.com/davidroman0O/gostage/v2/types"
	"github.com/davidroman0O/gostage/v2/types/defaults"
)

func TestOrchestratorEndToEnd(t *testing.T) {
	mgr := memory.New()
	reg := registry.NewSafeRegistry()
	prev := registry.Default()
	registry.SetDefault(reg)
	t.Cleanup(func() { registry.SetDefault(prev) })

	wfFactory := &workflowFactory{mu: &sync.RWMutex{}, workflows: make(map[string]types.Workflow)}

	coordCfg := coordinator.Config{
		WorkerID:        "main",
		WorkerType:      "local",
		ClaimInterval:   10 * time.Millisecond,
		WorkflowFactory: wfFactory.Load,
	}

	orch, err := New(Config{
		Manager:           mgr,
		Registry:          reg,
		Factory:           local.Factory{},
		BrokerBuilder:     func() (broker.Broker, error) { return broker.NewLocal(mgr), nil },
		CoordinatorConfig: coordCfg,
		LocalHosts:        2,
	})
	if err != nil {
		t.Fatalf("New orchestrator: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := orch.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	def1, _ := makeWorkflow(t, reg, wfFactory, "orch-wf-1", func(types.Context) error { return nil })
	def2, _ := makeWorkflow(t, reg, wfFactory, "orch-wf-2", func(types.Context) error { return nil })

	if _, err := orch.Submit(def1); err != nil {
		t.Fatalf("Submit wf1: %v", err)
	}
	if _, err := orch.Submit(def2); err != nil {
		t.Fatalf("Submit wf2: %v", err)
	}

	waitCompletion(t, mgr, def1.ID)
	waitCompletion(t, mgr, def2.ID)

	stats := orch.Stats()
	if stats.Coordinator.Completed < 2 {
		t.Fatalf("expected coordinator completed >= 2, got %+v", stats.Coordinator)
	}

	hostStats := orch.HostStats()
	if len(hostStats) != 2 {
		t.Fatalf("expected 2 host stats, got %d", len(hostStats))
	}

	if err := orch.Stop(context.Background()); err != nil {
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

func makeWorkflow(t *testing.T, reg registry.Registry, factory *workflowFactory, id string, fn func(types.Context) error) (state.SubWorkflowDef, types.Workflow) {
	t.Helper()
	actionID := id + "-action"
	if err := reg.RegisterAction(actionID, func(ctx types.Context) error { return fn(ctx) }, registry.ActionMetadata{}); err != nil {
		t.Fatalf("register action: %v", err)
	}
	action := defaults.NewAction(actionID)
	stage := defaults.NewStage(id+"-stage", "stage", "")
	stage.AddActions(action)
	wf := defaults.NewWorkflow(id, "wf", "")
	wf.AddStage(stage)

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
	return def, wf
}

func waitCompletion(t *testing.T, mgr *memory.Manager, id string) {
	t.Helper()
	if _, err := mgr.WaitForCompletion(id, time.Second); err != nil {
		t.Fatalf("WaitForCompletion %s: %v", id, err)
	}
}
