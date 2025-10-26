package gostage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/process"
	processproto "github.com/davidroman0O/gostage/v3/process/proto"
	"github.com/davidroman0O/gostage/v3/spawner"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

type diagCollector struct {
	mu     sync.Mutex
	events []diagnostics.Event
}

func (d *diagCollector) Write(evt diagnostics.Event) {
	d.mu.Lock()
	d.events = append(d.events, evt)
	d.mu.Unlock()
}

func (d *diagCollector) Events() []diagnostics.Event {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]diagnostics.Event, len(d.events))
	copy(out, d.events)
	return out
}

func buildRemoteCoordinatorForTest(t *testing.T, ctx context.Context, diag node.DiagnosticsWriter, bindings []*poolBinding) (*remoteCoordinator, *node.HealthDispatcher) {
	t.Helper()
	queue := state.NewMemoryQueue()
	health := node.NewHealthDispatcher()
	base := node.New(ctx, nil, node.TelemetryDispatcherConfig{})

	dispatcher := newDispatcher(ctx, queue, nil, nil, nil, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, 0, 0, 0, nil, bindings)
	rc := &remoteCoordinator{
		ctx:         ctx,
		cancel:      func() {},
		dispatcher:  dispatcher,
		queue:       queue,
		telemetry:   base.TelemetryDispatcher(),
		diagnostics: diag,
		health:      health,
		logger:      telemetry.NoopLogger{},
		pools:       make(map[string]*remotePool),
		jobs:        make(map[string]*remoteJob),
		spawned:     make(map[string]*spawner.ProcessHandle),
	}
	for _, binding := range bindings {
		rp := &remotePool{binding: binding}
		rc.pools[binding.pool.Name()] = rp
		if binding.remote != nil {
			binding.remote.pool = rp
			binding.remote.coordinator = rc
		}
	}
	return rc, health
}

func waitHealthEvents(t *testing.T, ch <-chan node.HealthEvent, want int) []node.HealthEvent {
	t.Helper()
	events := make([]node.HealthEvent, 0, want)
	deadline := time.NewTimer(500 * time.Millisecond)
	defer deadline.Stop()
	for len(events) < want {
		select {
		case evt := <-ch:
			events = append(events, evt)
		case <-deadline.C:
			t.Fatalf("expected %d health events, got %d", want, len(events))
		}
	}
	return events
}

func TestRemoteCoordinatorRegisterMultiplePools(t *testing.T) {
	ctx := context.Background()
	diag := &diagCollector{}

	spBinding := &spawnerBinding{name: "remote-spawner", cfg: SpawnerConfig{Name: "remote-spawner"}}
	bindingA := &poolBinding{
		pool: pools.NewLocal("remote-a", state.Selector{}, 1),
		remote: &remoteBinding{
			spawner: spBinding,
			poolCfg: PoolConfig{Name: "remote-a", Slots: 1, Tags: []string{"remote"}},
		},
	}
	bindingB := &poolBinding{
		pool: pools.NewLocal("remote-b", state.Selector{}, 1),
		remote: &remoteBinding{
			spawner: spBinding,
			poolCfg: PoolConfig{Name: "remote-b", Slots: 2, Tags: []string{"remote"}},
		},
	}

	rc, health := buildRemoteCoordinatorForTest(t, ctx, diag, []*poolBinding{bindingA, bindingB})
	events := make(chan node.HealthEvent, 4)
	stop := health.Subscribe(func(evt node.HealthEvent) { events <- evt })
	defer stop()

	req := &processproto.RegisterNode{
		NodeId: "child-1",
		Pools: []*processproto.ChildPool{
			{Name: "remote-a", Slots: 1},
			{Name: "remote-b", Slots: 2},
		},
	}
	ack, err := rc.OnRegister(ctx, &process.Connection{}, req)
	if err != nil {
		t.Fatalf("OnRegister returned error: %v", err)
	}
	if ack == nil {
		t.Fatalf("expected ack response")
	}

	rc.mu.Lock()
	if rc.pools["remote-a"].worker == nil || rc.pools["remote-b"].worker == nil {
		t.Fatalf("expected workers assigned for both pools")
	}
	rc.mu.Unlock()

	got := waitHealthEvents(t, events, 2)
	names := map[string]node.HealthStatus{}
	for _, evt := range got {
		names[evt.Pool] = evt.Status
	}
	if names["remote-a"] != node.HealthHealthy || names["remote-b"] != node.HealthHealthy {
		t.Fatalf("unexpected health statuses: %#v", names)
	}
	if len(diag.Events()) != 0 {
		t.Fatalf("expected no diagnostics, got %v", diag.Events())
	}
}

func TestRemoteCoordinatorRegisterUnknownPoolDiagnostic(t *testing.T) {
	ctx := context.Background()
	diag := &diagCollector{}

	spBinding := &spawnerBinding{name: "remote-spawner", cfg: SpawnerConfig{Name: "remote-spawner"}}
	binding := &poolBinding{
		pool: pools.NewLocal("remote-a", state.Selector{}, 1),
		remote: &remoteBinding{
			spawner: spBinding,
			poolCfg: PoolConfig{Name: "remote-a", Slots: 1},
		},
	}

	rc, health := buildRemoteCoordinatorForTest(t, ctx, diag, []*poolBinding{binding})
	events := make(chan node.HealthEvent, 2)
	stop := health.Subscribe(func(evt node.HealthEvent) { events <- evt })
	defer stop()

	req := &processproto.RegisterNode{
		NodeId:    "child-1",
		ChildType: "default",
		Pools: []*processproto.ChildPool{
			{Name: "remote-a", Slots: 1},
			{Name: "unknown", Slots: 1},
		},
	}

	ack, err := rc.OnRegister(ctx, &process.Connection{}, req)
	if err != nil {
		t.Fatalf("OnRegister returned error: %v", err)
	}
	if ack == nil {
		t.Fatalf("expected ack response")
	}

	waitHealthEvents(t, events, 1)

	diagnostics := diag.Events()
	if len(diagnostics) == 0 {
		t.Fatalf("expected diagnostic for unknown pool")
	}
	found := false
	for _, evt := range diagnostics {
		if evt.Metadata["unknown_pools"] != nil {
			pools := evt.Metadata["unknown_pools"].([]string)
			if len(pools) == 1 && pools[0] == "unknown" {
				found = true
				break
			}
		}
	}
	if !found {
		t.Fatalf("expected unknown pool diagnostic, got %v", diagnostics)
	}
}

func TestRemoteCoordinatorRegisterMissingPoolHealth(t *testing.T) {
	ctx := context.Background()
	diag := &diagCollector{}

	spBinding := &spawnerBinding{name: "remote-spawner", cfg: SpawnerConfig{Name: "remote-spawner"}}
	bindingA := &poolBinding{
		pool: pools.NewLocal("remote-a", state.Selector{}, 1),
		remote: &remoteBinding{
			spawner: spBinding,
			poolCfg: PoolConfig{Name: "remote-a", Slots: 1},
		},
	}
	bindingB := &poolBinding{
		pool: pools.NewLocal("remote-b", state.Selector{}, 1),
		remote: &remoteBinding{
			spawner: spBinding,
			poolCfg: PoolConfig{Name: "remote-b", Slots: 1},
		},
	}

	rc, health := buildRemoteCoordinatorForTest(t, ctx, diag, []*poolBinding{bindingA, bindingB})
	events := make(chan node.HealthEvent, 4)
	stop := health.Subscribe(func(evt node.HealthEvent) { events <- evt })
	defer stop()

	req := &processproto.RegisterNode{
		NodeId: "child-1",
		Pools: []*processproto.ChildPool{
			{Name: "remote-a", Slots: 1},
		},
	}

	ack, err := rc.OnRegister(ctx, &process.Connection{}, req)
	if err != nil {
		t.Fatalf("OnRegister returned error: %v", err)
	}
	if ack == nil {
		t.Fatalf("expected ack response")
	}

	got := waitHealthEvents(t, events, 2)
	statuses := map[string]node.HealthStatus{}
	for _, evt := range got {
		statuses[evt.Pool] = evt.Status
	}
	if statuses["remote-a"] != node.HealthHealthy {
		t.Fatalf("expected remote-a healthy, got %v", statuses["remote-a"])
	}
	if statuses["remote-b"] != node.HealthUnavailable {
		t.Fatalf("expected remote-b unavailable, got statuses %v", statuses)
	}

	diagnostics := diag.Events()
	found := false
	for _, evt := range diagnostics {
		if evt.Metadata["missing_pools"] != nil {
			pools := evt.Metadata["missing_pools"].([]string)
			if len(pools) == 1 && pools[0] == "remote-b" {
				found = true
				break
			}
		}
	}
	if !found {
		t.Fatalf("expected missing pool diagnostic, got %v", diagnostics)
	}
}

func TestRemoteCoordinatorMetadataValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("matching metadata", func(t *testing.T) {
		diag := &diagCollector{}
		spBinding := &spawnerBinding{name: "remote-spawner", cfg: SpawnerConfig{
			Name:     "remote-spawner",
			Metadata: map[string]string{"region": "us-east"},
			Tags:     []string{"remote"},
		}}
		binding := &poolBinding{
			pool: pools.NewLocal("remote", state.Selector{}, 1),
			remote: &remoteBinding{
				spawner: spBinding,
				poolCfg: PoolConfig{Name: "remote", Slots: 1},
			},
		}
		rc, _ := buildRemoteCoordinatorForTest(t, ctx, diag, []*poolBinding{binding})
		req := &processproto.RegisterNode{
			NodeId: "child-1",
			Metadata: map[string]string{
				"region": "us-east",
				"tags":   "remote",
			},
			Pools: []*processproto.ChildPool{{Name: "remote", Slots: 1}},
		}
		if _, err := rc.OnRegister(ctx, &process.Connection{}, req); err != nil {
			t.Fatalf("OnRegister returned error: %v", err)
		}
		if len(diag.Events()) != 0 {
			t.Fatalf("expected no diagnostics, got %v", diag.Events())
		}
		rc.mu.Lock()
		workerMeta := rc.pools["remote"].worker.metadata
		rc.mu.Unlock()
		if workerMeta["region"] != "us-east" {
			t.Fatalf("expected worker metadata region preserved, got %v", workerMeta)
		}
		if workerMeta["tags"] != "remote" {
			t.Fatalf("expected worker metadata tags preserved, got %v", workerMeta)
		}
	})

	t.Run("missing metadata emits diagnostic", func(t *testing.T) {
		diag := &diagCollector{}
		spBinding := &spawnerBinding{name: "remote-spawner", cfg: SpawnerConfig{
			Name:     "remote-spawner",
			Metadata: map[string]string{"region": "us-west"},
		}}
		binding := &poolBinding{
			pool: pools.NewLocal("remote", state.Selector{}, 1),
			remote: &remoteBinding{
				spawner: spBinding,
				poolCfg: PoolConfig{Name: "remote", Slots: 1},
			},
		}
		rc, _ := buildRemoteCoordinatorForTest(t, ctx, diag, []*poolBinding{binding})
		req := &processproto.RegisterNode{
			NodeId:   "child-1",
			Metadata: map[string]string{"tags": "remote"},
			Pools:    []*processproto.ChildPool{{Name: "remote", Slots: 1}},
		}
		if _, err := rc.OnRegister(ctx, &process.Connection{}, req); err != nil {
			t.Fatalf("OnRegister returned error: %v", err)
		}
		diagnostics := diag.Events()
		if len(diagnostics) == 0 {
			t.Fatalf("expected metadata diagnostic, got none")
		}
		found := false
		for _, evt := range diagnostics {
			if keys, ok := evt.Metadata["missing_keys"].([]string); ok {
				if len(keys) == 1 && keys[0] == "region" {
					found = true
					break
				}
			}
		}
		if !found {
			t.Fatalf("expected missing_keys diagnostic containing region, got %v", diagnostics)
		}
	})
}
