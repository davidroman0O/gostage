package coordinator

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/process"
	processproto "github.com/davidroman0O/gostage/v3/process/proto"
	"github.com/davidroman0O/gostage/v3/runner"
	"github.com/davidroman0O/gostage/v3/scheduler"
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

func testBinaryPath(t *testing.T) string {
	path, err := os.Executable()
	if err != nil {
		t.Fatalf("executable path: %v", err)
	}
	return path
}

func buildRemoteCoordinatorForTest(t *testing.T, ctx context.Context, diag node.DiagnosticsWriter, bindings []*Binding) (*RemoteCoordinator, *node.HealthDispatcher) {
	t.Helper()
	queue := state.NewMemoryQueue()
	health := node.NewHealthDispatcher()
	base := node.New(ctx, nil, node.TelemetryDispatcherConfig{})
	originalSpawners := make([]*SpawnerBinding, len(bindings))
	for i, binding := range bindings {
		if binding != nil && binding.Remote != nil {
			originalSpawners[i] = binding.Remote.Spawner
			binding.Remote.Spawner = nil
		}
	}

	dispatcher := newTestDispatcher(ctx, queue, nil, nil, nil, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, 0, 0, 0, nil, bindings, time.Now)
	rc, err := NewRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, bindings, time.Now, RemoteBridgeConfig{})
	if err != nil {
		t.Fatalf("NewRemoteCoordinator: %v", err)
	}
	for i, binding := range bindings {
		if binding != nil && binding.Remote != nil {
			binding.Remote.Spawner = originalSpawners[i]
			if pool := rc.PoolsForTest()[binding.Pool.Name()]; pool != nil {
				pool.Binding.Remote.Spawner = originalSpawners[i]
			}
		}
	}
	return rc, health
}

func newTestDispatcher(
	ctx context.Context,
	queue state.Queue,
	store state.Store,
	manager state.Manager,
	run *runner.Runner,
	telemetryDisp scheduler.TelemetryDispatcher,
	diag scheduler.DiagnosticsWriter,
	health scheduler.HealthPublisher,
	logger telemetry.Logger,
	claimInterval, jitter time.Duration,
	maxInFlight int,
	failure bootstrap.FailurePolicy,
	bindings []*Binding,
	clock func() time.Time,
) *scheduler.Dispatcher {
	schedBindings := make([]*scheduler.Binding, 0, len(bindings))
	for _, binding := range bindings {
		if binding == nil {
			continue
		}
		schedBindings = append(schedBindings, &scheduler.Binding{Pool: binding.Pool, Remote: binding.Remote})
	}

	opts := scheduler.Options{
		ClaimInterval: claimInterval,
		Jitter:        jitter,
		MaxInFlight:   maxInFlight,
		FailurePolicy: failure,
		Clock:         clock,
	}
	return scheduler.New(ctx, queue, store, manager, run, telemetryDisp, diag, health, logger, schedBindings, opts)
}

func TestRemoteCoordinatorPoolMetadataEncoding(t *testing.T) {
	ctx := context.Background()
	diag := &diagCollector{}

	spBinding := &SpawnerBinding{Name: "remote-spawner", Cfg: bootstrap.SpawnerConfig{Name: "remote-spawner"}}
	binding := &Binding{
		Pool: pools.NewLocal("remote", state.Selector{}, 1),
		Remote: &RemoteBinding{
			Spawner: spBinding,
			PoolCfg: bootstrap.PoolConfig{
				Name:  "remote",
				Slots: 1,
				Metadata: map[string]any{
					"limits":  map[string]any{"cpu": 2},
					"weights": []string{"a", "b"},
					"enabled": true,
				},
			},
		},
	}

	rc, _ := buildRemoteCoordinatorForTest(t, ctx, diag, []*Binding{binding})
	specs := rc.PoolSpecsForSpawnerForTest(spBinding)
	if len(specs) != 1 {
		t.Fatalf("expected 1 pool spec, got %d", len(specs))
	}
	meta := specs[0].Metadata
	if meta["limits"] != `{"cpu":2}` {
		t.Fatalf("expected JSON encoded limits metadata, got %q", meta["limits"])
	}
	if meta["weights"] != `["a","b"]` {
		t.Fatalf("expected JSON encoded weights metadata, got %q", meta["weights"])
	}
	if meta["enabled"] != "true" {
		t.Fatalf("expected boolean metadata encoded as true, got %q", meta["enabled"])
	}

	t.Run("panic on unencodable metadata", func(t *testing.T) {
		badDiag := &diagCollector{}
		badBinding := &Binding{
			Pool: pools.NewLocal("bad-remote", state.Selector{}, 1),
			Remote: &RemoteBinding{
				Spawner: spBinding,
				PoolCfg: bootstrap.PoolConfig{
					Name:  "bad-remote",
					Slots: 1,
					Metadata: map[string]any{
						"invalid": make(chan int),
					},
				},
			},
		}

		rcBad, _ := buildRemoteCoordinatorForTest(t, ctx, badDiag, []*Binding{badBinding})
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic when encoding invalid metadata")
			}
		}()
		rcBad.PoolSpecsForSpawnerForTest(spBinding)
	})
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

	spBinding := &SpawnerBinding{Name: "remote-spawner", Cfg: bootstrap.SpawnerConfig{Name: "remote-spawner"}}
	bindingA := &Binding{
		Pool: pools.NewLocal("remote-a", state.Selector{}, 1),
		Remote: &RemoteBinding{
			Spawner: spBinding,
			PoolCfg: bootstrap.PoolConfig{Name: "remote-a", Slots: 1, Tags: []string{"remote"}},
		},
	}
	bindingB := &Binding{
		Pool: pools.NewLocal("remote-b", state.Selector{}, 1),
		Remote: &RemoteBinding{
			Spawner: spBinding,
			PoolCfg: bootstrap.PoolConfig{Name: "remote-b", Slots: 2, Tags: []string{"remote"}},
		},
	}

	rc, health := buildRemoteCoordinatorForTest(t, ctx, diag, []*Binding{bindingA, bindingB})
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

	pools := rc.PoolsForTest()
	if pools["remote-a"].Worker == nil || pools["remote-b"].Worker == nil {
		t.Fatalf("expected workers assigned for both pools")
	}

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

	spBinding := &SpawnerBinding{Name: "remote-spawner", Cfg: bootstrap.SpawnerConfig{Name: "remote-spawner"}}
	binding := &Binding{
		Pool: pools.NewLocal("remote-a", state.Selector{}, 1),
		Remote: &RemoteBinding{
			Spawner: spBinding,
			PoolCfg: bootstrap.PoolConfig{Name: "remote-a", Slots: 1},
		},
	}

	rc, health := buildRemoteCoordinatorForTest(t, ctx, diag, []*Binding{binding})
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

	spBinding := &SpawnerBinding{Name: "remote-spawner", Cfg: bootstrap.SpawnerConfig{Name: "remote-spawner"}}
	bindingA := &Binding{
		Pool: pools.NewLocal("remote-a", state.Selector{}, 1),
		Remote: &RemoteBinding{
			Spawner: spBinding,
			PoolCfg: bootstrap.PoolConfig{Name: "remote-a", Slots: 1},
		},
	}
	bindingB := &Binding{
		Pool: pools.NewLocal("remote-b", state.Selector{}, 1),
		Remote: &RemoteBinding{
			Spawner: spBinding,
			PoolCfg: bootstrap.PoolConfig{Name: "remote-b", Slots: 1},
		},
	}

	rc, health := buildRemoteCoordinatorForTest(t, ctx, diag, []*Binding{bindingA, bindingB})
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
		spBinding := &SpawnerBinding{Name: "remote-spawner", Cfg: bootstrap.SpawnerConfig{
			Name:     "remote-spawner",
			Metadata: map[string]string{"region": "us-east"},
			Tags:     []string{"remote"},
		}}
		binding := &Binding{
			Pool: pools.NewLocal("remote", state.Selector{}, 1),
			Remote: &RemoteBinding{
				Spawner: spBinding,
				PoolCfg: bootstrap.PoolConfig{Name: "remote", Slots: 1},
			},
		}
		rc, _ := buildRemoteCoordinatorForTest(t, ctx, diag, []*Binding{binding})
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
		pools := rc.PoolsForTest()
		workerMeta := pools["remote"].Worker.MetadataForTest()
		if workerMeta["region"] != "us-east" {
			t.Fatalf("expected worker metadata region preserved, got %v", workerMeta)
		}
		if workerMeta["tags"] != "remote" {
			t.Fatalf("expected worker metadata tags preserved, got %v", workerMeta)
		}
	})

	t.Run("missing metadata emits diagnostic", func(t *testing.T) {
		diag := &diagCollector{}
		spBinding := &SpawnerBinding{Name: "remote-spawner", Cfg: bootstrap.SpawnerConfig{
			Name:     "remote-spawner",
			Metadata: map[string]string{"region": "us-west"},
		}}
		binding := &Binding{
			Pool: pools.NewLocal("remote", state.Selector{}, 1),
			Remote: &RemoteBinding{
				Spawner: spBinding,
				PoolCfg: bootstrap.PoolConfig{Name: "remote", Slots: 1},
			},
		}
		rc, _ := buildRemoteCoordinatorForTest(t, ctx, diag, []*Binding{binding})
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

func TestRemoteCoordinatorValidation(t *testing.T) {
	ctx := context.Background()
	queue := state.NewMemoryQueue()
	store := state.NewMemoryStore()
	manager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}

	base := node.New(ctx, nil, node.TelemetryDispatcherConfig{})
	health := node.NewHealthDispatcher()
	binary := testBinaryPath(t)

	newBindings := func(sp *SpawnerBinding) []*Binding {
		return []*Binding{
			{
				Pool: pools.NewLocal("remote", state.Selector{}, 1),
				Remote: &RemoteBinding{
					Spawner: sp,
					PoolCfg: bootstrap.PoolConfig{Name: "remote", Slots: 1},
				},
			},
		}
	}

	makeDispatcher := func(diag scheduler.DiagnosticsWriter, bindings []*Binding) *scheduler.Dispatcher {
		return newTestDispatcher(ctx, queue, nil, manager, nil, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, 0, 0, 0, nil, bindings, time.Now)
	}

	t.Run("missing auth token", func(t *testing.T) {
		bindings := newBindings(&SpawnerBinding{Name: "remote", Cfg: bootstrap.SpawnerConfig{Name: "remote", BinaryPath: binary}})
		diag := &diagCollector{}
		dispatcher := makeDispatcher(diag, bindings)
		_, err := NewRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, bindings, time.Now, RemoteBridgeConfig{})
		if err == nil || !errors.Is(err, ErrMissingAuthToken) {
			t.Fatalf("expected missing auth token error, got %v", err)
		}
		events := diag.Events()
		found := false
		for _, evt := range events {
			if evt.Component == "remote.bootstrap" && evt.Metadata["missing_auth_token"] == true {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected diagnostics for missing auth token, got %v", events)
		}
	})

	t.Run("partial TLS configuration", func(t *testing.T) {
		bindings := newBindings(&SpawnerBinding{Name: "remote-tls", Cfg: bootstrap.SpawnerConfig{
			Name:       "remote-tls",
			BinaryPath: binary,
			AuthToken:  "secret",
			TLS: TLSFiles{
				CertPath: "cert.pem",
			},
		}})
		diag := &diagCollector{}
		dispatcher := makeDispatcher(diag, bindings)
		_, err := NewRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, bindings, time.Now, RemoteBridgeConfig{})
		if err == nil || !errors.Is(err, ErrMissingTLSPair) {
			t.Fatalf("expected TLS validation error, got %v", err)
		}
		events := diag.Events()
		found := false
		for _, evt := range events {
			missing, ok := evt.Metadata["missing_tls"].([]string)
			if !ok {
				continue
			}
			if len(missing) > 0 {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected missing TLS diagnostic, got %v", events)
		}
	})

	t.Run("missing TLS CA", func(t *testing.T) {
		bindings := newBindings(&SpawnerBinding{Name: "remote-tls-ca", Cfg: bootstrap.SpawnerConfig{
			Name:       "remote-tls-ca",
			BinaryPath: binary,
			AuthToken:  "secret",
			TLS: TLSFiles{
				CertPath: "cert.pem",
				KeyPath:  "key.pem",
			},
		}})
		diag := &diagCollector{}
		dispatcher := makeDispatcher(diag, bindings)
		_, err := NewRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, bindings, time.Now, RemoteBridgeConfig{})
		if err == nil || !errors.Is(err, ErrMissingTLSCA) {
			t.Fatalf("expected missing TLS CA error, got %v", err)
		}
	})

}
