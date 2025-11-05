package coordinator

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/spawner"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/telemetry"
)

func TestProcessSupervisorExhaustsRestarts(t *testing.T) {
	crashScript := writeCrashScript(t)

	ctx := context.Background()
	queue := state.NewMemoryQueue()
	base := node.New(ctx, nil, node.TelemetryDispatcherConfig{})
	health := node.NewHealthDispatcher()
	diag := &diagCollector{}

	spCfg := bootstrap.SpawnerConfig{
		Name:           "crashy",
		BinaryPath:     crashScript,
		MaxRestarts:    2,
		RestartBackoff: 10 * time.Millisecond,
		AuthToken:      "supervisor-secret",
	}

	binding := &Binding{
		Pool: pools.NewLocal("remote-crash", state.Selector{}, 1),
		Remote: &RemoteBinding{
			Spawner: &SpawnerBinding{
				Name: spCfg.Name,
				Cfg:  spCfg,
				Process: spawner.NewProcessSpawner(spawner.Config{
					BinaryPath:     spCfg.BinaryPath,
					MaxRestarts:    spCfg.MaxRestarts,
					RestartBackoff: spCfg.RestartBackoff,
				}),
			},
			PoolCfg: bootstrap.PoolConfig{Name: "remote-crash", Slots: 1},
		},
	}

	dispatcher := newTestDispatcher(ctx, queue, nil, nil, nil, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, 0, 0, 0, nil, []*Binding{binding}, time.Now)
	rc, err := NewRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, []*Binding{binding}, time.Now, RemoteBridgeConfig{BindAddress: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewRemoteCoordinator: %v", err)
	}
	defer rc.ShutdownForTest()

	if err := rc.LaunchSpawnerForTest(binding); err != nil {
		t.Fatalf("launchSpawner: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	foundCritical := false
	for time.Now().Before(deadline) {
		events := diag.Events()
		for _, evt := range events {
			if evt.Severity == diagnostics.SeverityCritical && evt.Component == "remote.spawner.crashy" {
				foundCritical = true
				break
			}
		}
		if foundCritical {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !foundCritical {
		t.Fatalf("expected critical diagnostic after exhausting restarts; diagnostics=%+v", diag.Events())
	}
}

func writeCrashScript(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("process supervision test not supported on windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "crash.sh")
	script := "#!/bin/sh\nexit 1\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write crash script: %v", err)
	}
	return path
}
