package gostage

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

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

	spCfg := SpawnerConfig{
		Name:           "crashy",
		BinaryPath:     crashScript,
		MaxRestarts:    2,
		RestartBackoff: 10 * time.Millisecond,
	}

	binding := &poolBinding{
		pool: pools.NewLocal("remote-crash", state.Selector{}, 1),
		remote: &remoteBinding{
			spawner: &spawnerBinding{
				name: spCfg.Name,
				cfg:  spCfg,
				process: spawner.NewProcessSpawner(spawner.Config{
					BinaryPath:     spCfg.BinaryPath,
					MaxRestarts:    spCfg.MaxRestarts,
					RestartBackoff: spCfg.RestartBackoff,
				}),
			},
			poolCfg: PoolConfig{Name: "remote-crash", Slots: 1},
		},
	}

	dispatcher := newDispatcher(ctx, queue, nil, nil, nil, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, 0, 0, 0, nil, []*poolBinding{binding}, time.Now)
	rc, err := newRemoteCoordinator(ctx, dispatcher, queue, base.TelemetryDispatcher(), diag, health, telemetry.NoopLogger{}, []*poolBinding{binding}, time.Now, RemoteBridgeConfig{BindAddress: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("newRemoteCoordinator: %v", err)
	}
	defer rc.shutdown()

	if err := rc.launchSpawner(binding); err != nil {
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
