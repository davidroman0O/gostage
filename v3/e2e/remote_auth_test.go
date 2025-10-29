package e2e

import (
	"context"
	"errors"
	"os"
	"testing"

	gostage "github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/bootstrap"
)

func TestRemoteAuthValidationMissingToken(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	binary, err := os.Executable()
	if err != nil {
		t.Fatalf("executable path: %v", err)
	}

	_, _, err = gostage.Run(ctx,
		gostage.WithPool(gostage.PoolConfig{Name: "remote", Slots: 1, Spawner: "remote"}),
		gostage.WithSpawner(gostage.SpawnerConfig{
			Name:       "remote",
			BinaryPath: binary,
		}),
	)
	if err == nil || !errors.Is(err, gostage.ErrRemoteMissingAuthToken) {
		t.Fatalf("expected missing auth token error, got %v", err)
	}
}

func TestRemoteAuthValidationPartialTLS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	binary, err := os.Executable()
	if err != nil {
		t.Fatalf("executable path: %v", err)
	}

	_, _, err = gostage.Run(ctx,
		gostage.WithPool(gostage.PoolConfig{Name: "remote", Slots: 1, Spawner: "remote"}),
		gostage.WithSpawner(gostage.SpawnerConfig{
			Name:       "remote",
			BinaryPath: binary,
			AuthToken:  "secret",
			TLS:        gostage.TLSFiles{CertPath: "cert-only.pem"},
		}),
	)
	if err == nil || (!errors.Is(err, gostage.ErrRemoteMissingTLSPair) && !errors.Is(err, bootstrap.ErrInvalidTLSConfig)) {
		t.Fatalf("expected TLS validation error, got %v", err)
	}
}

func TestRemoteAuthValidationMissingTLSCA(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	binary, err := os.Executable()
	if err != nil {
		t.Fatalf("executable path: %v", err)
	}

	_, _, err = gostage.Run(ctx,
		gostage.WithPool(gostage.PoolConfig{Name: "remote", Slots: 1, Spawner: "remote"}),
		gostage.WithSpawner(gostage.SpawnerConfig{
			Name:       "remote",
			BinaryPath: binary,
			AuthToken:  "secret",
			TLS:        gostage.TLSFiles{CertPath: "cert.pem", KeyPath: "key.pem"},
		}),
	)
	if err == nil || !errors.Is(err, gostage.ErrRemoteMissingTLSCA) {
		t.Fatalf("expected missing TLS CA error, got %v", err)
	}
}
