package gostage

import (
	"context"
	"errors"
	"testing"

	"github.com/davidroman0O/gostage/v3/child"
)

func resetChildRegistrations() {
	childHandlersMu.Lock()
	defaultChildHandler = nil
	namedChildHandlers = make(map[string]*childHandlerRegistration)
	childHandlersMu.Unlock()
}

func TestRunChildModeExecutesHandler(t *testing.T) {
	t.Cleanup(func() {
		childDetect = child.Detect
		resetChildRegistrations()
	})

	sentinelErr := errors.New("sentinel")
	childDetect = func(args []string, getenv child.GetenvFunc) (child.Config, bool, error) {
		return child.Config{Address: "127.0.0.1:12345"}, true, nil
	}

	called := false
	HandleChild(func(ctx context.Context, node ChildNode) error {
		if node == nil {
			t.Fatalf("expected non-nil child node")
		}
		called = true
		return sentinelErr
	})

	node, diag, err := Run(context.Background())
	if node != nil || diag != nil {
		t.Fatalf("expected no node/diagnostics in child mode")
	}
	if !errors.Is(err, sentinelErr) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	if !called {
		t.Fatalf("child handler was not invoked")
	}
}

func TestMergeChildConfigUsesOptions(t *testing.T) {
	logger := &stubLogger{}
	reg := &childHandlerRegistration{
		options: childOptions{
			pools: []PoolConfig{{
				Name:     "remote",
				Slots:    2,
				Tags:     []string{"payments"},
				Metadata: map[string]any{"zone": "us-east"},
			}},
			metadata: map[string]string{
				"region":  "us",
				"cluster": "blue",
			},
			tls: &TLSFiles{
				CertPath: "/tmp/cert.pem",
				KeyPath:  "/tmp/key.pem",
				CAPath:   "/tmp/ca.pem",
			},
			authToken: ptrToString("override-token"),
			logger:    logger,
			tags:      []string{"remote", "child"},
		},
	}

	cfg := mergeChildConfig(child.Config{}, reg)
	if len(cfg.Pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(cfg.Pools))
	}
	if cfg.Pools[0].Name != "remote" || cfg.Pools[0].Slots != 2 {
		t.Fatalf("unexpected pool spec: %+v", cfg.Pools[0])
	}
	if len(cfg.Pools[0].Tags) != 1 || cfg.Pools[0].Tags[0] != "payments" {
		t.Fatalf("unexpected tags: %+v", cfg.Pools[0].Tags)
	}
	if cfg.Metadata["region"] != "us" {
		t.Fatalf("expected metadata applied, got %v", cfg.Metadata)
	}
	if cfg.AuthToken != "override-token" {
		t.Fatalf("expected auth token override, got %q", cfg.AuthToken)
	}
	if cfg.TLS.CertPath != "/tmp/cert.pem" || cfg.TLS.KeyPath != "/tmp/key.pem" || cfg.TLS.CAPath != "/tmp/ca.pem" {
		t.Fatalf("unexpected TLS override: %+v", cfg.TLS)
	}
	if cfg.Logger != logger {
		t.Fatalf("expected logger override")
	}
	if len(cfg.Tags) != 2 {
		t.Fatalf("expected tags override, got %+v", cfg.Tags)
	}

	base := child.Config{
		Pools:     []child.PoolSpec{{Name: "existing"}},
		Metadata:  map[string]string{"region": "override"},
		AuthToken: "base-token",
		TLS: child.TLSConfig{
			CertPath: "base-cert",
			KeyPath:  "base-key",
			CAPath:   "base-ca",
		},
		Logger: &stubLogger{},
		Tags:   []string{"existing"},
	}
	merged := mergeChildConfig(base, reg)
	if len(merged.Pools) != 1 || merged.Pools[0].Name != "existing" {
		t.Fatalf("expected existing pools to remain, got %+v", merged.Pools)
	}
	if merged.Metadata["region"] != "override" {
		t.Fatalf("expected existing metadata to remain, got %v", merged.Metadata)
	}
	if merged.Metadata["cluster"] != "blue" {
		t.Fatalf("expected handler metadata to augment, got %v", merged.Metadata)
	}
	if merged.AuthToken != "override-token" {
		t.Fatalf("expected auth token override, got %q", merged.AuthToken)
	}
	if merged.TLS.CertPath != "/tmp/cert.pem" || merged.TLS.KeyPath != "/tmp/key.pem" || merged.TLS.CAPath != "/tmp/ca.pem" {
		t.Fatalf("expected TLS override, got %+v", merged.TLS)
	}
	if merged.Logger != base.Logger {
		t.Fatalf("expected existing logger to remain")
	}
	if len(merged.Tags) != 3 {
		t.Fatalf("expected tags merged, got %+v", merged.Tags)
	}
}

func ptrToString(s string) *string {
	return &s
}

type stubLogger struct{}

func (stubLogger) Debug(string, ...interface{}) {}
func (stubLogger) Info(string, ...interface{})  {}
func (stubLogger) Warn(string, ...interface{})  {}
func (stubLogger) Error(string, ...interface{}) {}
