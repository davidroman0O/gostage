package gostage

import (
	"context"
	"errors"
	"testing"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/child"
	"github.com/davidroman0O/gostage/v3/orchestrator"
)

func resetChildRegistrations() {
	orchestrator.ResetChildRegistrationsForTest()
}

func TestRunChildModeExecutesHandler(t *testing.T) {
	originalDetect := orchestrator.ChildDetectForTest()
	t.Cleanup(func() {
		orchestrator.SetChildDetectForTest(originalDetect)
		resetChildRegistrations()
	})

	sentinelErr := errors.New("sentinel")
	orchestrator.SetChildDetectForTest(func(args []string, getenv child.GetenvFunc) (child.Config, bool, error) {
		return child.Config{Address: "127.0.0.1:12345"}, true, nil
	})

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

func TestRunChildModeMetadataConflictFails(t *testing.T) {
	originalDetect := orchestrator.ChildDetectForTest()
	t.Cleanup(func() {
		orchestrator.SetChildDetectForTest(originalDetect)
		resetChildRegistrations()
	})

	orchestrator.SetChildDetectForTest(func(args []string, getenv child.GetenvFunc) (child.Config, bool, error) {
		return child.Config{
			Address:  "127.0.0.1:12345",
			Metadata: map[string]string{"region": "parent"},
		}, true, nil
	})

	HandleChild(func(ctx context.Context, node ChildNode) error {
		t.Fatalf("handler should not execute when metadata conflicts")
		return nil
	}, bootstrap.ChildOptionFunc(func(opts *bootstrap.ChildOptions) {
		if opts.Metadata == nil {
			opts.Metadata = make(map[string]string)
		}
		opts.Metadata["region"] = "child"
	}))

	_, _, err := Run(context.Background())
	if err == nil {
		t.Fatalf("expected metadata conflict error")
	}
	if !errors.Is(err, bootstrap.ErrChildMetadataConflict) {
		t.Fatalf("expected ErrChildMetadataConflict, got %v", err)
	}
}

func TestMergeChildConfigUsesOptions(t *testing.T) {
	logger := &stubLogger{}
	reg := orchestrator.NewChildHandlerRegistration(bootstrap.ChildOptions{
		Pools: []PoolConfig{{
			Name:     "remote",
			Slots:    2,
			Tags:     []string{"payments"},
			Metadata: map[string]any{"zone": "us-east"},
		}},
		Metadata: map[string]string{
			"region":  "us",
			"cluster": "blue",
		},
		TLS: &TLSFiles{
			CertPath: "/tmp/cert.pem",
			KeyPath:  "/tmp/key.pem",
			CAPath:   "/tmp/ca.pem",
		},
		AuthToken: ptrToString("override-token"),
		Logger:    logger,
		Tags:      []string{"remote", "child"},
	})

	cfg, err := orchestrator.MergeChildConfigForTest(child.Config{}, reg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
	if cfg.Metadata["cluster"] != "blue" {
		t.Fatalf("expected metadata enriched, got %v", cfg.Metadata)
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

	t.Run("metadata scenarios", func(t *testing.T) {
		t.Run("matching value succeeds", func(t *testing.T) {
			base := child.Config{Metadata: map[string]string{"region": "us"}}
			merged, err := orchestrator.MergeChildConfigForTest(base, reg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if merged.Metadata["region"] != "us" || merged.Metadata["cluster"] != "blue" {
				t.Fatalf("expected metadata preserved and enriched, got %v", merged.Metadata)
			}
		})

		t.Run("distinct keys succeed", func(t *testing.T) {
			base := child.Config{Metadata: map[string]string{"env": "prod"}}
			merged, err := orchestrator.MergeChildConfigForTest(base, reg)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if merged.Metadata["env"] != "prod" {
				t.Fatalf("expected env metadata preserved, got %v", merged.Metadata)
			}
			if merged.Metadata["region"] != "us" {
				t.Fatalf("expected handler metadata applied, got %v", merged.Metadata)
			}
		})

		t.Run("conflicting values fail", func(t *testing.T) {
			base := child.Config{Metadata: map[string]string{"region": "eu"}}
			_, err := orchestrator.MergeChildConfigForTest(base, reg)
			if err == nil {
				t.Fatalf("expected conflict error")
			}
			if !errors.Is(err, bootstrap.ErrChildMetadataConflict) {
				t.Fatalf("expected ErrChildMetadataConflict, got %v", err)
			}
			var conflictErr *orchestrator.ChildMetadataConflictError
			if !errors.As(err, &conflictErr) {
				t.Fatalf("expected ChildMetadataConflictError, got %T", err)
			}
			conflicts := conflictErr.Conflicts()
			if len(conflicts) != 1 || conflicts[0].Key != "region" || conflicts[0].Existing != "eu" || conflicts[0].Proposed != "us" {
				t.Fatalf("unexpected conflict payload: %+v", conflicts)
			}
		})
	})

	baseLogger := &stubLogger{}
	base := child.Config{
		Pools:     []child.PoolSpec{{Name: "existing"}},
		Metadata:  map[string]string{"env": "prod"},
		AuthToken: "base-token",
		TLS: child.TLSConfig{
			CertPath: "base-cert",
			KeyPath:  "base-key",
			CAPath:   "base-ca",
		},
		Logger: baseLogger,
		Tags:   []string{"existing"},
	}
	merged, err := orchestrator.MergeChildConfigForTest(base, reg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(merged.Pools) != 1 || merged.Pools[0].Name != "existing" {
		t.Fatalf("expected existing pools to remain, got %+v", merged.Pools)
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
	if len(baseLogger.warns) != 0 {
		t.Fatalf("expected no warnings, got %+v", baseLogger.warns)
	}
}

func ptrToString(s string) *string {
	return &s
}

type logEntry struct {
	msg string
	kv  []any
}

type stubLogger struct {
	warns []logEntry
}

func (s *stubLogger) Debug(string, ...interface{}) {}
func (s *stubLogger) Info(string, ...interface{})  {}
func (s *stubLogger) Warn(msg string, kv ...interface{}) {
	s.warns = append(s.warns, logEntry{msg: msg, kv: append([]any(nil), kv...)})
}
func (s *stubLogger) Error(string, ...interface{}) {}

func containsKV(kv []any, key string, expect any) bool {
	for i := 0; i < len(kv)-1; i += 2 {
		if k, ok := kv[i].(string); ok && k == key {
			if kv[i+1] == expect {
				return true
			}
		}
	}
	return false
}
