package gostage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	gostage "github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/e2e/testkit"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestRunDuplicatePoolFails(t *testing.T) {
	ctx := context.Background()
	_, _, err := gostage.Run(ctx,
		gostage.WithPool(gostage.PoolConfig{Name: "dup", Slots: 1}),
		gostage.WithPool(gostage.PoolConfig{Name: "dup", Slots: 1}),
	)
	if !errors.Is(err, gostage.ErrDuplicatePool) {
		t.Fatalf("expected ErrDuplicatePool, got %v", err)
	}
}

func TestRunInvalidPoolSlotsFails(t *testing.T) {
	ctx := context.Background()
	_, _, err := gostage.Run(ctx,
		gostage.WithPool(gostage.PoolConfig{Name: "invalid", Slots: 0}),
	)
	if !errors.Is(err, gostage.ErrInvalidPoolConfig) {
		t.Fatalf("expected ErrInvalidPoolConfig, got %v", err)
	}
}

func TestRunUnknownSpawnerFails(t *testing.T) {
	ctx := context.Background()
	_, _, err := gostage.Run(ctx,
		gostage.WithPool(gostage.PoolConfig{Name: "remote", Slots: 1, Spawner: "missing"}),
	)
	if !errors.Is(err, gostage.ErrUnknownSpawner) {
		t.Fatalf("expected ErrUnknownSpawner, got %v", err)
	}
}

func TestRunDuplicateSpawnerFails(t *testing.T) {
	ctx := context.Background()
	_, _, err := gostage.Run(ctx,
		gostage.WithSpawner(gostage.SpawnerConfig{Name: "child", BinaryPath: "/bin/true"}),
		gostage.WithSpawner(gostage.SpawnerConfig{Name: "child", BinaryPath: "/bin/false"}),
	)
	if !errors.Is(err, gostage.ErrDuplicateSpawner) {
		t.Fatalf("expected ErrDuplicateSpawner, got %v", err)
	}
}

func TestRunInvalidSpawnerFails(t *testing.T) {
	ctx := context.Background()
	_, _, err := gostage.Run(ctx,
		gostage.WithSpawner(gostage.SpawnerConfig{Name: "child"}),
	)
	if !errors.Is(err, gostage.ErrInvalidSpawnerConfig) {
		t.Fatalf("expected ErrInvalidSpawnerConfig, got %v", err)
	}
}

func TestSubmitWithoutMatchingPool(t *testing.T) {
	actionID := t.Name() + ".action"
	gostage.MustRegisterAction(actionID, func() gostage.ActionFunc {
		return func(rt.Context) error { return nil }
	})
	workflowID, _ := gostage.MustRegisterWorkflow(workflow.Definition{
		Name: "no-match",
		Stages: []workflow.Stage{{
			Name:    "stage",
			Actions: []workflow.Action{{Ref: actionID}},
		}},
	})

	backends := testkit.NewMemoryBackends()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	node, diag, err := gostage.Run(ctx, append(testkit.MemoryOptions(backends),
		gostage.WithPool(gostage.PoolConfig{Name: "payments", Tags: []string{"payments"}, Slots: 1}),
	)...)
	if err != nil {
		t.Fatalf("run error: %v", err)
	}
	defer node.Close()
	go func() {
		for range diag {
		}
	}()

	_, err = node.Submit(ctx, gostage.WorkflowRef(workflowID), gostage.WithTags("shipping"))
	if !errors.Is(err, gostage.ErrSubmissionRejected) || !errors.Is(err, gostage.ErrNoMatchingPool) {
		t.Fatalf("expected ErrSubmissionRejected and ErrNoMatchingPool, got %v", err)
	}
}
