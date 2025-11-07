package gostage_test

import (
	"context"
	"testing"

	"github.com/davidroman0O/gostage/v3/e2e/testkit"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/workflow"

	gostage "github.com/davidroman0O/gostage/v3"
)

func TestWithWorkerPool(t *testing.T) {
	ctx := context.Background()
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("test.echo", func(ctx rt.Context) error {
		return nil
	})

	def := workflow.Definition{
		ID: "test-workflow",
		Stages: []workflow.Stage{
			{
				Name: "stage1",
				Actions: []workflow.Action{
					{Ref: "test.echo"},
				},
			},
		},
	}

	backends := testkit.NewMemoryBackends()
	node, _, err := gostage.Run(ctx, append(testkit.MemoryOptions(backends),
		gostage.WithWorkerPool(gostage.WorkerPoolConfig{
			Name:    "test-pool",
			Workers: 2,
			Tags:    []string{"test"},
		}))...)
	if err != nil {
		t.Fatalf("Run with WithWorkerPool: %v", err)
	}
	defer node.Close()

	workflowID, _ := gostage.MustRegisterWorkflow(def)
	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID), gostage.WithTags("test"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty run ID")
	}
}

func TestWithPoolBackwardCompatibility(t *testing.T) {
	ctx := context.Background()
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("test.echo", func(ctx rt.Context) error {
		return nil
	})

	def := workflow.Definition{
		ID: "test-workflow",
		Stages: []workflow.Stage{
			{
				Name: "stage1",
				Actions: []workflow.Action{
					{Ref: "test.echo"},
				},
			},
		},
	}

	backends := testkit.NewMemoryBackends()
	// Test backward compatibility: old WithPool with Slots still works
	node, _, err := gostage.Run(ctx, append(testkit.MemoryOptions(backends),
		gostage.WithPool(gostage.PoolConfig{
			Name:  "test-pool",
			Slots: 2,
			Tags:  []string{"test"},
		}))...)
	if err != nil {
		t.Fatalf("Run with WithPool (backward compat): %v", err)
	}
	defer node.Close()

	workflowID, _ := gostage.MustRegisterWorkflow(def)
	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID), gostage.WithTags("test"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty run ID")
	}
}

func TestWorkersTakesPrecedenceOverSlots(t *testing.T) {
	ctx := context.Background()
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("test.echo", func(ctx rt.Context) error {
		return nil
	})

	backends := testkit.NewMemoryBackends()
	// Test that Workers takes precedence when both are set
	node, _, err := gostage.Run(ctx, append(testkit.MemoryOptions(backends),
		gostage.WithWorkerPool(gostage.WorkerPoolConfig{
			Name:    "test-pool",
			Workers: 5,
			Slots:   2, // Should be ignored
			Tags:    []string{"test"},
		}))...)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	defer node.Close()

	stats, err := node.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if len(stats.Pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(stats.Pools))
	}
	if stats.Pools[0].Slots != 5 {
		t.Fatalf("expected 5 slots (from Workers), got %d", stats.Pools[0].Slots)
	}
}

func TestWithSimpleWorkerPool(t *testing.T) {
	ctx := context.Background()
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("test.echo", func(ctx rt.Context) error {
		return nil
	})

	def := workflow.Definition{
		ID: "test-workflow",
		Stages: []workflow.Stage{
			{
				Name: "stage1",
				Actions: []workflow.Action{
					{Ref: "test.echo"},
				},
			},
		},
	}

	backends := testkit.NewMemoryBackends()
	node, _, err := gostage.Run(ctx, append(testkit.MemoryOptions(backends),
		gostage.WithSimpleWorkerPool("test-pool", 3, "test"))...)
	if err != nil {
		t.Fatalf("Run with WithSimpleWorkerPool: %v", err)
	}
	defer node.Close()

	workflowID, _ := gostage.MustRegisterWorkflow(def)
	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID), gostage.WithTags("test"))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty run ID")
	}
}

func TestWithDefaultWorkerPool(t *testing.T) {
	ctx := context.Background()
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("test.echo", func(ctx rt.Context) error {
		return nil
	})

	def := workflow.Definition{
		ID: "test-workflow",
		Stages: []workflow.Stage{
			{
				Name: "stage1",
				Actions: []workflow.Action{
					{Ref: "test.echo"},
				},
			},
		},
	}

	backends := testkit.NewMemoryBackends()
	node, _, err := gostage.Run(ctx, append(testkit.MemoryOptions(backends),
		gostage.WithDefaultWorkerPool(4))...)
	if err != nil {
		t.Fatalf("Run with WithDefaultWorkerPool: %v", err)
	}
	defer node.Close()

	workflowID, _ := gostage.MustRegisterWorkflow(def)
	// Default pool accepts all workflows (no tags needed)
	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty run ID")
	}
}

func TestWithChildWorkerPool(t *testing.T) {
	testkit.ResetRegistry(t)

	// Test that WithChildWorkerPool can be used
	gostage.HandleChild(func(ctx context.Context, child gostage.ChildNode) error {
		// Child handler - just verify it compiles
		_ = ctx
		_ = child
		return nil
	}, gostage.WithChildWorkerPool(gostage.WorkerPoolConfig{
		Name:    "child-pool",
		Workers: 2,
		Tags:    []string{"child"},
	}))

	// This test just verifies the API compiles and works
	// Full child process testing happens in Phase 4
}

