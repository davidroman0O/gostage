package testing

import (
	"context"
	"testing"

	"github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/layers/foundation/cleanup"
	rt "github.com/davidroman0O/gostage/v3/shared/runtime"
	"github.com/davidroman0O/gostage/v3/shared/store"
	"github.com/davidroman0O/gostage/v3/shared/workflow"
)

func TestNewTestNode(t *testing.T) {
	ctx := context.Background()
	node, diag, err := NewTestNode(ctx)
	if err != nil {
		t.Fatalf("NewTestNode: %v", err)
	}
	defer cleanup.SafeClose(node)

	if node == nil {
		t.Fatal("node is nil")
	}
	if diag == nil {
		t.Fatal("diagnostics channel is nil")
	}
}

func TestNewTestNodeWithOptions(t *testing.T) {
	ctx := context.Background()
	node, _, err := NewTestNode(ctx, gostage.WithWorkerPool(gostage.WorkerPoolConfig{Workers: 2}))
	if err != nil {
		t.Fatalf("NewTestNode with options: %v", err)
	}
	defer cleanup.SafeClose(node)

	if node == nil {
		t.Fatal("node is nil")
	}
}

func TestNewTestWorkflow(t *testing.T) {
	def := NewTestWorkflow("test-id", workflow.Stage{
		Name: "stage1",
		Actions: []workflow.Action{
			{Ref: "test.action"},
		},
	})

	if def.ID != "test-id" {
		t.Fatalf("expected ID 'test-id', got %q", def.ID)
	}
	if len(def.Stages) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(def.Stages))
	}
}

func TestNewTestWorkflowAutoID(t *testing.T) {
	def := NewTestWorkflow("", workflow.Stage{
		Name: "stage1",
	})

	if def.ID == "" {
		t.Fatal("expected auto-generated ID")
	}
	if !startsWith(def.ID, "test-workflow-") {
		t.Fatalf("expected ID to start with 'test-workflow-', got %q", def.ID)
	}
}

func TestExecuteTestWorkflow(t *testing.T) {
	ctx := context.Background()

	// Register an action
	gostage.MustRegisterAction("test.echo", func(_ rt.Context) error {
		return nil
	})

	// Create test node
	node, _, err := NewTestNode(ctx)
	if err != nil {
		t.Fatalf("NewTestNode: %v", err)
	}
	defer cleanup.SafeClose(node)

	// Create workflow
	def := NewTestWorkflow("test-workflow", workflow.Stage{
		Name: "stage1",
		Actions: []workflow.Action{
			{Ref: "test.echo"},
		},
	})

	// Execute workflow
	result, err := ExecuteTestWorkflow(node, def)
	if err != nil {
		t.Fatalf("ExecuteTestWorkflow: %v", err)
	}

	if !result.Success {
		t.Fatalf("expected success, got %+v", result)
	}
}

func TestExecuteTestWorkflowWithOptions(t *testing.T) {
	ctx := context.Background()

	gostage.MustRegisterAction("test.with-store", func(ctx rt.Context) error {
		val, err := store.Get[string](ctx, "test-key")
		if err != nil {
			return err
		}
		if val != "test-value" {
			t.Errorf("expected 'test-value', got %v", val)
		}
		return nil
	})

	node, _, err := NewTestNode(ctx)
	if err != nil {
		t.Fatalf("NewTestNode: %v", err)
	}
	defer cleanup.SafeClose(node)

	def := NewTestWorkflow("test-workflow", workflow.Stage{
		Name: "stage1",
		Actions: []workflow.Action{
			{Ref: "test.with-store"},
		},
	})

	result, err := ExecuteTestWorkflow(node, def, gostage.WithInitialStore(map[string]any{
		"test-key": "test-value",
	}))
	if err != nil {
		t.Fatalf("ExecuteTestWorkflow: %v", err)
	}

	if !result.Success {
		t.Fatalf("expected success, got %+v", result)
	}
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
