// Package testing provides testing utilities for Gostage workflows.
package testing

import (
	"context"

	"github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/e2e/testkit"
	idgen "github.com/davidroman0O/gostage/v3/internal/foundation/id"
	"github.com/davidroman0O/gostage/v3/shared/workflow"
)

// NewTestNode creates a node with in-memory backend for testing.
// It uses MemoryBackends from e2e/testkit and applies LocalDev profile defaults.
func NewTestNode(ctx context.Context, opts ...gostage.Option) (*gostage.Node, <-chan gostage.DiagnosticEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	backends := testkit.NewMemoryBackends()
	testOpts := []gostage.Option{
		gostage.WithProfile(gostage.LocalDev),
	}
	testOpts = append(testOpts, testkit.MemoryOptions(backends)...)
	testOpts = append(testOpts, opts...)
	return gostage.Run(ctx, testOpts...)
}

// ExecuteTestWorkflow executes a workflow synchronously for testing.
// It submits the workflow, waits for completion, and returns the result.
func ExecuteTestWorkflow(node *gostage.Node, def workflow.Definition, opts ...gostage.SubmitOption) (gostage.Result, error) {
	ctx := context.Background()
	id, err := node.Submit(ctx, gostage.WorkflowDefinition(def), opts...)
	if err != nil {
		return gostage.Result{}, err
	}
	return node.Wait(ctx, id)
}

// NewTestWorkflow creates a simple workflow definition for testing.
// It generates a unique ID if not provided and ensures the definition is valid.
// Uses TestGenerator for deterministic test IDs.
func NewTestWorkflow(id string, stages ...workflow.Stage) workflow.Definition {
	if id == "" {
		// Use TestGenerator for deterministic test IDs
		testGen := idgen.NewTestGenerator("test-node")
		id = "test-workflow-" + testGen.WorkflowID()
	}
	return workflow.Definition{
		ID:     id,
		Stages: stages,
	}
}
