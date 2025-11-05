package gostage_test

import (
	"context"
	"testing"
	"time"

	gostage "github.com/davidroman0O/gostage/v3"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestRunProvidesStateReaderWithoutSQLite(t *testing.T) {
	actionID := t.Name() + ".action"
	workflowID, assignment := registerTestWorkflow(t, actionID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	node, diag, err := gostage.Run(ctx)
	if err != nil {
		t.Fatalf("gostage.Run: %v", err)
	}
	t.Cleanup(func() {
		_ = node.Close()
	})
	go drainDiagnostics(diag)

	if node.State == nil {
		t.Fatal("expected Node.State to be non-nil")
	}

	runID, err := node.Submit(ctx, gostage.WorkflowRef(workflowID))
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	if _, err := node.Wait(ctx, runID); err != nil {
		t.Fatalf("Wait: %v", err)
	}

	summary, err := node.State.WorkflowSummary(ctx, runID)
	if err != nil {
		t.Fatalf("WorkflowSummary: %v", err)
	}
	if !summary.Success {
		t.Fatalf("expected success summary, got %+v", summary)
	}

	actions, err := node.State.ActionHistory(ctx, runID)
	if err != nil {
		t.Fatalf("ActionHistory: %v", err)
	}
	if len(actions) == 0 {
		t.Fatalf("expected action history entries")
	}

	// Ensure generated IDs from registration are present in the action history.
	generated := assignment.Stages[0].Actions[0].ID
	found := false
	for _, record := range actions {
		if record.ActionID == generated {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected action ID %q in history, got %+v", generated, actions)
	}
}

func registerTestWorkflow(t *testing.T, actionID string) (string, workflow.IDAssignment) {
	t.Helper()
	if err := gostage.RegisterAction(actionID, func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			return nil
		}
	}); err != nil {
		t.Fatalf("register action: %v", err)
	}

	def := workflow.Definition{
		Name: "test",
		Stages: []workflow.Stage{
			{Actions: []workflow.Action{{Ref: actionID}}},
		},
	}

	id, assignment, err := gostage.RegisterWorkflow(def)
	if err != nil {
		t.Fatalf("register workflow: %v", err)
	}
	return id, assignment
}

func drainDiagnostics(diag <-chan gostage.DiagnosticEvent) {
	for range diag {
	}
}
