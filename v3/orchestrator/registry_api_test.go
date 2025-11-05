package orchestrator

import (
	"testing"

	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestRegisterWorkflowReturnsAssignments(t *testing.T) {
	registry.SetDefault(registry.NewSafeRegistry())

	MustRegisterAction("test.echo", func() ActionFunc {
		return func(ctx rt.Context) error { return nil }
	})

	def := workflow.Definition{
		Name: "auto-id",
		Stages: []workflow.Stage{
			{
				Name: "first",
				Actions: []workflow.Action{
					{Ref: "test.echo"},
					{ID: "custom", Ref: "test.echo"},
				},
			},
		},
	}

	id, assignment, err := RegisterWorkflow(def)
	if err != nil {
		t.Fatalf("register workflow: %v", err)
	}
	if len(assignment.Stages) != len(def.Stages) {
		t.Fatalf("unexpected stage mapping length: %d", len(assignment.Stages))
	}
	if assignment.Stages[0].ID == "" {
		t.Fatalf("expected generated stage id")
	}
	if len(assignment.Stages[0].Actions) != len(def.Stages[0].Actions) {
		t.Fatalf("unexpected action mapping length: %d", len(assignment.Stages[0].Actions))
	}
	if assignment.Stages[0].Actions[0].ID == "" || !assignment.Stages[0].Actions[0].Generated {
		t.Fatalf("expected generated action id")
	}
	if assignment.Stages[0].Actions[1].ID != "custom" || assignment.Stages[0].Actions[1].Generated {
		t.Fatalf("expected custom action id preserved: %+v", assignment.Stages[0].Actions[1])
	}

	fetched, ok := WorkflowIDs(id)
	if !ok {
		t.Fatalf("expected workflow ids available")
	}
	if fetched.Stages[0].ID != assignment.Stages[0].ID {
		t.Fatalf("workflow ids mismatch: want %s got %s", assignment.Stages[0].ID, fetched.Stages[0].ID)
	}

	fetched.Stages[0].ID = "mutated"
	again, ok := WorkflowIDs(id)
	if !ok {
		t.Fatalf("expected workflow ids available on second lookup")
	}
	if again.Stages[0].ID == "mutated" {
		t.Fatalf("expected copy of assignment to be returned")
	}

	refIDs, ok := WorkflowReferenceIDs(WorkflowRef(id))
	if !ok {
		t.Fatalf("expected ids from workflow ref")
	}
	if refIDs.Stages[0].ID != assignment.Stages[0].ID {
		t.Fatalf("workflow ref ids mismatch: want %s got %s", assignment.Stages[0].ID, refIDs.Stages[0].ID)
	}

	inline := workflow.Definition{
		Name: "inline",
		Stages: []workflow.Stage{{
			Actions: []workflow.Action{{Ref: "test.echo"}},
		}},
	}
	inlineRef := WorkflowDefinition(inline)
	inlineIDs, ok := WorkflowReferenceIDs(inlineRef)
	if !ok {
		t.Fatalf("expected ids for inline definition")
	}
	if inlineIDs.Stages[0].ID == "" {
		t.Fatalf("inline ids not populated")
	}
	resolved, err := ResolveWorkflowReferenceForTest(inlineRef)
	if err != nil {
		t.Fatalf("resolve inline: %v", err)
	}
	if resolved.Stages[0].ID != inlineIDs.Stages[0].ID {
		t.Fatalf("resolved stage id mismatch: want %s got %s", inlineIDs.Stages[0].ID, resolved.Stages[0].ID)
	}
	if resolved.Stages[0].Actions[0].ID != inlineIDs.Stages[0].Actions[0].ID {
		t.Fatalf("resolved action id mismatch: want %s got %s", inlineIDs.Stages[0].Actions[0].ID, resolved.Stages[0].Actions[0].ID)
	}
}
