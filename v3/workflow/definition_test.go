package workflow

import (
	"context"
	"reflect"
	"testing"

	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
)

func TestDefinitionJSONRoundTrip(t *testing.T) {
	def := Definition{
		ID:         "wf-json",
		Name:       "JSON Workflow",
		Tags:       []string{"a", "b"},
		Metadata:   map[string]any{"foo": "bar"},
		Middleware: []string{"wf-log"},
		Stages: []Stage{
			{
				ID:         "stage-1",
				Middleware: []string{"stage-log"},
				Actions: []Action{
					{ID: "action-1", Ref: "noop", Middleware: []string{"action-log"}},
				},
			},
		},
	}

	data, err := ToJSON(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	round, err := FromJSON(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !reflect.DeepEqual(def, round) {
		t.Fatalf("round trip mismatch:\nwant %#v\ngot  %#v", def, round)
	}
}

func TestMaterializeFailsForUnknownMiddleware(t *testing.T) {
	resetMiddlewareRegistries()
	defer resetMiddlewareRegistries()

	reg := registry.NewSafeRegistry()
	if err := reg.RegisterAction("noop", func(rt.Context) error { return nil }, registry.ActionMetadata{}); err != nil {
		t.Fatalf("register noop: %v", err)
	}

	def := Definition{
		ID:         "wf-missing-mw",
		Middleware: []string{"missing"},
		Stages: []Stage{
			{
				ID:      "stage-1",
				Actions: []Action{{ID: "action-1", Ref: "noop"}},
			},
		},
	}

	if _, err := Materialize(def, reg); err == nil {
		t.Fatalf("expected error for missing workflow middleware")
	}
}

func TestMaterializeResolvesMiddleware(t *testing.T) {
	resetMiddlewareRegistries()
	defer resetMiddlewareRegistries()

	reg := registry.NewSafeRegistry()
	if err := reg.RegisterAction("noop", func(rt.Context) error { return nil }, registry.ActionMetadata{}); err != nil {
		t.Fatalf("register noop: %v", err)
	}

	MustRegisterWorkflowMiddleware("wf-log", func(next rt.WorkflowStageRunnerFunc) rt.WorkflowStageRunnerFunc {
		return func(ctx context.Context, stage rt.Stage, wf rt.Workflow, logger rt.Logger) error {
			return next(ctx, stage, wf, logger)
		}
	})

	MustRegisterStageMiddleware("stage-log", func(next rt.StageRunnerFunc) rt.StageRunnerFunc {
		return func(ctx context.Context, stage rt.Stage, wf rt.Workflow, logger rt.Logger) error {
			return next(ctx, stage, wf, logger)
		}
	})

	MustRegisterActionMiddleware("action-log", func(next rt.ActionRunnerFunc) rt.ActionRunnerFunc {
		return func(ctx rt.Context, act rt.Action, index int, isLast bool) error {
			return next(ctx, act, index, isLast)
		}
	})

	def := Definition{
		ID:         "wf-mw",
		Middleware: []string{"wf-log"},
		Stages: []Stage{
			{
				ID:         "stage-1",
				Middleware: []string{"stage-log"},
				Actions: []Action{
					{ID: "action-1", Ref: "noop", Middleware: []string{"action-log"}},
				},
			},
		},
	}

	wf, err := Materialize(def, reg)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}

	runtimeWF, ok := wf.(*RuntimeWorkflow)
	if !ok {
		t.Fatalf("expected *RuntimeWorkflow, got %T", wf)
	}
	if len(runtimeWF.Middlewares()) != 1 {
		t.Fatalf("expected workflow middleware resolved")
	}

	stages := runtimeWF.Stages()
	if len(stages) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(stages))
	}
	runtimeStage, ok := stages[0].(*RuntimeStage)
	if !ok {
		t.Fatalf("expected *RuntimeStage, got %T", stages[0])
	}
	if len(runtimeStage.Middlewares()) != 1 {
		t.Fatalf("expected stage middleware resolved")
	}

	actions := runtimeStage.ActionList()
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	runtimeAction, ok := actions[0].(*runtimeAction)
	if !ok {
		t.Fatalf("expected *runtimeAction, got %T", actions[0])
	}
	if len(runtimeAction.MiddlewareChain()) != 1 {
		t.Fatalf("expected action middleware resolved")
	}
}

func TestEnsureIDsGeneratesMissingValues(t *testing.T) {
	def := Definition{
		ID: "wf-auto",
		Stages: []Stage{
			{
				Name: "prepare",
				Actions: []Action{
					{Ref: "noop"},
					{Ref: "noop"},
				},
			},
			{
				ID: "custom-stage",
				Actions: []Action{
					{Ref: "noop"},
					{ID: "custom-action", Ref: "noop"},
				},
			},
		},
	}

	normalized, assignment, err := EnsureIDs(def)
	if err != nil {
		t.Fatalf("ensure ids: %v", err)
	}

	if normalized.Stages[0].ID == "" {
		t.Fatalf("expected generated stage id for first stage")
	}
	if normalized.Stages[1].ID != "custom-stage" {
		t.Fatalf("expected to preserve existing stage id, got %s", normalized.Stages[1].ID)
	}

	if len(assignment.Stages) != 2 {
		t.Fatalf("unexpected stage assignment length: %d", len(assignment.Stages))
	}
	if !assignment.Stages[0].Generated {
		t.Fatalf("expected first stage id to be marked as generated")
	}
	if assignment.Stages[1].Generated {
		t.Fatalf("expected second stage id to be marked as provided")
	}

	firstStage := normalized.Stages[0]
	if len(firstStage.Actions) != 2 {
		t.Fatalf("expected actions to remain, got %d", len(firstStage.Actions))
	}
	if firstStage.Actions[0].ID == "" || firstStage.Actions[1].ID == "" {
		t.Fatalf("expected generated action ids for first stage actions")
	}

	secondStage := normalized.Stages[1]
	if secondStage.Actions[1].ID != "custom-action" {
		t.Fatalf("expected custom action id preserved, got %s", secondStage.Actions[1].ID)
	}
	if assignment.Stages[1].Actions[0].Generated != true {
		t.Fatalf("expected generated flag for missing action id")
	}
	if assignment.Stages[1].Actions[1].Generated {
		t.Fatalf("expected custom action id to be marked as provided")
	}
}
