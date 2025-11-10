package workflow

import (
	"context"
	"reflect"
	"testing"

	"github.com/davidroman0O/gostage/v3/metadata"
	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
)

func TestDefinitionJSONRoundTrip(t *testing.T) {
	meta := metadata.New()
	meta.Set("foo", "bar")
	def := Definition{
		ID:         "wf-json",
		Name:       "JSON Workflow",
		Tags:       []string{"a", "b"},
		Metadata:   meta,
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

type noopRuntimeAction struct {
	name string
	tags []string
}

func (n noopRuntimeAction) Name() string             { return n.name }
func (n noopRuntimeAction) Description() string      { return "" }
func (n noopRuntimeAction) Tags() []string           { return append([]string(nil), n.tags...) }
func (n noopRuntimeAction) Execute(rt.Context) error { return nil }

func TestRuntimeStageActionMutationToggles(t *testing.T) {
	stage := NewRuntimeStage("stage-1", "Stage", "")
	stage.AddActions(
		noopRuntimeAction{name: "action-1", tags: []string{"grp"}},
		noopRuntimeAction{name: "action-2", tags: []string{"grp"}},
		noopRuntimeAction{name: "action-3", tags: []string{"other"}},
	)

	mutation := stage.Actions()
	if !mutation.IsEnabled("action-1") {
		t.Fatalf("expected action-1 enabled by default")
	}

	mutation.Disable("action-1")
	if mutation.IsEnabled("action-1") {
		t.Fatalf("expected action-1 disabled")
	}

	state := stage.RuntimeState()
	if _, ok := state.Disabled["action-1"]; !ok {
		t.Fatalf("expected runtime state to record disabled action")
	}

	wf := NewRuntimeWorkflow("wf-runtime", "wf", "")
	wf.AddStage(stage)

	if actions, _ := wf.DisabledSnapshot(); actions == nil || !actions["action-1"] {
		t.Fatalf("expected workflow snapshot to include disabled action")
	}

	mutation.Enable("action-1")
	if !mutation.IsEnabled("action-1") {
		t.Fatalf("expected action-1 re-enabled")
	}

	if actions, _ := wf.DisabledSnapshot(); actions != nil {
		if actions["action-1"] {
			t.Fatalf("expected workflow snapshot not to include action-1 after enable")
		}
	}

	if disabled := mutation.DisableByTags([]string{"grp"}); disabled != 2 {
		t.Fatalf("expected two actions disabled by tag, got %d", disabled)
	}
	if !mutation.IsEnabled("action-3") {
		t.Fatalf("expected action-3 to remain enabled")
	}

	if enabled := mutation.EnableByTags([]string{"grp"}); enabled != 2 {
		t.Fatalf("expected two actions re-enabled by tag, got %d", enabled)
	}
	if !mutation.IsEnabled("action-2") {
		t.Fatalf("expected action-2 re-enabled")
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

func TestEnsureIDsDeterministic(t *testing.T) {
	def := Definition{
		Stages: []Stage{
			{
				Name: "prep",
				Actions: []Action{
					{Ref: "noop"},
					{Ref: "noop"},
				},
			},
			{
				Name: "run",
				Actions: []Action{
					{Ref: "noop"},
				},
			},
		},
	}

	first, assign1, err := EnsureIDs(def)
	if err != nil {
		t.Fatalf("ensure ids (first): %v", err)
	}
	second, assign2, err := EnsureIDs(def)
	if err != nil {
		t.Fatalf("ensure ids (second): %v", err)
	}

	if len(first.Stages) != len(second.Stages) {
		t.Fatalf("stage length mismatch: %d vs %d", len(first.Stages), len(second.Stages))
	}
	for i := range first.Stages {
		if first.Stages[i].ID != second.Stages[i].ID {
			t.Fatalf("stage %d id mismatch: %s vs %s", i, first.Stages[i].ID, second.Stages[i].ID)
		}
		for j := range first.Stages[i].Actions {
			if first.Stages[i].Actions[j].ID != second.Stages[i].Actions[j].ID {
				t.Fatalf("action id mismatch at stage %d action %d: %s vs %s",
					i, j, first.Stages[i].Actions[j].ID, second.Stages[i].Actions[j].ID)
			}
		}
		if assign1.Stages[i].ID != assign2.Stages[i].ID {
			t.Fatalf("assignment stage id mismatch: %s vs %s", assign1.Stages[i].ID, assign2.Stages[i].ID)
		}
		for j := range assign1.Stages[i].Actions {
			if assign1.Stages[i].Actions[j].ID != assign2.Stages[i].Actions[j].ID {
				t.Fatalf("assignment action id mismatch at stage %d action %d: %s vs %s",
					i, j, assign1.Stages[i].Actions[j].ID, assign2.Stages[i].Actions[j].ID)
			}
		}
	}
}
