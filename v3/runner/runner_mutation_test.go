package runner

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/davidroman0O/gostage/v3/broker"
	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/runtime/local"
	"github.com/davidroman0O/gostage/v3/state"
	otel "github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestRunnerRecordsRemovedStageAndAction(t *testing.T) {
	registry.SetDefault(registry.NewSafeRegistry())

	register := func(name string, fn func(rt.Context) error) {
		if err := registry.Default().RegisterAction(name, fn, registry.ActionMetadata{}); err != nil {
			t.Fatalf("register action %s: %v", name, err)
		}
	}

	var dynamicStageID string

	register("remove-action", func(ctx rt.Context) error {
		stage := workflow.NewRuntimeStage("dynamic", "Dynamic stage", "")
		stage.AddActions(workflow.MustRuntimeAction("target-noop"))
		dynamicStageID = ctx.Stages().Add(stage)
		ctx.Actions().Remove("noop")
		ctx.Stages().Remove(dynamicStageID)
		return nil
	})
	register("noop", func(rt.Context) error { return nil })
	register("target-noop", func(rt.Context) error { return nil })

	def := workflow.Definition{
		ID: "wf",
		Stages: []workflow.Stage{
			{
				ID:   "control",
				Name: "control",
				Actions: []workflow.Action{
					{ID: "remove-action", Ref: "remove-action"},
					{ID: "noop", Ref: "noop"},
				},
			},
			{
				ID:   "target",
				Name: "target",
				Actions: []workflow.Action{
					{ID: "target-noop", Ref: "target-noop"},
				},
			},
		},
	}
	def, _, err := workflow.EnsureIDs(def)
	if err != nil {
		t.Fatalf("ensure ids: %v", err)
	}

	rtWorkflow, err := workflow.Materialize(def, registry.Default())
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}

	store := state.NewMemoryStore()
	manager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}
	run := New(local.Factory{}, registry.Default(), broker.NewLocal(manager), WithDefaultLogger(otel.NoopLogger{}))

	result := run.Run(rtWorkflow, RunOptions{Context: context.Background()})
	if !result.Success {
		t.Fatalf("expected success, got error %v", result.Error)
	}
	if len(result.RemovedStages) != 1 {
		t.Fatalf("unexpected removed stages: %+v", result.RemovedStages)
	}
	if createdBy, ok := result.RemovedStages[dynamicStageID]; !ok || createdBy != "control::remove-action" {
		t.Fatalf("unexpected dynamic stage removal metadata: id=%s map=%+v", dynamicStageID, result.RemovedStages)
	}
	if len(result.RemovedActions) != 1 || result.RemovedActions["control::noop"] != "control::remove-action" {
		t.Fatalf("unexpected removed actions: %+v", result.RemovedActions)
	}
}

func TestRunnerRecordsDisabledStageAndAction(t *testing.T) {
	registry.SetDefault(registry.NewSafeRegistry())

	var actionRun, followupRun atomic.Bool

	if err := registry.Default().RegisterAction("disable", func(ctx rt.Context) error {
		ctx.Actions().Disable("to-run")
		ctx.Stages().Disable("followup")
		return nil
	}, registry.ActionMetadata{}); err != nil {
		t.Fatalf("register disable: %v", err)
	}
	if err := registry.Default().RegisterAction("to-run", func(ctx rt.Context) error {
		actionRun.Store(true)
		return nil
	}, registry.ActionMetadata{}); err != nil {
		t.Fatalf("register to-run: %v", err)
	}
	if err := registry.Default().RegisterAction("followup-action", func(ctx rt.Context) error {
		followupRun.Store(true)
		return nil
	}, registry.ActionMetadata{}); err != nil {
		t.Fatalf("register followup: %v", err)
	}

	def := workflow.Definition{
		ID: "wf-disable",
		Stages: []workflow.Stage{
			{
				ID:   "control",
				Name: "control",
				Actions: []workflow.Action{
					{ID: "disable", Ref: "disable"},
					{ID: "to-run", Ref: "to-run"},
				},
			},
			{
				ID:   "followup",
				Name: "followup",
				Actions: []workflow.Action{
					{ID: "followup-action", Ref: "followup-action"},
				},
			},
		},
	}
	def, _, err := workflow.EnsureIDs(def)
	if err != nil {
		t.Fatalf("ensure ids: %v", err)
	}

	rtWorkflow, err := workflow.Materialize(def, registry.Default())
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}

	store := state.NewMemoryStore()
	manager, err := state.NewStoreManager(store)
	if err != nil {
		t.Fatalf("store manager: %v", err)
	}
	run := New(local.Factory{}, registry.Default(), broker.NewLocal(manager), WithDefaultLogger(otel.NoopLogger{}))

	result := run.Run(rtWorkflow, RunOptions{Context: context.Background()})
	if !result.Success {
		t.Fatalf("expected success, got error %v", result.Error)
	}
	if actionRun.Load() {
		t.Fatalf("disabled action unexpectedly executed")
	}
	if followupRun.Load() {
		t.Fatalf("disabled stage action unexpectedly executed")
	}
	if !result.DisabledActions["to-run"] {
		t.Fatalf("disabled actions map missing entry: %+v", result.DisabledActions)
	}
	if !result.DisabledStages["followup"] {
		t.Fatalf("disabled stages map missing entry: %+v", result.DisabledStages)
	}
	var followupStage *StageStatus
	for i := range result.Stages {
		if result.Stages[i].ID == "followup" {
			followupStage = &result.Stages[i]
			break
		}
	}
	if followupStage == nil || followupStage.Status != StatusSkipped {
		t.Fatalf("expected followup stage to be skipped, got %+v", followupStage)
	}
	for _, action := range result.Actions {
		if action.StageID == "control" && action.Name == "to-run" && action.Status != StatusSkipped {
			t.Fatalf("expected to-run action skipped, got %s", action.Status)
		}
	}
}
