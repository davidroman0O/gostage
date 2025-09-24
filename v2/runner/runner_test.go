package runner

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v2/broker"
	"github.com/davidroman0O/gostage/v2/registry"
	"github.com/davidroman0O/gostage/v2/runner/middleware"
	"github.com/davidroman0O/gostage/v2/runtime/local"
	"github.com/davidroman0O/gostage/v2/state"
	"github.com/davidroman0O/gostage/v2/state/memory"
	"github.com/davidroman0O/gostage/v2/types"
	"github.com/davidroman0O/gostage/v2/types/defaults"
)

type captureLogger struct {
	infos  []string
	warns  []string
	errors []string
}

type stubAction struct {
	name            string
	DescriptionText string
}

func (s stubAction) Name() string                { return s.name }
func (s stubAction) Description() string         { return s.DescriptionText }
func (s stubAction) Tags() []string              { return nil }
func (s stubAction) Execute(types.Context) error { return nil }

func (c *captureLogger) Debug(format string, args ...interface{}) {
}

func (c *captureLogger) Info(format string, args ...interface{}) {
	c.infos = append(c.infos, fmt.Sprintf(format, args...))
}

func (c *captureLogger) Warn(format string, args ...interface{}) {
	c.warns = append(c.warns, fmt.Sprintf(format, args...))
}

func (c *captureLogger) Error(format string, args ...interface{}) {
	c.errors = append(c.errors, fmt.Sprintf(format, args...))
}

type recordingBroker struct {
	progressCalls []struct {
		workflowID string
		stageID    string
		actionName string
		percent    int
		message    string
	}
	summaries []state.ExecutionReport
}

type failingSummaryBroker struct {
	recordingBroker
	returnErr error
}

func (r *recordingBroker) WorkflowRegistered(context.Context, state.WorkflowRecord) error { return nil }
func (r *recordingBroker) WorkflowStatus(context.Context, string, state.WorkflowState) error {
	return nil
}
func (r *recordingBroker) StageRegistered(context.Context, string, state.StageRecord) error {
	return nil
}
func (r *recordingBroker) StageStatus(context.Context, string, string, state.WorkflowState) error {
	return nil
}
func (r *recordingBroker) ActionRegistered(context.Context, string, string, state.ActionRecord) error {
	return nil
}
func (r *recordingBroker) ActionStatus(context.Context, string, string, string, state.WorkflowState) error {
	return nil
}
func (r *recordingBroker) ActionProgress(ctx context.Context, workflowID, stageID, actionName string, progress int, message string) error {
	r.progressCalls = append(r.progressCalls, struct {
		workflowID string
		stageID    string
		actionName string
		percent    int
		message    string
	}{workflowID, stageID, actionName, progress, message})
	return nil
}
func (r *recordingBroker) ActionRemoved(context.Context, string, string, string, string) error {
	return nil
}
func (r *recordingBroker) StageRemoved(context.Context, string, string, string) error { return nil }
func (r *recordingBroker) ExecutionSummary(ctx context.Context, workflowID string, report state.ExecutionReport) error {
	r.summaries = append(r.summaries, report)
	return nil
}

func (f *failingSummaryBroker) ExecutionSummary(ctx context.Context, workflowID string, report state.ExecutionReport) error {
	f.recordingBroker.ExecutionSummary(ctx, workflowID, report)
	return f.returnErr
}

func newTestRunner() *Runner {
	return New(local.Factory{}, registry.Default(), &recordingBroker{})
}

func mustRegisterAction(name string, run func(types.Context) error) {
	defaults.MustRegisterAction(name, "", run)
}

func stageStatus(result RunResult, id string) (StageStatus, bool) {
	for _, status := range result.Stages {
		if status.ID == id {
			return status, true
		}
	}
	return StageStatus{}, false
}

func actionStatus(result RunResult, stageID, name string) (ActionStatus, bool) {
	for _, status := range result.Actions {
		if status.StageID == stageID && status.Name == name {
			return status, true
		}
	}
	return ActionStatus{}, false
}

func TestRunnerExecutesActions(t *testing.T) {
	executed := make([]string, 0)

	stage := defaults.NewStage("stage-1", "Stage One", "basic stage")
	mustRegisterAction("a1", func(ctx types.Context) error {
		executed = append(executed, "a1")
		return nil
	})
	mustRegisterAction("a2", func(ctx types.Context) error {
		executed = append(executed, "a2")
		return nil
	})
	stage.AddActions(defaults.NewAction("a1"), defaults.NewAction("a2"))
	_ = stage.InitialStore().Put("seed", "value")

	workflow := defaults.NewWorkflow("wf-1", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})

	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got error %v", result.Error)
	}

	expected := []string{"a1", "a2"}
	if len(executed) != len(expected) {
		t.Fatalf("unexpected execution order: %v", executed)
	}
	for i, name := range expected {
		if executed[i] != name {
			t.Fatalf("unexpected order %v", executed)
		}
	}

	status, ok := stageStatus(result, "stage-1")
	if !ok || status.Status != StatusCompleted {
		t.Fatalf("expected stage completed, got %+v", status)
	}

	if act, ok := actionStatus(result, "stage-1", "a1"); !ok || act.Status != StatusCompleted {
		t.Fatalf("expected action a1 completed, got %+v", act)
	}
	if act, ok := actionStatus(result, "stage-1", "a2"); !ok || act.Status != StatusCompleted {
		t.Fatalf("expected action a2 completed, got %+v", act)
	}

	if value, err := store.Get[string](workflow.Store(), "seed"); err != nil || value != "value" {
		t.Fatalf("expected initial store key propagated, got (%v, %v)", value, err)
	}
}

func TestRunnerFailsOnUnregisteredAction(t *testing.T) {
	stage := defaults.NewStage("stage-unregistered", "Stage", "")
	stage.AddActions(stubAction{name: "missing", DescriptionText: ""})
	workflow := defaults.NewWorkflow("wf-unregistered", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})
	if result.Success || result.Error == nil {
		t.Fatalf("expected failure for unregistered action, got success=%t error=%v", result.Success, result.Error)
	}
	if !strings.Contains(result.Error.Error(), "missing") {
		t.Fatalf("expected error mentioning missing action, got %v", result.Error)
	}
}

func TestRunnerDynamicActions(t *testing.T) {
	executed := make([]string, 0)

	stage := defaults.NewStage("stage-1", "Stage One", "dynamic stage")
	mustRegisterAction("seed", func(ctx types.Context) error {
		executed = append(executed, "seed")
		defaults.MustRegisterAction("dynamic", "", func(ctx types.Context) error {
			executed = append(executed, "dynamic")
			return nil
		})
		ctx.Actions().Add(defaults.NewAction("dynamic"))
		return nil
	})
	mustRegisterAction("tail", func(ctx types.Context) error {
		executed = append(executed, "tail")
		return nil
	})
	stage.AddActions(defaults.NewAction("seed"), defaults.NewAction("tail"))

	workflow := defaults.NewWorkflow("wf-dyn-actions", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	expected := []string{"seed", "dynamic", "tail"}
	if len(executed) != len(expected) {
		t.Fatalf("unexpected execution count: %v", executed)
	}
	for i, name := range expected {
		if executed[i] != name {
			t.Fatalf("unexpected order %v", executed)
		}
	}

	if len(result.DynamicActions) != 1 || result.DynamicActions[0].Action.Name() != "dynamic" {
		t.Fatalf("expected dynamic action telemetry, got %+v", result.DynamicActions)
	}
}

func TestRunnerActionMiddleware(t *testing.T) {
	calls := make([]string, 0)

	stage := defaults.NewStage("stage-1", "Stage", "")
	stage.WithActionMiddleware(func(next types.ActionRunnerFunc) types.ActionRunnerFunc {
		return func(ctx types.Context, action types.Action, index int, isLast bool) error {
			calls = append(calls, "before:"+action.Name())
			err := next(ctx, action, index, isLast)
			calls = append(calls, "after:"+action.Name())
			return err
		}
	})
	mustRegisterAction("run", func(ctx types.Context) error {
		calls = append(calls, "execute")
		return nil
	})
	stage.AddActions(defaults.NewAction("run"))

	workflow := defaults.NewWorkflow("wf-action-mw", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})

	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	expected := []string{"before:run", "execute", "after:run"}
	if len(calls) != len(expected) {
		t.Fatalf("unexpected middleware calls %v", calls)
	}
	for i, entry := range expected {
		if calls[i] != entry {
			t.Fatalf("unexpected order %v", calls)
		}
	}
}

func TestRunnerDynamicStages(t *testing.T) {
	execution := make([]string, 0)

	dynamicStage := defaults.NewStage("stage-2", "Dynamic", "")
	mustRegisterAction("dyn-action", func(ctx types.Context) error {
		execution = append(execution, "stage-2:dyn-action")
		return nil
	})
	dynamicStage.AddActions(defaults.NewAction("dyn-action"))

	seedStage := defaults.NewStage("stage-1", "Seed", "")
	mustRegisterAction("spawn", func(ctx types.Context) error {
		execution = append(execution, "stage-1:spawn")
		ctx.Stages().Add(dynamicStage)
		return nil
	})
	seedStage.AddActions(defaults.NewAction("spawn"))

	workflow := defaults.NewWorkflow("wf-dyn-stage", "Workflow", "")
	workflow.AddStage(seedStage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	expected := []string{"stage-1:spawn", "stage-2:dyn-action"}
	if len(execution) != len(expected) {
		t.Fatalf("unexpected execution %v", execution)
	}
	for i, name := range expected {
		if execution[i] != name {
			t.Fatalf("unexpected order %v", execution)
		}
	}

	if len(result.DynamicStages) != 1 || result.DynamicStages[0].Stage.ID() != "stage-2" {
		t.Fatalf("expected dynamic stage telemetry, got %+v", result.DynamicStages)
	}
}

func TestRunnerDisablesAction(t *testing.T) {
	executed := make([]string, 0)

	stage := defaults.NewStage("stage-1", "Stage", "")
	mustRegisterAction("first", func(ctx types.Context) error {
		executed = append(executed, "first")
		ctx.Actions().Disable("second")
		return nil
	})
	mustRegisterAction("second", func(ctx types.Context) error {
		executed = append(executed, "second")
		return nil
	})
	stage.AddActions(defaults.NewAction("first"), defaults.NewAction("second"))

	workflow := defaults.NewWorkflow("wf-disable", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	if len(executed) != 1 || executed[0] != "first" {
		t.Fatalf("expected only first action to execute, got %v", executed)
	}

	if status, ok := actionStatus(result, "stage-1", "second"); !ok || status.Status != StatusSkipped {
		t.Fatalf("expected second action skipped, got %+v", status)
	}

	if !result.DisabledActions["second"] {
		t.Fatalf("expected disabled actions map to contain 'second'")
	}
}

func TestRunnerReportsDisabledActionToState(t *testing.T) {
	mgr := memory.New()
	defer mgr.Close()

	stage := defaults.NewStage("stage-1", "Stage", "")
	mustRegisterAction("first", func(ctx types.Context) error {
		ctx.Actions().Disable("second")
		return nil
	})
	mustRegisterAction("second", func(ctx types.Context) error {
		return nil
	})
	stage.AddActions(defaults.NewAction("first"), defaults.NewAction("second"))

	workflow := defaults.NewWorkflow("wf-disable-state", "Workflow", "")
	workflow.SetType("workflow-demo")
	workflow.SetPayload(map[string]interface{}{"foo": "bar"})
	workflow.AddStage(stage)

	br := broker.NewLocal(mgr)
	runner := New(local.Factory{}, registry.Default(), br)
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	status, err := mgr.GetStatus("wf-disable-state")
	if err != nil {
		t.Fatalf("state lookup failed: %v", err)
	}
	stageRec := status.Stages["stage-1"]
	if stageRec == nil {
		t.Fatalf("expected stage record")
	}
	if stageRec.Actions == nil {
		t.Fatalf("expected actions map populated")
	}
	actionRec, ok := stageRec.Actions["second"]
	if !ok {
		t.Fatalf("expected action 'second' recorded")
	}
	if actionRec.Status != state.WorkflowSkipped {
		t.Fatalf("expected action skipped in state, got %s", actionRec.Status)
	}
	if status.Definition.Type != "workflow-demo" {
		t.Fatalf("expected workflow type propagated, got %s", status.Definition.Type)
	}
	if status.Definition.Payload == nil || status.Definition.Payload["foo"] != "bar" {
		t.Fatalf("expected workflow payload propagated, got %v", status.Definition.Payload)
	}
}

func TestRunnerEmitsExecutionSummary(t *testing.T) {
	mgr := memory.New()
	defer mgr.Close()

	stage := defaults.NewStage("stage-summary", "Summary Stage", "captures execution details")
	stage.AddTags("core", "summary")
	mustRegisterAction("summary-action", func(ctx types.Context) error {
		if store := ctx.Store(); store != nil {
			_ = store.Put("result", "ok")
		}
		return nil
	})
	stage.AddActions(defaults.NewAction("summary-action"))

	workflow := defaults.NewWorkflow("wf-summary", "Workflow Summary", "workflow description")
	workflow.AddTags("primary")
	workflow.SetType("demo")
	workflow.SetPayload(map[string]interface{}{"run": 1})
	workflow.AddStage(stage)

	br := broker.NewLocal(mgr)
	runner := New(local.Factory{}, registry.Default(), br)
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	report, err := mgr.GetExecutionSummary(context.Background(), "wf-summary")
	if err != nil {
		t.Fatalf("expected execution summary, got error %v", err)
	}
	if report.WorkflowID != "wf-summary" {
		t.Fatalf("expected workflow id wf-summary, got %s", report.WorkflowID)
	}
	if report.WorkflowName != "Workflow Summary" || report.WorkflowType != "demo" {
		t.Fatalf("unexpected workflow metadata: %+v", report)
	}
	if len(report.WorkflowTags) != 1 || report.WorkflowTags[0] != "primary" {
		t.Fatalf("unexpected workflow tags: %+v", report.WorkflowTags)
	}
	if report.FinalStore == nil || report.FinalStore["result"] != "ok" {
		t.Fatalf("expected final store with result=ok, got %+v", report.FinalStore)
	}
	if len(report.Stages) != 1 {
		t.Fatalf("expected single stage summary, got %+v", report.Stages)
	}
	stageSummary := report.Stages[0]
	if stageSummary.Description != "captures execution details" {
		t.Fatalf("unexpected stage description: %+v", stageSummary)
	}
	if len(stageSummary.Tags) != 2 {
		t.Fatalf("expected stage tags propagated, got %+v", stageSummary.Tags)
	}
	if len(stageSummary.Actions) != 1 {
		t.Fatalf("expected one action summary, got %+v", stageSummary.Actions)
	}
	actionSummary := stageSummary.Actions[0]
	if actionSummary.Name != "summary-action" || actionSummary.Description != "" {
		t.Fatalf("unexpected action summary: %+v", actionSummary)
	}
}

func TestRunnerReportsActionProgress(t *testing.T) {
	mgr := memory.New()
	defer mgr.Close()

	stage := defaults.NewStage("stage-1", "Stage", "")
	mustRegisterAction("work", func(ctx types.Context) error {
		if err := ctx.Broker().Progress(10); err != nil {
			t.Fatalf("progress failed: %v", err)
		}
		if err := ctx.Broker().ProgressCause("half", 50); err != nil {
			t.Fatalf("progress cause failed: %v", err)
		}
		return nil
	})
	stage.AddActions(defaults.NewAction("work"))

	workflow := defaults.NewWorkflow("wf-progress", "Workflow", "")
	workflow.AddStage(stage)

	runner := New(local.Factory{}, registry.Default(), broker.NewLocal(mgr))
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	status, err := mgr.GetStatus("wf-progress")
	if err != nil {
		t.Fatalf("state lookup failed: %v", err)
	}
	stageRec := status.Stages["stage-1"]
	if stageRec == nil {
		t.Fatalf("expected stage record")
	}
	actionRec := stageRec.Actions["work"]
	if actionRec == nil {
		t.Fatalf("expected action record")
	}
	if actionRec.Progress != 50 {
		t.Fatalf("expected final progress 50, got %d", actionRec.Progress)
	}
	if actionRec.Message != "half" {
		t.Fatalf("expected progress message 'half', got %q", actionRec.Message)
	}
}

func TestRunnerReportsActionRemoval(t *testing.T) {
	mgr := memory.New()
	defer mgr.Close()

	stage := defaults.NewStage("stage-1", "Stage", "")
	mustRegisterAction("remove", func(ctx types.Context) error {
		if !ctx.Actions().Remove("target") {
			t.Fatalf("expected removal to succeed")
		}
		return nil
	})
	mustRegisterAction("target", func(ctx types.Context) error { return nil })
	stage.AddActions(defaults.NewAction("remove"), defaults.NewAction("target"))

	workflow := defaults.NewWorkflow("wf-remove-action", "Workflow", "")
	workflow.AddStage(stage)

	runner := New(local.Factory{}, registry.Default(), broker.NewLocal(mgr))
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}
	if result.RemovedActions == nil {
		t.Fatalf("expected removed actions map")
	}
	if got := result.RemovedActions["stage-1::target"]; got != "stage-1::remove" {
		t.Fatalf("expected removal createdBy stage-1::remove, got %q", got)
	}

	status, err := mgr.GetStatus("wf-remove-action")
	if err != nil {
		t.Fatalf("state lookup failed: %v", err)
	}
	actionRec := status.Stages["stage-1"].Actions["target"]
	if actionRec == nil {
		t.Fatalf("expected target action record")
	}
	if actionRec.Status != state.WorkflowRemoved {
		t.Fatalf("expected action removed state, got %s", actionRec.Status)
	}
}

func TestRunnerReportsStageRemoval(t *testing.T) {
	mgr := memory.New()
	defer mgr.Close()

	stage := defaults.NewStage("stage-1", "Stage", "")
	mustRegisterAction("spawn-remove", func(ctx types.Context) error {
		dyn := defaults.NewStage("dyn-stage", "Dynamic", "")
		defaults.MustRegisterAction("noop", "", func(types.Context) error { return nil })
		dyn.AddActions(defaults.NewAction("noop"))
		ctx.Stages().Add(dyn)
		ctx.Stages().Remove("dyn-stage")
		return nil
	})
	stage.AddActions(defaults.NewAction("spawn-remove"))

	workflow := defaults.NewWorkflow("wf-remove-stage", "Workflow", "")
	workflow.AddStage(stage)

	runner := New(local.Factory{}, registry.Default(), broker.NewLocal(mgr))
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}
	if result.RemovedStages == nil {
		t.Fatalf("expected removed stages map")
	}
	if got := result.RemovedStages["dyn-stage"]; got != "stage-1::spawn-remove" {
		t.Fatalf("expected removal createdBy stage-1::spawn-remove, got %q", got)
	}

	status, err := mgr.GetStatus("wf-remove-stage")
	if err != nil {
		t.Fatalf("state lookup failed: %v", err)
	}
	stageRec := status.Stages["dyn-stage"]
	if stageRec == nil {
		t.Fatalf("expected dynamic stage record")
	}
	if stageRec.Status != state.WorkflowRemoved {
		t.Fatalf("expected dynamic stage removed state, got %s", stageRec.Status)
	}
}

func TestWorkflowLoggerMiddleware(t *testing.T) {
	logger := &captureLogger{}
	stage := defaults.NewStage("stage-1", "Stage1", "")
	mustRegisterAction("run", func(types.Context) error { return nil })
	stage.AddActions(defaults.NewAction("run"))
	workflow := defaults.NewWorkflow("wf-logger", "Workflow", "")
	workflow.AddStage(stage)

	runner := New(local.Factory{}, registry.Default(), &recordingBroker{}, WithWorkflowMiddleware(middleware.WorkflowLogger()), WithDefaultLogger(logger))
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}
	foundStart, foundEnd := false, false
	for _, msg := range logger.infos {
		if strings.Contains(msg, "stage Stage1 starting") {
			foundStart = true
		}
		if strings.Contains(msg, "stage Stage1 completed") {
			foundEnd = true
		}
	}
	if !foundStart || !foundEnd {
		t.Fatalf("expected workflow logger messages, got %v", logger.infos)
	}
}

func TestStageTimerMiddleware(t *testing.T) {
	logger := &captureLogger{}
	stage := defaults.NewStage("stage-1", "Stage", "")
	stage.WithMiddleware(middleware.StageTimer())
	mustRegisterAction("stage-run", func(types.Context) error { return nil })
	stage.AddActions(defaults.NewAction("stage-run"))
	workflow := defaults.NewWorkflow("wf-stage-timer", "Workflow", "")
	workflow.AddStage(stage)

	runner := New(local.Factory{}, registry.Default(), &recordingBroker{}, WithDefaultLogger(logger))
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}
	enterLogged, finishedLogged := false, false
	for _, msg := range logger.infos {
		if strings.Contains(msg, "entering with actions") {
			enterLogged = true
		}
		if strings.Contains(msg, "finished") {
			finishedLogged = true
		}
	}
	if !enterLogged || !finishedLogged {
		t.Fatalf("expected stage timer logs, got %v", logger.infos)
	}
}

func TestActionProgressMiddleware(t *testing.T) {
	rec := &recordingBroker{}
	stage := defaults.NewStage("stage-1", "Stage", "")
	stage.WithActionMiddleware(middleware.ActionProgress())
	mustRegisterAction("a1", func(types.Context) error { return nil })
	mustRegisterAction("a2", func(types.Context) error { return nil })
	stage.AddActions(defaults.NewAction("a1"), defaults.NewAction("a2"))
	workflow := defaults.NewWorkflow("wf-action-progress", "Workflow", "")
	workflow.AddStage(stage)

	runner := New(local.Factory{}, registry.Default(), rec)
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}
	if len(rec.progressCalls) != 2 {
		t.Fatalf("expected 2 progress entries, got %v", rec.progressCalls)
	}
	if rec.progressCalls[0].percent != 50 || rec.progressCalls[0].message != "a1:running" {
		t.Fatalf("unexpected first progress entry: %+v", rec.progressCalls[0])
	}
	if rec.progressCalls[1].percent != 100 || rec.progressCalls[1].message != "a2:completed" {
		t.Fatalf("unexpected second progress entry: %+v", rec.progressCalls[1])
	}
}

func TestRunnerWarnsWhenSummaryPersistenceFails(t *testing.T) {
	fb := &failingSummaryBroker{returnErr: fmt.Errorf("boom")}
	logger := &captureLogger{}

	stage := defaults.NewStage("stage-fail-summary", "Stage", "")
	mustRegisterAction("summary-fail", func(types.Context) error { return nil })
	stage.AddActions(defaults.NewAction("summary-fail"))

	workflow := defaults.NewWorkflow("wf-fail-summary", "Workflow", "")
	workflow.AddStage(stage)

	runner := New(local.Factory{}, registry.Default(), fb, WithDefaultLogger(logger))
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}
	if len(fb.summaries) != 1 {
		t.Fatalf("expected summary attempt recorded, got %d", len(fb.summaries))
	}
	foundWarn := false
	for _, msg := range logger.warns {
		if strings.Contains(msg, "failed to persist execution summary") {
			foundWarn = true
			break
		}
	}
	if !foundWarn {
		t.Fatalf("expected warning for failed summary persistence, got %+v", logger.warns)
	}
}

func TestRunnerReportsRemovedAction(t *testing.T) {
	mgr := memory.New()
	defer mgr.Close()

	stage := defaults.NewStage("stage-remove", "Stage Remove", "")
	mustRegisterAction("remove-trigger", func(ctx types.Context) error {
		ctx.Actions().Remove("remove-target")
		return nil
	})
	mustRegisterAction("remove-target", func(types.Context) error { return nil })
	stage.AddActions(defaults.NewAction("remove-trigger"), defaults.NewAction("remove-target"))

	workflow := defaults.NewWorkflow("wf-remove", "Workflow Remove", "")
	workflow.AddStage(stage)

	br := broker.NewLocal(mgr)
	runner := New(local.Factory{}, registry.Default(), br)
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}

	report, err := mgr.GetExecutionSummary(context.Background(), "wf-remove")
	if err != nil {
		t.Fatalf("expected execution summary, got error %v", err)
	}
	if report.RemovedActions == nil || report.RemovedActions["stage-remove::remove-target"] == "" {
		t.Fatalf("expected removed action recorded, got %+v", report.RemovedActions)
	}
	if len(report.Stages) != 1 || len(report.Stages[0].Actions) != 2 {
		t.Fatalf("unexpected stage/action summaries: %+v", report.Stages)
	}
	var removedSummary state.ActionSummary
	for _, action := range report.Stages[0].Actions {
		if action.Name == "remove-target" {
			removedSummary = action
			break
		}
	}
	if removedSummary.Name == "" || removedSummary.Status != state.WorkflowRemoved {
		t.Fatalf("expected remove-target marked removed, got %+v", removedSummary)
	}

	status, err := mgr.GetStatus("wf-remove")
	if err != nil {
		t.Fatalf("expected workflow status, got %v", err)
	}
	stageRecord := status.Stages["stage-remove"]
	if stageRecord == nil {
		t.Fatalf("expected stage record")
	}
	actionRecord := stageRecord.Actions["remove-target"]
	if actionRecord == nil || actionRecord.Status != state.WorkflowRemoved {
		t.Fatalf("expected action removed in state manager, got %+v", actionRecord)
	}
}

func TestRunnerContinuesAfterDisabledStage(t *testing.T) {
	executed := make([]string, 0)

	stage1 := defaults.NewStage("stage-1", "Stage1", "")
	mustRegisterAction("s1", func(ctx types.Context) error {
		executed = append(executed, "stage-1:s1")
		ctx.Stages().Disable("stage-2")
		return nil
	})
	stage1.AddActions(defaults.NewAction("s1"))

	stage2 := defaults.NewStage("stage-2", "Stage2", "")
	mustRegisterAction("s2", func(ctx types.Context) error {
		executed = append(executed, "stage-2:s2")
		return nil
	})
	stage2.AddActions(defaults.NewAction("s2"))

	stage3 := defaults.NewStage("stage-3", "Stage3", "")
	mustRegisterAction("s3", func(ctx types.Context) error {
		executed = append(executed, "stage-3:s3")
		return nil
	})
	stage3.AddActions(defaults.NewAction("s3"))

	workflow := defaults.NewWorkflow("wf-skip-stage", "Workflow", "")
	workflow.AddStage(stage1, stage2, stage3)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{})
	if !result.Success || result.Error != nil {
		t.Fatalf("expected success, got %v", result.Error)
	}
	if len(executed) != 2 {
		t.Fatalf("expected 2 executed actions, got %v", executed)
	}
	if executed[0] != "stage-1:s1" || executed[1] != "stage-3:s3" {
		t.Fatalf("unexpected execution order %v", executed)
	}
	if status, ok := stageStatus(result, "stage-2"); !ok || status.Status != StatusSkipped {
		t.Fatalf("expected stage-2 skipped, got %+v", status)
	}
	if status, ok := stageStatus(result, "stage-3"); !ok || status.Status != StatusCompleted {
		t.Fatalf("expected stage-3 completed, got %+v", status)
	}
}

func TestRunnerIgnoreErrors(t *testing.T) {
	stage := defaults.NewStage("stage-1", "Stage", "")
	mustRegisterAction("fail", func(ctx types.Context) error {
		return exampleError("boom")
	})
	stage.AddActions(defaults.NewAction("fail"))

	workflow := defaults.NewWorkflow("wf-ignore", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	result := runner.Run(workflow, RunOptions{IgnoreErrors: true})

	if result.Error != nil {
		t.Fatalf("expected error to be suppressed when IgnoreErrors is set")
	}
	if !result.Success {
		t.Fatalf("expected success flag")
	}

	status, ok := stageStatus(result, "stage-1")
	if !ok || status.Status != StatusFailed {
		t.Fatalf("expected stage marked failed, got %+v", status)
	}
}

type exampleError string

func (e exampleError) Error() string { return string(e) }

func TestRunnerWithContextOption(t *testing.T) {
	stage := defaults.NewStage("stage-1", "Stage", "")
	mustRegisterAction("noop", func(types.Context) error { return nil })
	stage.AddActions(defaults.NewAction("noop"))

	workflow := defaults.NewWorkflow("wf-context", "Workflow", "")
	workflow.AddStage(stage)

	runner := newTestRunner()
	runCtx, cancel := context.WithCancel(context.Background())
	cancel()
	result := runner.Run(workflow, RunOptions{Context: runCtx})
	if !result.Success {
		t.Fatalf("expected success when context already canceled; got %v", result.Error)
	}
}
