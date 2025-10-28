package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v3"
	"github.com/davidroman0O/gostage/v3/diagnostics"
	"github.com/davidroman0O/gostage/v3/e2e/testkit"
	nodepkg "github.com/davidroman0O/gostage/v3/node"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/state"
	statetest "github.com/davidroman0O/gostage/v3/state/testkit"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

const (
	lifecycleSuccessWorkflowID = "workflow.lifecycle.success"
	lifecycleFailureWorkflowID = "workflow.lifecycle.failure"
	lifecycleCancelWorkflowID  = "workflow.lifecycle.cancel"
	lifecycleDynamicWorkflowID = "workflow.lifecycle.dynamic"

	stageSuccessID  = "stage-success"
	actionSuccessID = "action-success"

	stageFailureID  = "stage-failure"
	actionFailureID = "action-failure"

	stageCancelID  = "stage-cancel"
	actionCancelID = "action-cancel"

	stageControlID  = "stage-control"
	actionControlID = "action-control"
	actionRemoveID  = "action-remove"
	stageSkipID     = "stage-skip"
	actionSkipID    = "action-skip"
	stageRemovedID  = "stage-removed"
	actionRemovedID = "action-removed"

	lifecycleRemotePool  = "lifecycle-remote"
	lifecycleRemoteAlt   = "lifecycle-remote-alt"
	lifecycleSpawnerName = "lifecycle-spawner"
	lifecycleRemoteChild = "lifecycle-remote-child"
)

func TestLifecycleStatesLocal(t *testing.T) {
	registerLifecycleWorkflows(t)

	backends := testkit.NewMemoryBackends()
	opts := testkit.MemoryOptions(backends)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diagCh, err := gostage.Run(ctx, opts...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	diag := testkit.StartDiagnosticsCollector(t, diagCh)
	defer diag.Close()

	telemetryBuf := testkit.StartTelemetryBuffer(ctx, t, node, 512)
	defer telemetryBuf.Close()
	defer node.Close()

	scenarios := []lifecycleScenario{
		{
			name:     "success",
			workflow: lifecycleSuccessWorkflowID,
			submit:   []gostage.SubmitOption{gostage.WithTags("lifecycle")},
			exp: lifecycleExpectation{
				workflowState: state.WorkflowCompleted,
				reason:        state.TerminationReasonSuccess,
				stageStates: map[string]state.WorkflowState{
					stageSuccessID: state.WorkflowCompleted,
				},
				actionStates: map[actionKey]state.WorkflowState{
					{stageSuccessID, actionSuccessID}: state.WorkflowCompleted,
				},
				workflowEvents: []telemetry.EventKind{
					telemetry.EventWorkflowCompleted,
				},
			},
		},
		{
			name:     "failure",
			workflow: lifecycleFailureWorkflowID,
			submit:   []gostage.SubmitOption{gostage.WithTags("lifecycle")},
			exp: lifecycleExpectation{
				workflowState: state.WorkflowFailed,
				reason:        "",
				stageStates: map[string]state.WorkflowState{
					stageFailureID: state.WorkflowFailed,
				},
				actionStates: map[actionKey]state.WorkflowState{
					{stageFailureID, actionFailureID}: state.WorkflowFailed,
				},
				workflowEvents: []telemetry.EventKind{
					telemetry.EventWorkflowFailed,
				},
			},
		},
		{
			name:     "cancel",
			workflow: lifecycleCancelWorkflowID,
			cancel:   true,
			submit:   []gostage.SubmitOption{gostage.WithTags("lifecycle")},
			exp: lifecycleExpectation{
				workflowState: state.WorkflowCancelled,
				reason:        state.TerminationReasonUserCancel,
				stageStates: map[string]state.WorkflowState{
					stageCancelID: state.WorkflowCancelled,
				},
				actionStates: map[actionKey]state.WorkflowState{
					{stageCancelID, actionCancelID}: state.WorkflowCancelled,
				},
				workflowEvents: []telemetry.EventKind{
					telemetry.EventWorkflowCancelled,
				},
			},
		},
		{
			name:     "dynamic",
			workflow: lifecycleDynamicWorkflowID,
			submit:   []gostage.SubmitOption{gostage.WithTags("lifecycle")},
			exp: lifecycleExpectation{
				workflowState: state.WorkflowCompleted,
				reason:        state.TerminationReasonSuccess,
				stageStates: map[string]state.WorkflowState{
					stageControlID: state.WorkflowCompleted,
					stageSkipID:    state.WorkflowSkipped,
				},
				stageOptional: map[string]state.WorkflowState{
					stageRemovedID: state.WorkflowSkipped,
				},
				actionStates: map[actionKey]state.WorkflowState{
					{stageControlID, actionControlID}: state.WorkflowCompleted,
					{stageControlID, actionRemoveID}:  state.WorkflowCompleted,
					{stageSkipID, actionSkipID}:       state.WorkflowPending,
				},
				actionOptional: map[actionKey]state.WorkflowState{
					{stageRemovedID, actionRemovedID}: state.WorkflowPending,
				},
				workflowEvents: []telemetry.EventKind{
					telemetry.EventWorkflowCompleted,
				},
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			runID, summary, events := executeLifecycleScenario(t, ctx, node, telemetryBuf, backends.Observer, scenario)
			assertLifecycleExpectations(t, runID, summary, events, scenario.exp)
			ensureNoDiagnosticErrors(t, diag.Events())
		})
	}
}

func TestLifecycleStatesRemote(t *testing.T) {
	if testing.Short() {
		t.Skip("skip remote lifecycle test in short mode")
	}

	killOrphans(t)
	resetChildInfo()
	registerLifecycleWorkflows(t)

	gostage.HandleChildNamed(lifecycleRemoteChild, func(ctx context.Context, childNode gostage.ChildNode) error {
		registerLifecycleActions()
		defer childNode.Close()
		return childNode.Run(ctx)
	}, gostage.WithChildPool(gostage.PoolConfig{Name: lifecycleRemotePool, Slots: 1}))

	backends := testkit.NewMemoryBackends()
	opts := append(testkit.MemoryOptions(backends), remoteLifecycleOptions()...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, diagCh, err := gostage.Run(ctx, opts...)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	diag := testkit.StartDiagnosticsCollector(t, diagCh)
	defer diag.Close()

	telemetryBuf := testkit.StartTelemetryBuffer(ctx, t, node, 1024)
	defer telemetryBuf.Close()

	healthBuf := testkit.StartHealthBuffer(ctx, t, node, 16)
	defer healthBuf.Close()
	waitForHealthStatus(t, healthBuf, string(nodepkg.HealthHealthy))
	defer node.Close()

	scenarios := []lifecycleScenario{
		{
			name:     "success",
			workflow: lifecycleSuccessWorkflowID,
			submit:   []gostage.SubmitOption{gostage.WithTags("lifecycle")},
			exp: lifecycleExpectation{
				workflowState: state.WorkflowCompleted,
				reason:        state.TerminationReasonSuccess,
				stageStates: map[string]state.WorkflowState{
					stageSuccessID: state.WorkflowCompleted,
				},
				actionStates: map[actionKey]state.WorkflowState{
					{stageSuccessID, actionSuccessID}: state.WorkflowCompleted,
				},
				workflowEvents: []telemetry.EventKind{
					telemetry.EventWorkflowExecution,
					telemetry.EventWorkflowSummary,
				},
			},
		},
		{
			name:     "failure",
			workflow: lifecycleFailureWorkflowID,
			submit:   []gostage.SubmitOption{gostage.WithTags("lifecycle")},
			exp: lifecycleExpectation{
				workflowState: state.WorkflowFailed,
				reason:        state.TerminationReasonFailure,
				stageStates: map[string]state.WorkflowState{
					stageFailureID: state.WorkflowFailed,
				},
				actionStates: map[actionKey]state.WorkflowState{
					{stageFailureID, actionFailureID}: state.WorkflowFailed,
				},
				workflowEvents: []telemetry.EventKind{
					telemetry.EventWorkflowExecution,
					telemetry.EventWorkflowSummary,
				},
			},
		},
		{
			name:     "cancel",
			workflow: lifecycleCancelWorkflowID,
			cancel:   true,
			submit:   []gostage.SubmitOption{gostage.WithTags("lifecycle")},
			exp: lifecycleExpectation{
				workflowState: state.WorkflowCancelled,
				reason:        state.TerminationReasonUserCancel,
				stageStates: map[string]state.WorkflowState{
					stageCancelID: state.WorkflowCancelled,
				},
				actionStates: map[actionKey]state.WorkflowState{
					{stageCancelID, actionCancelID}: state.WorkflowCancelled,
				},
				workflowEvents: []telemetry.EventKind{
					telemetry.EventWorkflowExecution,
					telemetry.EventWorkflowSummary,
				},
			},
		},
		{
			name:     "dynamic",
			workflow: lifecycleDynamicWorkflowID,
			submit:   []gostage.SubmitOption{gostage.WithTags("lifecycle")},
			exp: lifecycleExpectation{
				workflowState: state.WorkflowCompleted,
				reason:        state.TerminationReasonSuccess,
				stageStates: map[string]state.WorkflowState{
					stageControlID: state.WorkflowCompleted,
					stageSkipID:    state.WorkflowSkipped,
				},
				stageOptional: map[string]state.WorkflowState{
					stageRemovedID: state.WorkflowSkipped,
				},
				actionStates: map[actionKey]state.WorkflowState{
					{stageControlID, actionControlID}: state.WorkflowCompleted,
					{stageControlID, actionRemoveID}:  state.WorkflowCompleted,
					{stageSkipID, actionSkipID}:       state.WorkflowSkipped,
				},
				actionOptional: map[actionKey]state.WorkflowState{
					{stageRemovedID, actionRemovedID}: state.WorkflowPending,
				},
				workflowEvents: []telemetry.EventKind{
					telemetry.EventWorkflowExecution,
					telemetry.EventWorkflowSummary,
				},
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			runID, summary, events := executeLifecycleScenario(t, ctx, node, telemetryBuf, backends.Observer, scenario)
			assertLifecycleExpectations(t, runID, summary, events, scenario.exp)
			ensureNoDiagnosticErrors(t, diag.Events())
		})
	}

	killOrphans(t)
}

// --- helpers ---

type actionKey struct {
	stage  string
	action string
}

type lifecycleExpectation struct {
	workflowState  state.WorkflowState
	reason         state.TerminationReason
	stageStates    map[string]state.WorkflowState
	stageOptional  map[string]state.WorkflowState
	actionStates   map[actionKey]state.WorkflowState
	actionOptional map[actionKey]state.WorkflowState
	workflowEvents []telemetry.EventKind
}

type lifecycleScenario struct {
	name     string
	workflow string
	cancel   bool
	submit   []gostage.SubmitOption
	exp      lifecycleExpectation
}

func executeLifecycleScenario(t *testing.T, ctx context.Context, node *gostage.Node, buf *testkit.TelemetryBuffer, observer *statetest.CaptureObserver, scenario lifecycleScenario) (gostage.WorkflowID, state.WorkflowSummary, []telemetry.Event) {
	t.Helper()

	submitOpts := append([]gostage.SubmitOption{}, scenario.submit...)
	runID, err := node.Submit(ctx, gostage.WorkflowRef(scenario.workflow), submitOpts...)
	if err != nil {
		t.Fatalf("submit %s: %v", scenario.name, err)
	}

	if scenario.cancel {
		testkit.WaitForWorkflowInState(t, node.State, state.WorkflowID(runID), state.WorkflowRunning)
		cancelCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := node.Cancel(cancelCtx, runID); err != nil {
			t.Fatalf("cancel %s: %v", scenario.name, err)
		}
	}

	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	result, err := node.Wait(waitCtx, runID)
	if scenario.cancel {
		if err != nil {
			t.Fatalf("wait cancel %s: %v", scenario.name, err)
		}
		if result.Success {
			t.Fatalf("expected cancellation failure: %+v", result)
		}
	} else if err != nil {
		t.Fatalf("wait %s: %v", scenario.name, err)
	}

	summary := testkit.AwaitWorkflowSummary(t, node.State, observer, ctx, state.WorkflowID(runID))
	if testing.Verbose() {
		t.Logf("workflow summary: %+v", summary)
		for id, stage := range summary.Stages {
			actionStatuses := make(map[string]state.WorkflowState, len(stage.Actions))
			for actionID, action := range stage.Actions {
				actionStatuses[actionID] = action.Status
			}
			t.Logf("stage %s status=%s actions=%v", id, stage.Status, actionStatuses)
		}
	}
	needed := make(map[telemetry.EventKind]struct{}, len(scenario.exp.workflowEvents))
	for _, kind := range scenario.exp.workflowEvents {
		needed[kind] = struct{}{}
	}
	events := make([]telemetry.Event, 0, 32)
	deadline := time.Now().Add(3 * time.Second)
	for len(needed) > 0 && time.Now().Before(deadline) {
		batch := buf.Collect(t, 200*time.Millisecond)
		if len(batch) == 0 {
			continue
		}
		events = append(events, batch...)
		for _, evt := range batch {
			delete(needed, evt.Kind)
		}
	}
	if len(needed) > 0 {
		missing := make([]telemetry.EventKind, 0, len(needed))
		for kind := range needed {
			missing = append(missing, kind)
		}
		t.Fatalf("missing workflow telemetry %v", missing)
	}
	events = append(events, buf.Collect(t, 200*time.Millisecond)...)
	return runID, summary, events
}

func assertLifecycleExpectations(t *testing.T, runID gostage.WorkflowID, summary state.WorkflowSummary, events []telemetry.Event, exp lifecycleExpectation) {
	t.Helper()
	if summary.State != exp.workflowState {
		t.Logf("workflow summary: %+v", summary)
		t.Logf("workflow events: %+v", events)
		t.Fatalf("workflow state mismatch: got %s want %s", summary.State, exp.workflowState)
	}
	if exp.reason != "" && summary.TerminationReason != exp.reason {
		t.Logf("workflow summary: %+v", summary)
		t.Logf("workflow events: %+v", events)
		t.Fatalf("workflow reason mismatch: got %s want %s", summary.TerminationReason, exp.reason)
	}
	if testing.Verbose() {
		kinds := make([]telemetry.EventKind, 0, len(events))
		for _, evt := range events {
			kinds = append(kinds, evt.Kind)
		}
		t.Logf("telemetry kinds: %v", kinds)
	}

	ensureEventKinds(t, events, exp.workflowEvents, func(evt telemetry.Event) bool {
		return evt.WorkflowID == string(runID)
	})

	verifyStages(t, summary, events, exp.stageStates, false)
	verifyStages(t, summary, events, exp.stageOptional, true)
	verifyActions(t, summary, events, exp.actionStates, false)
	verifyActions(t, summary, events, exp.actionOptional, true)
}

func verifyStages(t *testing.T, summary state.WorkflowSummary, events []telemetry.Event, expected map[string]state.WorkflowState, optional bool) {
	t.Helper()
	if len(expected) == 0 {
		return
	}
	for stageID, stateExpected := range expected {
		rec, ok := summary.Stages[stageID]
		if !ok {
			if optional {
				continue
			}
			t.Fatalf("missing stage %s", stageID)
		}
		if rec.Status != stateExpected {
			t.Fatalf("stage %s state mismatch: got %s want %s", stageID, rec.Status, stateExpected)
		}
		if kind, ok := stageEventForState(stateExpected); ok {
			if !containsEvent(events, kind, func(evt telemetry.Event) bool {
				return evt.StageID == stageID
			}) {
				t.Fatalf("missing telemetry %s for stage %s", kind, stageID)
			}
		}
	}
}

func verifyActions(t *testing.T, summary state.WorkflowSummary, events []telemetry.Event, expected map[actionKey]state.WorkflowState, optional bool) {
	t.Helper()
	if len(expected) == 0 {
		return
	}
	for key, stateExpected := range expected {
		stage, ok := summary.Stages[key.stage]
		if !ok {
			if optional {
				continue
			}
			t.Fatalf("missing stage %s for action %s", key.stage, key.action)
		}
		if stage.Actions == nil {
			if optional {
				continue
			}
			t.Fatalf("missing action map for stage %s", key.stage)
		}
		rec, ok := stage.Actions[key.action]
		if !ok {
			if optional {
				continue
			}
			t.Fatalf("missing action %s::%s", key.stage, key.action)
		}
		if rec.Status != stateExpected {
			t.Fatalf("action %s::%s mismatch: got %s want %s", key.stage, key.action, rec.Status, stateExpected)
		}
		if kind, ok := actionEventForState(stateExpected); ok {
			if !containsEvent(events, kind, func(evt telemetry.Event) bool {
				return evt.StageID == key.stage && evt.ActionID == key.action
			}) {
				t.Fatalf("missing telemetry %s for action %s::%s", kind, key.stage, key.action)
			}
		}
	}
}

func ensureEventKinds(t *testing.T, events []telemetry.Event, kinds []telemetry.EventKind, predicate func(telemetry.Event) bool) {
	t.Helper()
	for _, kind := range kinds {
		if !containsEvent(events, kind, predicate) {
			t.Fatalf("missing workflow telemetry %s", kind)
		}
	}
}

func containsEvent(events []telemetry.Event, kind telemetry.EventKind, predicate func(telemetry.Event) bool) bool {
	for _, evt := range events {
		if evt.Kind == kind && (predicate == nil || predicate(evt)) {
			return true
		}
	}
	return false
}

func stageEventForState(st state.WorkflowState) (telemetry.EventKind, bool) {
	switch st {
	case state.WorkflowCompleted:
		return telemetry.EventStageCompleted, true
	case state.WorkflowFailed:
		return telemetry.EventStageFailed, true
	case state.WorkflowCancelled:
		return telemetry.EventStageCancelled, true
	case state.WorkflowSkipped:
		return telemetry.EventStageSkipped, true
	case state.WorkflowRemoved:
		return telemetry.EventStageRemoved, true
	default:
		return "", false
	}
}

func actionEventForState(st state.WorkflowState) (telemetry.EventKind, bool) {
	switch st {
	case state.WorkflowCompleted:
		return telemetry.EventActionCompleted, true
	case state.WorkflowFailed:
		return telemetry.EventActionFailed, true
	case state.WorkflowCancelled:
		return telemetry.EventActionCancelled, true
	case state.WorkflowSkipped:
		return telemetry.EventActionSkipped, true
	case state.WorkflowRemoved:
		return telemetry.EventActionRemoved, true
	default:
		return "", false
	}
}

func ensureNoDiagnosticErrors(t *testing.T, events []gostage.DiagnosticEvent) {
	t.Helper()
	for _, evt := range events {
		if evt.Severity == diagnostics.SeverityError || evt.Severity == diagnostics.SeverityCritical {
			t.Fatalf("diagnostic error: %+v", evt)
		}
	}
}

func waitForHealthStatus(t *testing.T, buf *testkit.HealthBuffer, status string) {
	t.Helper()
	buf.Next(t, status, 30*time.Second)
}

// registration helpers

func registerLifecycleWorkflows(t *testing.T) {
	testkit.ResetRegistry(t)

	registerLifecycleActions()

	if _, _, err := gostage.RegisterWorkflow(successDefinition()); err != nil {
		t.Fatalf("register success workflow: %v", err)
	}
	if _, _, err := gostage.RegisterWorkflow(failureDefinition()); err != nil {
		t.Fatalf("register failure workflow: %v", err)
	}
	if _, _, err := gostage.RegisterWorkflow(cancelDefinition()); err != nil {
		t.Fatalf("register cancel workflow: %v", err)
	}
	if _, _, err := gostage.RegisterWorkflow(dynamicDefinition()); err != nil {
		t.Fatalf("register dynamic workflow: %v", err)
	}
}

func registerLifecycleActions() {
	gostage.MustRegisterAction("lifecycle.success", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			return nil
		}
	})

	gostage.MustRegisterAction("lifecycle.fail", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			return fmt.Errorf("intentional failure")
		}
	})

	gostage.MustRegisterAction("lifecycle.wait", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
			}
		}
	})

	gostage.MustRegisterAction("lifecycle.control", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			ctx.Stages().Disable(stageSkipID)
			ctx.Actions().Disable(actionSkipID)
			return nil
		}
	})

	gostage.MustRegisterAction("lifecycle.remove", func() gostage.ActionFunc {
		return func(ctx rt.Context) error {
			ctx.Actions().Remove(actionRemovedID)
			ctx.Stages().Remove(stageRemovedID)
			return nil
		}
	})
}

func successDefinition() workflow.Definition {
	return workflow.Definition{
		ID:   lifecycleSuccessWorkflowID,
		Name: "LifecycleSuccess",
		Stages: []workflow.Stage{{
			ID:   stageSuccessID,
			Name: "Success",
			Actions: []workflow.Action{{
				ID:  actionSuccessID,
				Ref: "lifecycle.success",
			}},
		}},
	}
}

func failureDefinition() workflow.Definition {
	return workflow.Definition{
		ID:   lifecycleFailureWorkflowID,
		Name: "LifecycleFailure",
		Stages: []workflow.Stage{{
			ID:   stageFailureID,
			Name: "Failure",
			Actions: []workflow.Action{{
				ID:  actionFailureID,
				Ref: "lifecycle.fail",
			}},
		}},
	}
}

func cancelDefinition() workflow.Definition {
	return workflow.Definition{
		ID:   lifecycleCancelWorkflowID,
		Name: "LifecycleCancel",
		Stages: []workflow.Stage{{
			ID:   stageCancelID,
			Name: "Cancelable",
			Actions: []workflow.Action{{
				ID:  actionCancelID,
				Ref: "lifecycle.wait",
			}},
		}},
	}
}

func dynamicDefinition() workflow.Definition {
	return workflow.Definition{
		ID:   lifecycleDynamicWorkflowID,
		Name: "LifecycleDynamic",
		Stages: []workflow.Stage{
			{
				ID:   stageControlID,
				Name: "Control",
				Actions: []workflow.Action{
					{ID: actionControlID, Ref: "lifecycle.control"},
					{ID: actionRemoveID, Ref: "lifecycle.remove"},
				},
			},
			{
				ID:   stageSkipID,
				Name: "Skip",
				Actions: []workflow.Action{
					{ID: actionSkipID, Ref: "lifecycle.success"},
				},
			},
			{
				ID:   stageRemovedID,
				Name: "Removed",
				Actions: []workflow.Action{
					{ID: actionRemovedID, Ref: "lifecycle.success"},
				},
			},
		},
	}
}

func remoteLifecycleOptions() []gostage.Option {
	spawnerCfg := gostage.SpawnerConfig{
		Name:        lifecycleSpawnerName,
		BinaryPath:  gostage.CurrentBinary(),
		ChildType:   lifecycleRemoteChild,
		MaxRestarts: 0,
	}

	return []gostage.Option{
		gostage.WithSpawner(spawnerCfg),
		gostage.WithPool(gostage.PoolConfig{
			Name:    lifecycleRemotePool,
			Slots:   1,
			Spawner: spawnerCfg.Name,
		}),
	}
}
