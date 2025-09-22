package runner

import (
	"context"
	"time"

	"github.com/davidroman0O/gostage/v2/runtime/core"
	"github.com/davidroman0O/gostage/v2/types"
)

// ExecutionStatus represents the lifecycle state of a stage or action.
type ExecutionStatus string

const (
	StatusPending   ExecutionStatus = "pending"
	StatusRunning   ExecutionStatus = "running"
	StatusCompleted ExecutionStatus = "completed"
	StatusFailed    ExecutionStatus = "failed"
	StatusSkipped   ExecutionStatus = "skipped"
)

// StageStatus captures lifecycle details for a stage.
type StageStatus struct {
	ID        string
	Name      string
	Status    ExecutionStatus
	Dynamic   bool
	CreatedBy string
}

// ActionStatus captures lifecycle details for an action.
type ActionStatus struct {
	StageID   string
	Name      string
	Status    ExecutionStatus
	Dynamic   bool
	CreatedBy string
}

// DynamicStage describes a stage generated during execution.
type DynamicStage struct {
	Stage     types.Stage
	CreatedBy string
}

// DynamicAction describes an action generated during execution.
type DynamicAction struct {
	StageID   string
	Action    types.Action
	CreatedBy string
}

// RunResult provides execution outcome details.
type RunResult struct {
	WorkflowID string
	Success    bool
	Error      error
	Duration   time.Duration
	FinalStore map[string]interface{}

	Stages  []StageStatus
	Actions []ActionStatus

	DynamicStages  []DynamicStage
	DynamicActions []DynamicAction

	DisabledStages  map[string]bool
	DisabledActions map[string]bool
}

// RunOptions configures Run behaviour.
type RunOptions struct {
	Context      context.Context
	Logger       types.Logger
	IgnoreErrors bool
	InitialStore map[string]interface{}
}

// Runner executes workflows using a supplied execution context factory.
type Runner struct {
	factory       core.Factory
	workflowMW    []types.WorkflowMiddleware
	defaultLogger types.Logger
}

// Option configures a runner.
type Option func(*Runner)

// WithWorkflowMiddleware appends workflow-level middleware to the runner.
func WithWorkflowMiddleware(mw ...types.WorkflowMiddleware) Option {
	return func(r *Runner) {
		r.workflowMW = append(r.workflowMW, mw...)
	}
}

// WithContextFactory swaps the execution context factory (e.g. remote backends later).
func WithContextFactory(factory core.Factory) Option {
	return func(r *Runner) {
		if factory != nil {
			r.factory = factory
		}
	}
}

// WithDefaultLogger overrides the default logger used when none is provided.
func WithDefaultLogger(logger types.Logger) Option {
	return func(r *Runner) {
		if logger != nil {
			r.defaultLogger = logger
		}
	}
}

// New creates a local runner ready for execution.
func New(factory core.Factory, options ...Option) *Runner {
	if factory == nil {
		panic("runner: execution context factory required")
	}
	r := &Runner{
		factory:       factory,
		workflowMW:    make([]types.WorkflowMiddleware, 0),
		defaultLogger: noopLogger{},
	}
	for _, opt := range options {
		opt(r)
	}
	return r
}

type noopLogger struct{}

func (noopLogger) Debug(string, ...interface{}) {}
func (noopLogger) Info(string, ...interface{})  {}
func (noopLogger) Warn(string, ...interface{})  {}
func (noopLogger) Error(string, ...interface{}) {}

// Run executes the workflow using the configured factory and returns execution telemetry.
func (r *Runner) Run(workflow types.Workflow, options RunOptions) RunResult {
	logger := options.Logger
	if logger == nil {
		logger = r.defaultLogger
	}

	execContext := r.factory.New(workflow, nil)
	execContext.SetLogger(logger)
	execContext.SetDisabledMaps(make(map[string]bool), make(map[string]bool))

	if store := workflow.Store(); store != nil && options.InitialStore != nil {
		for key, value := range options.InitialStore {
			_ = store.Put(key, value)
		}
	}

	runCtx := options.Context
	if runCtx == nil {
		runCtx = context.Background()
	}

	tele := newTelemetry(workflow)
	start := time.Now()
	err := r.executeWorkflow(runCtx, workflow, execContext, logger, tele)
	duration := time.Since(start)

	result := RunResult{
		WorkflowID:     workflow.ID(),
		Success:        err == nil,
		Error:          err,
		Duration:       duration,
		FinalStore:     map[string]interface{}{},
		Stages:         tele.stageStatuses(),
		Actions:        tele.actionStatuses(),
		DynamicStages:  tele.dynamicStages,
		DynamicActions: tele.dynamicActions,
	}

	if store := workflow.Store(); store != nil {
		result.FinalStore = store.ExportAll()
	}

	if actions, stages := execContext.DisabledMaps(); actions != nil || stages != nil {
		if actions != nil {
			result.DisabledActions = copyBoolMap(actions)
		}
		if stages != nil {
			result.DisabledStages = copyBoolMap(stages)
		}
	}

	if err != nil && options.IgnoreErrors {
		result.Success = true
		result.Error = nil
	}

	return result
}

func (r *Runner) executeWorkflow(ctx context.Context, workflow types.Workflow, execCtx core.ExecutionContext, logger types.Logger, tele *telemetry) error {
	stages := append([]types.Stage(nil), workflow.Stages()...)
	combinedMW := append([]types.WorkflowMiddleware{}, r.workflowMW...)
	if mw := workflow.Middlewares(); len(mw) > 0 {
		combinedMW = append(combinedMW, mw...)
	}

	stageRunner := func(runCtx context.Context, stage types.Stage, wf types.Workflow, log types.Logger) error {
		return r.executeStage(runCtx, wf, stage, execCtx, log, tele)
	}

	for i := len(combinedMW) - 1; i >= 0; i-- {
		stageRunner = combinedMW[i](stageRunner)
	}

	disabledStages := tele.disabledStages(execCtx)

	for i := 0; i < len(stages); i++ {
		stage := stages[i]
		stageStatus := tele.stage(stage.ID(), stage.Name(), false, "")

		if disabledStages[stage.ID()] {
			stageStatus.Status = StatusSkipped
			continue
		}

		stageStatus.Status = StatusRunning
		if err := stageRunner(ctx, stage, workflow, logger); err != nil {
			stageStatus.Status = StatusFailed
			return err
		}
		stageStatus.Status = StatusCompleted

		if len(tele.pendingStageInsertions) > 0 {
			stages = tele.insertPendingStages(stages, i+1)
		}
	}

	return nil
}

func (r *Runner) executeStage(ctx context.Context, workflow types.Workflow, stage types.Stage, execCtx core.ExecutionContext, logger types.Logger, tele *telemetry) error {
	if err := mergeStageInitialStore(workflow, stage); err != nil {
		return err
	}

	stageActions := append([]types.Action(nil), stage.ActionList()...)
	execCtx.SetStage(stage)
	execCtx.SetActionList(stageActions)
	defer execCtx.ClearStage()

	stageRunner := func(runCtx context.Context, st types.Stage, wf types.Workflow, log types.Logger) error {
		return r.runActions(runCtx, wf, st, execCtx, logger, tele)
	}

	if mw := stage.Middlewares(); len(mw) > 0 {
		for i := len(mw) - 1; i >= 0; i-- {
			stageRunner = mw[i](stageRunner)
		}
	}

	return stageRunner(ctx, stage, workflow, logger)
}

func (r *Runner) runActions(ctx context.Context, workflow types.Workflow, stage types.Stage, execCtx core.ExecutionContext, logger types.Logger, tele *telemetry) error {
	actionList := append([]types.Action(nil), stage.ActionList()...)
	execCtx.SetActionList(actionList)
	disabledActions, _ := execCtx.DisabledMaps()
	actionMW := collectActionMiddleware(stage)

	for i := 0; i < len(actionList); i++ {
		action := actionList[i]
		actionKey := actionKey(stage.ID(), action.Name())
		actionStatus := tele.action(stage.ID(), action.Name(), false, "")

		if disabledActions != nil && disabledActions[action.Name()] {
			actionStatus.Status = StatusSkipped
			continue
		}

		actionStatus.Status = StatusRunning
		execCtx.SetAction(action, i, i == len(actionList)-1)

		runnerFn := func(ctx types.Context, act types.Action, index int, isLast bool) error {
			return act.Execute(ctx)
		}
		if len(actionMW) > 0 {
			for idx := len(actionMW) - 1; idx >= 0; idx-- {
				runnerFn = actionMW[idx](runnerFn)
			}
		}

		if err := runnerFn(execCtx, action, i, i == len(actionList)-1); err != nil {
			actionStatus.Status = StatusFailed
			return err
		}

		actionStatus.Status = StatusCompleted

		if added := execCtx.ConsumeDynamicActions(); len(added) > 0 {
			actionList = insertActions(actionList, added, i+1)
			execCtx.SetActionList(actionList)
			for _, dyn := range added {
				dynStatus := tele.action(stage.ID(), dyn.Name(), true, actionKey)
				dynStatus.Status = StatusPending
				tele.dynamicActions = append(tele.dynamicActions, DynamicAction{
					StageID:   stage.ID(),
					Action:    dyn,
					CreatedBy: actionKey,
				})
			}
		}

		if dynStages := execCtx.ConsumeDynamicStages(); len(dynStages) > 0 {
			for _, dynStage := range dynStages {
				t := tele.stage(dynStage.ID(), dynStage.Name(), true, actionKey)
				t.Status = StatusPending
				tele.pendingStageInsertions = append(tele.pendingStageInsertions, pendingStage{
					stage:     dynStage,
					createdBy: actionKey,
				})
				tele.dynamicStages = append(tele.dynamicStages, DynamicStage{
					Stage:     dynStage,
					CreatedBy: actionKey,
				})
			}
		}
	}

	return nil
}

func insertActions(actions []types.Action, additions []types.Action, index int) []types.Action {
	if index >= len(actions) {
		return append(actions, additions...)
	}
	result := make([]types.Action, 0, len(actions)+len(additions))
	result = append(result, actions[:index]...)
	result = append(result, additions...)
	result = append(result, actions[index:]...)
	return result
}

func actionKey(stageID, actionName string) string { return stageID + "::" + actionName }

func mergeStageInitialStore(workflow types.Workflow, stage types.Stage) error {
	wfStore := workflow.Store()
	if wfStore == nil {
		return nil
	}
	initial := stage.InitialStore()
	if initial == nil {
		return nil
	}
	_, _, err := wfStore.CopyFromWithOverwrite(initial)
	return err
}

type actionMiddlewareProvider interface {
	ActionMiddlewares() []types.ActionMiddleware
}

func collectActionMiddleware(stage types.Stage) []types.ActionMiddleware {
	if provider, ok := stage.(actionMiddlewareProvider); ok {
		return provider.ActionMiddlewares()
	}
	return nil
}

func copyBoolMap(src map[string]bool) map[string]bool {
	if src == nil {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// telemetry tracks execution state while running.
type telemetry struct {
	stages                 map[string]*StageStatus
	actions                map[string]*ActionStatus
	dynamicStages          []DynamicStage
	dynamicActions         []DynamicAction
	pendingStageInsertions []pendingStage
}

type pendingStage struct {
	stage     types.Stage
	createdBy string
}

func newTelemetry(workflow types.Workflow) *telemetry {
	t := &telemetry{
		stages:                 make(map[string]*StageStatus),
		actions:                make(map[string]*ActionStatus),
		dynamicStages:          make([]DynamicStage, 0),
		dynamicActions:         make([]DynamicAction, 0),
		pendingStageInsertions: make([]pendingStage, 0),
	}
	for _, stage := range workflow.Stages() {
		status := &StageStatus{ID: stage.ID(), Name: stage.Name(), Status: StatusPending}
		t.stages[stage.ID()] = status
	}
	return t
}

func (t *telemetry) disabledStages(execCtx core.ExecutionContext) map[string]bool {
	if _, stageMap := execCtx.DisabledMaps(); stageMap != nil {
		return stageMap
	}
	return map[string]bool{}
}

func (t *telemetry) stage(id, name string, dynamic bool, createdBy string) *StageStatus {
	status, ok := t.stages[id]
	if !ok {
		status = &StageStatus{ID: id, Name: name, Status: StatusPending}
		t.stages[id] = status
	}
	if dynamic {
		status.Dynamic = true
	}
	if createdBy != "" {
		status.CreatedBy = createdBy
	}
	return status
}

func (t *telemetry) action(stageID, name string, dynamic bool, createdBy string) *ActionStatus {
	key := actionKey(stageID, name)
	status, ok := t.actions[key]
	if !ok {
		status = &ActionStatus{StageID: stageID, Name: name, Status: StatusPending}
		t.actions[key] = status
	}
	if dynamic {
		status.Dynamic = true
	}
	if createdBy != "" {
		status.CreatedBy = createdBy
	}
	return status
}

func (t *telemetry) stageStatuses() []StageStatus {
	result := make([]StageStatus, 0, len(t.stages))
	for _, status := range t.stages {
		result = append(result, *status)
	}
	return result
}

func (t *telemetry) actionStatuses() []ActionStatus {
	result := make([]ActionStatus, 0, len(t.actions))
	for _, status := range t.actions {
		result = append(result, *status)
	}
	return result
}

func (t *telemetry) insertPendingStages(existing []types.Stage, index int) []types.Stage {
	if len(t.pendingStageInsertions) == 0 {
		return existing
	}

	stages := make([]types.Stage, 0, len(existing)+len(t.pendingStageInsertions))
	stages = append(stages, existing[:index]...)
	for _, pending := range t.pendingStageInsertions {
		stages = append(stages, pending.stage)
	}
	stages = append(stages, existing[index:]...)

	for _, pending := range t.pendingStageInsertions {
		status := t.stage(pending.stage.ID(), pending.stage.Name(), true, pending.createdBy)
		if status.Status == StatusPending {
			status.Status = StatusPending
		}
	}
	// reset buffer
	t.pendingStageInsertions = t.pendingStageInsertions[:0]
	return stages
}
