package runner

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/davidroman0O/gostage/v3/broker"
	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
	"github.com/davidroman0O/gostage/v3/runtime/core"
	runtimeengine "github.com/davidroman0O/gostage/v3/runtime/engine"
	"github.com/davidroman0O/gostage/v3/state"
	storepkg "github.com/davidroman0O/gostage/v3/store"
)

type ExecutionStatus = runtimeengine.ExecutionStatus

const (
	StatusPending   = runtimeengine.StatusPending
	StatusRunning   = runtimeengine.StatusRunning
	StatusCompleted = runtimeengine.StatusCompleted
	StatusFailed    = runtimeengine.StatusFailed
	StatusSkipped   = runtimeengine.StatusSkipped
	StatusRemoved   = runtimeengine.StatusRemoved
	StatusCancelled = runtimeengine.StatusCancelled
)

type StageStatus = runtimeengine.StageStatus
type ActionStatus = runtimeengine.ActionStatus
type DynamicStage = runtimeengine.DynamicStage
type DynamicAction = runtimeengine.DynamicAction

// RunResult provides execution outcome details.
type RunResult struct {
	WorkflowID string
	Success    bool
	Error      error
	Duration   time.Duration
	FinalStore map[string]interface{}
	Attempt    int

	Stages  []StageStatus
	Actions []ActionStatus

	DynamicStages  []DynamicStage
	DynamicActions []DynamicAction

	DisabledStages  map[string]bool
	DisabledActions map[string]bool
	RemovedStages   map[string]string
	RemovedActions  map[string]string
}

// RunOptions configures Run behaviour.
type RunOptions struct {
	Context      context.Context
	Logger       rt.Logger
	IgnoreErrors bool
	InitialStore map[string]interface{}
	Attempt      int
}

// Runner executes workflows using a supplied execution context factory.
type Runner struct {
	factory       core.Factory
	workflowMW    []rt.WorkflowMiddleware
	defaultLogger rt.Logger
	broker        broker.Broker
	registry      registry.Registry
}

// Option configures a runner.
type Option func(*Runner)

// WithWorkflowMiddleware appends workflow-level middleware to the runner.
func WithWorkflowMiddleware(mw ...rt.WorkflowMiddleware) Option {
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
func WithDefaultLogger(logger rt.Logger) Option {
	return func(r *Runner) {
		if logger != nil {
			r.defaultLogger = logger
		}
	}
}

// New creates a runner wired to the provided factory, registry, and broker.
func New(factory core.Factory, reg registry.Registry, br broker.Broker, options ...Option) *Runner {
	if factory == nil {
		panic("runner: execution context factory required")
	}
	if reg == nil {
		panic("runner: registry required")
	}
	if br == nil {
		panic("runner: broker required")
	}
	r := &Runner{
		factory:       factory,
		workflowMW:    make([]rt.WorkflowMiddleware, 0),
		defaultLogger: noopLogger{},
		registry:      reg,
		broker:        br,
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
func (r *Runner) Run(workflow rt.Workflow, options RunOptions) RunResult {
	logger := options.Logger
	if logger == nil {
		logger = r.defaultLogger
	}

	if err := r.validateWorkflowRegistration(workflow); err != nil {
		return RunResult{
			WorkflowID: workflow.ID(),
			Success:    false,
			Error:      err,
		}
	}

	runCtx := options.Context
	if runCtx == nil {
		runCtx = context.Background()
	}

	var brokerProxy *runnerBrokerProxy
	var runtimeBroker rt.Broker
	if r.broker != nil {
		brokerProxy = r.newBrokerProxy(runCtx, workflow, logger)
		runtimeBroker = brokerProxy
	}

	execContext := r.factory.New(workflow, runtimeBroker)
	if brokerProxy != nil {
		brokerProxy.attach(execContext)
	}
	execContext.SetLogger(logger)
	var initialActions map[string]bool
	var initialStages map[string]bool
	if provider, ok := workflow.(rt.DisableSnapshotProvider); ok {
		if actions, stages := provider.DisabledSnapshot(); actions != nil || stages != nil {
			if actions != nil {
				initialActions = copyBoolMap(actions)
			}
			if stages != nil {
				initialStages = copyBoolMap(stages)
			}
		}
	}
	execContext.SetDisabledMaps(initialActions, initialStages)

	if cancelable, ok := execContext.(interface{ Cancel(error) }); ok && runCtx != nil {
		ctxDone := runCtx.Done()
		go func() {
			<-ctxDone
			cancelable.Cancel(runCtx.Err())
		}()
	}

	if wfStore := workflow.Store(); !wfStore.IsZero() && options.InitialStore != nil {
		for key, value := range options.InitialStore {
			_ = storepkg.Put(wfStore, key, value)
		}
	}

	tele := runtimeengine.NewTelemetry(workflow, StatusPending)
	// telemetry is maintained within the runner; broker proxy no longer mutates it directly.
	start := time.Now()
	if r.broker != nil {
		r.registerWorkflow(runCtx, workflow, logger)
		r.notifyWorkflowStatus(runCtx, logger, workflow.ID(), StatusRunning)
	}
	err := r.executeWorkflow(runCtx, workflow, execContext, logger, tele)
	duration := time.Since(start)

	result := RunResult{
		WorkflowID:     workflow.ID(),
		Success:        err == nil,
		Error:          err,
		Duration:       duration,
		FinalStore:     nil,
		Stages:         tele.StageStatuses(),
		Actions:        tele.ActionStatuses(),
		DynamicStages:  tele.DynamicStages(),
		DynamicActions: tele.DynamicActions(),
	}
	if options.Attempt > 0 {
		result.Attempt = options.Attempt
	}

	if wfStore := workflow.Store(); !wfStore.IsZero() {
		result.FinalStore = storepkg.ExportAll(wfStore)
	}

	if actions, stages := execContext.DisabledMaps(); actions != nil || stages != nil {
		if actions != nil {
			result.DisabledActions = copyBoolMap(actions)
		}
		if stages != nil {
			result.DisabledStages = copyBoolMap(stages)
		}
	}

	result.RemovedStages = tele.RemovedStages()
	result.RemovedActions = tele.RemovedActions()

	if err != nil && options.IgnoreErrors {
		result.Success = true
		result.Error = nil
	}

	if r.broker != nil {
		workflowState := StatusCompleted
		if !result.Success {
			if errors.Is(err, context.Canceled) {
				workflowState = StatusCancelled
			} else {
				workflowState = StatusFailed
			}
		}
		r.flushStageStatuses(runCtx, workflow, tele, logger)
		r.flushActionStatuses(runCtx, workflow, tele, logger)
		r.notifyWorkflowStatus(runCtx, logger, workflow.ID(), workflowState)
	}

	return result
}

func (r *Runner) executeWorkflow(ctx context.Context, workflow rt.Workflow, execCtx core.ExecutionContext, logger rt.Logger, tele *runtimeengine.Telemetry) error {
	stages := append([]rt.Stage(nil), workflow.Stages()...)
	combinedMW := append([]rt.WorkflowMiddleware{}, r.workflowMW...)
	if mw := workflow.Middlewares(); len(mw) > 0 {
		combinedMW = append(combinedMW, mw...)
	}

	stageRunner := func(runCtx context.Context, stage rt.Stage, wf rt.Workflow, log rt.Logger) error {
		return r.executeStage(runCtx, wf, stage, execCtx, log, tele)
	}

	for i := len(combinedMW) - 1; i >= 0; i-- {
		stageRunner = combinedMW[i](stageRunner)
	}

	disabledStages := tele.DisabledStages(execCtx)

	for i := 0; i < len(stages); i++ {
		r.drainRemovedStages(execCtx, tele, ctx, workflow, logger)
		stage := stages[i]
		stageStatus := tele.Stage(stage.ID(), stage.Name(), stage.Description(), stage.Tags(), false, "")

		if disabledStages[stage.ID()] {
			stageStatus.Status = StatusSkipped
			r.notifyStageStatus(ctx, workflow.ID(), stage.ID(), StatusSkipped, logger)
			continue
		}

		stageStatus.Status = StatusRunning
		r.notifyStageStatus(ctx, workflow.ID(), stage.ID(), StatusRunning, logger)
		err := stageRunner(ctx, stage, workflow, logger)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				stageStatus.Status = StatusCancelled
				r.notifyStageStatus(ctx, workflow.ID(), stage.ID(), StatusCancelled, logger)
			} else {
				stageStatus.Status = StatusFailed
				r.notifyStageStatus(ctx, workflow.ID(), stage.ID(), StatusFailed, logger)
			}
			return err
		}
		if errors.Is(ctx.Err(), context.Canceled) {
			stageStatus.Status = StatusCancelled
			r.notifyStageStatus(ctx, workflow.ID(), stage.ID(), StatusCancelled, logger)
			return ctx.Err()
		}
		stageStatus.Status = StatusCompleted
		r.notifyStageStatus(ctx, workflow.ID(), stage.ID(), StatusCompleted, logger)

		if tele.HasPendingStages() {
			stages = tele.InsertPendingStages(stages, i+1)
		}
		r.drainRemovedStages(execCtx, tele, ctx, workflow, logger)
	}

	r.drainRemovedStages(execCtx, tele, ctx, workflow, logger)
	return nil
}

func (r *Runner) executeStage(ctx context.Context, workflow rt.Workflow, stage rt.Stage, execCtx core.ExecutionContext, logger rt.Logger, tele *runtimeengine.Telemetry) error {
	if err := mergeStageInitialStore(workflow, stage); err != nil {
		return err
	}

	stageActions := append([]rt.Action(nil), stage.ActionList()...)
	execCtx.SetStage(stage)
	execCtx.SetActionList(stageActions)
	defer execCtx.ClearStage()

	stageRunner := func(runCtx context.Context, st rt.Stage, wf rt.Workflow, log rt.Logger) error {
		return r.runActions(runCtx, wf, st, execCtx, logger, tele)
	}

	if mw := stage.Middlewares(); len(mw) > 0 {
		for i := len(mw) - 1; i >= 0; i-- {
			stageRunner = mw[i](stageRunner)
		}
	}

	return stageRunner(ctx, stage, workflow, logger)
}

func (r *Runner) runActions(ctx context.Context, workflow rt.Workflow, stage rt.Stage, execCtx core.ExecutionContext, logger rt.Logger, tele *runtimeengine.Telemetry) error {
	actionList := append([]rt.Action(nil), stage.ActionList()...)
	execCtx.SetActionList(actionList)
	disabledActions, _ := execCtx.DisabledMaps()
	stageMW := collectActionMiddleware(stage)

	for i := 0; i < len(actionList); i++ {
		action := actionList[i]
		actionKey := runtimeengine.ActionKey(stage.ID(), action.Name())
		actionStatus := tele.Action(stage.ID(), action.Name(), action.Description(), action.Tags(), false, "")

		if err := r.ensureActionRegistered(action); err != nil {
			actionStatus.Status = StatusFailed
			r.notifyActionStatus(ctx, workflow.ID(), stage.ID(), action.Name(), StatusFailed, logger)
			return err
		}

		if removed, createdBy := execCtx.ConsumeRemovedAction(stage.ID(), action.Name()); removed {
			actionStatus.Status = StatusRemoved
			r.updateActionTelemetryRemoval(tele, stage.ID(), action.Name(), createdBy)
			r.notifyActionRemoved(ctx, workflow.ID(), stage.ID(), action.Name(), createdBy, logger)
			continue
		}

		if disabledActions != nil && disabledActions[action.Name()] {
			actionStatus.Status = StatusSkipped
			r.notifyActionStatus(ctx, workflow.ID(), stage.ID(), action.Name(), StatusSkipped, logger)
			continue
		}

		actionStatus.Status = StatusRunning
		execCtx.SetAction(action, i, i == len(actionList)-1)
		r.notifyActionStatus(ctx, workflow.ID(), stage.ID(), action.Name(), StatusRunning, logger)

		runnerFn := func(ctx rt.Context, act rt.Action, index int, isLast bool) error {
			return act.Execute(ctx)
		}
		if chainProvider, ok := action.(actionMiddlewareChain); ok {
			if chain := chainProvider.MiddlewareChain(); len(chain) > 0 {
				for idx := len(chain) - 1; idx >= 0; idx-- {
					runnerFn = chain[idx](runnerFn)
				}
			}
		}
		if len(stageMW) > 0 {
			for idx := len(stageMW) - 1; idx >= 0; idx-- {
				runnerFn = stageMW[idx](runnerFn)
			}
		}

		err := runnerFn(execCtx, action, i, i == len(actionList)-1)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(execCtx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
				actionStatus.Status = StatusCancelled
				r.notifyActionStatus(ctx, workflow.ID(), stage.ID(), action.Name(), StatusCancelled, logger)
			} else {
				actionStatus.Status = StatusFailed
				r.notifyActionStatus(ctx, workflow.ID(), stage.ID(), action.Name(), StatusFailed, logger)
			}
			return err
		}
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(execCtx.Err(), context.Canceled) {
			actionStatus.Status = StatusCancelled
			r.notifyActionStatus(ctx, workflow.ID(), stage.ID(), action.Name(), StatusCancelled, logger)
			return ctx.Err()
		}

		actionStatus.Status = StatusCompleted
		r.notifyActionStatus(ctx, workflow.ID(), stage.ID(), action.Name(), StatusCompleted, logger)

		if added := execCtx.ConsumeDynamicActions(); len(added) > 0 {
			actionList = insertActions(actionList, added, i+1)
			execCtx.SetActionList(actionList)
			for _, dyn := range added {
				dynStatus := tele.Action(stage.ID(), dyn.Name(), dyn.Description(), dyn.Tags(), true, actionKey)
				dynStatus.Status = StatusPending
				tele.AddDynamicAction(stage.ID(), dyn, actionKey)
				r.registerAction(ctx, logger, workflow.ID(), stage.ID(), dyn, true, actionKey)
			}
		}

		if dynStages := execCtx.ConsumeDynamicStages(); len(dynStages) > 0 {
			for _, dynStage := range dynStages {
				t := tele.Stage(dynStage.ID(), dynStage.Name(), dynStage.Description(), dynStage.Tags(), true, actionKey)
				t.Status = StatusPending
				tele.AddDynamicStage(dynStage, actionKey)
				r.registerStage(ctx, workflow, dynStage, true, actionKey, logger)
			}
		}
	}

	return nil
}

func insertActions(actions []rt.Action, additions []rt.Action, index int) []rt.Action {
	if index >= len(actions) {
		return append(actions, additions...)
	}
	result := make([]rt.Action, 0, len(actions)+len(additions))
	result = append(result, actions[:index]...)
	result = append(result, additions...)
	result = append(result, actions[index:]...)
	return result
}

func mergeStageInitialStore(workflow rt.Workflow, stage rt.Stage) error {
	wfStore := workflow.Store()
	if wfStore.IsZero() {
		return nil
	}
	initial := stage.InitialStore()
	if initial.IsZero() {
		return nil
	}
	_, _, err := storepkg.CopyFromWithOverwrite(wfStore, initial)
	return err
}

type actionMiddlewareProvider interface {
	ActionMiddlewares() []rt.ActionMiddleware
}

type actionMiddlewareChain interface {
	MiddlewareChain() []rt.ActionMiddleware
}

func collectActionMiddleware(stage rt.Stage) []rt.ActionMiddleware {
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

func copyStringMap(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func groupActionsByStage(actions []ActionStatus) map[string][]ActionStatus {
	if len(actions) == 0 {
		return nil
	}
	grouped := make(map[string][]ActionStatus)
	for _, action := range actions {
		stageID := action.StageID
		grouped[stageID] = append(grouped[stageID], action)
	}
	return grouped
}

func (result RunResult) ToExecutionReport(workflow rt.Workflow, finalState ExecutionStatus, started time.Time) state.ExecutionReport {
	report := state.ExecutionReport{
		WorkflowID:      result.WorkflowID,
		Status:          toWorkflowState(finalState),
		Success:         result.Success,
		Duration:        result.Duration,
		Stages:          buildStageSummaries(result),
		DisabledStages:  copyBoolMap(result.DisabledStages),
		DisabledActions: copyBoolMap(result.DisabledActions),
		RemovedStages:   copyStringMap(result.RemovedStages),
		RemovedActions:  copyStringMap(result.RemovedActions),
		Attempt:         result.Attempt,
	}
	if !started.IsZero() {
		report.StartedAt = started
		if result.Duration > 0 {
			report.CompletedAt = started.Add(result.Duration)
		} else {
			report.CompletedAt = started
		}
	}
	if result.Error != nil {
		report.ErrorMessage = result.Error.Error()
	}
	if len(result.FinalStore) > 0 {
		report.FinalStore = copyMetadata(result.FinalStore)
	}
	if workflow != nil {
		report.WorkflowName = workflow.Name()
		report.Description = workflow.Description()
		report.WorkflowTags = copyStrings(workflow.Tags())
		if typed, ok := workflow.(rt.TypedWorkflow); ok {
			report.WorkflowType = typed.WorkflowType()
		}
	}
	return report
}

func buildStageSummaries(result RunResult) []state.StageSummary {
	if len(result.Stages) == 0 {
		return nil
	}
	groupedActions := groupActionsByStage(result.Actions)
	summaries := make([]state.StageSummary, 0, len(result.Stages))
	for _, stage := range result.Stages {
		summary := state.StageSummary{
			ID:          stage.ID,
			Name:        stage.Name,
			Description: stage.Description,
			Tags:        copyStrings(stage.Tags),
			Dynamic:     stage.Dynamic,
			CreatedBy:   stage.CreatedBy,
			Status:      toWorkflowState(stage.Status),
			Disabled:    result.DisabledStages != nil && result.DisabledStages[stage.ID],
		}
		if result.RemovedStages != nil {
			summary.RemovedBy = result.RemovedStages[stage.ID]
		}
		if actions := groupedActions[stage.ID]; len(actions) > 0 {
			summary.Actions = buildActionSummariesForStage(stage.ID, actions, result)
		}
		summaries = append(summaries, summary)
	}
	return summaries
}

func buildActionSummariesForStage(stageID string, actions []ActionStatus, result RunResult) []state.ActionSummary {
	if len(actions) == 0 {
		return nil
	}
	summaries := make([]state.ActionSummary, 0, len(actions))
	for _, action := range actions {
		summary := state.ActionSummary{
			StageID:     stageID,
			Name:        action.Name,
			Description: action.Description,
			Tags:        copyStrings(action.Tags),
			Dynamic:     action.Dynamic,
			CreatedBy:   action.CreatedBy,
			Status:      toWorkflowState(action.Status),
			Disabled:    result.DisabledActions != nil && result.DisabledActions[action.Name],
		}
		if result.RemovedActions != nil {
			summary.RemovedBy = result.RemovedActions[runtimeengine.ActionKey(stageID, action.Name)]
		}
		summaries = append(summaries, summary)
	}
	return summaries
}

func (r *Runner) registerWorkflow(ctx context.Context, workflow rt.Workflow, logger rt.Logger) {
	if r.broker == nil || workflow == nil {
		return
	}
	stages := workflow.Stages()
	stageMap := make(map[string]*state.StageRecord, len(stages))
	for _, stage := range stages {
		if stage == nil {
			continue
		}
		rec := makeStageRecord(stage, false, "")
		copy := rec
		stageMap[stage.ID()] = &copy
	}
	definition := state.SubWorkflowDef{
		ID:        workflow.ID(),
		Name:      workflow.Name(),
		Tags:      copyStrings(workflow.Tags()),
		Metadata:  copyMetadata(workflow.Metadata()),
		Priority:  state.PriorityDefault,
		CreatedAt: time.Now(),
	}
	if typed, ok := workflow.(rt.TypedWorkflow); ok {
		definition.Type = typed.WorkflowType()
		definition.Payload = copyMetadata(typed.WorkflowPayload())
	}
	record := state.WorkflowRecord{
		ID:          state.WorkflowID(workflow.ID()),
		Name:        workflow.Name(),
		Description: workflow.Description(),
		Tags:        copyStrings(workflow.Tags()),
		State:       state.WorkflowPending,
		Definition:  definition,
		Stages:      stageMap,
	}
	r.safeBrokerCall(logger, "WorkflowRegistered", func() error {
		return r.broker.WorkflowRegistered(ctx, record)
	})
	for _, stage := range stages {
		r.registerStage(ctx, workflow, stage, false, "", logger)
	}
}

func (r *Runner) registerStage(ctx context.Context, workflow rt.Workflow, stage rt.Stage, dynamic bool, createdBy string, logger rt.Logger) {
	if r.broker == nil || workflow == nil || stage == nil {
		return
	}
	rec := makeStageRecord(stage, dynamic, createdBy)
	r.safeBrokerCall(logger, "StageRegistered", func() error {
		return r.broker.StageRegistered(ctx, workflow.ID(), rec)
	})
	for _, action := range stage.ActionList() {
		r.registerAction(ctx, logger, workflow.ID(), stage.ID(), action, dynamic, createdBy)
	}
}

func (r *Runner) registerAction(ctx context.Context, logger rt.Logger, workflowID, stageID string, action rt.Action, dynamic bool, createdBy string) {
	if r.broker == nil || action == nil {
		return
	}
	rec := makeActionRecord(action, dynamic, createdBy)
	r.safeBrokerCall(logger, "ActionRegistered", func() error {
		return r.broker.ActionRegistered(ctx, workflowID, stageID, rec)
	})
}

func (r *Runner) flushStageStatuses(ctx context.Context, workflow rt.Workflow, tele *runtimeengine.Telemetry, logger rt.Logger) {
	if r.broker == nil || workflow == nil || tele == nil {
		return
	}
	for _, status := range tele.StageStatuses() {
		r.notifyStageStatus(ctx, workflow.ID(), status.ID, status.Status, logger)
	}
}

func (r *Runner) flushActionStatuses(ctx context.Context, workflow rt.Workflow, tele *runtimeengine.Telemetry, logger rt.Logger) {
	if r.broker == nil || workflow == nil || tele == nil {
		return
	}
	for _, status := range tele.ActionStatuses() {
		r.notifyActionStatus(ctx, workflow.ID(), status.StageID, status.Name, status.Status, logger)
	}
}

func (r *Runner) notifyWorkflowStatus(ctx context.Context, logger rt.Logger, workflowID string, status ExecutionStatus) {
	if r.broker == nil {
		return
	}
	stateValue := toWorkflowState(status)
	callCtx := brokerContext(ctx)
	r.safeBrokerCall(logger, "WorkflowStatus", func() error {
		return r.broker.WorkflowStatus(callCtx, workflowID, stateValue)
	})
}

func (r *Runner) notifyStageStatus(ctx context.Context, workflowID, stageID string, status ExecutionStatus, logger rt.Logger) {
	if r.broker == nil {
		return
	}
	stateValue := toWorkflowState(status)
	callCtx := brokerContext(ctx)
	r.safeBrokerCall(logger, "StageStatus", func() error {
		return r.broker.StageStatus(callCtx, workflowID, stageID, stateValue)
	})
}

func (r *Runner) notifyActionStatus(ctx context.Context, workflowID, stageID, actionName string, status ExecutionStatus, logger rt.Logger) {
	if r.broker == nil {
		return
	}
	stateValue := toWorkflowState(status)
	callCtx := brokerContext(ctx)
	r.safeBrokerCall(logger, "ActionStatus", func() error {
		return r.broker.ActionStatus(callCtx, workflowID, stageID, actionName, stateValue)
	})
}

func (r *Runner) notifyActionProgress(ctx context.Context, workflowID, stageID, actionName string, progress int, message string, logger rt.Logger) {
	if r.broker == nil {
		return
	}
	callCtx := brokerContext(ctx)
	r.safeBrokerCall(logger, "ActionProgress", func() error {
		return r.broker.ActionProgress(callCtx, workflowID, stageID, actionName, progress, message)
	})
}

func (r *Runner) notifyActionEvent(ctx context.Context, workflowID, stageID, actionName, kind, message string, metadata map[string]any, logger rt.Logger) {
	if r.broker == nil {
		return
	}
	var metaCopy map[string]any
	if len(metadata) > 0 {
		metaCopy = make(map[string]any, len(metadata))
		for k, v := range metadata {
			metaCopy[k] = v
		}
	}
	callCtx := brokerContext(ctx)
	r.safeBrokerCall(logger, "ActionEvent", func() error {
		return r.broker.ActionEvent(callCtx, workflowID, stageID, actionName, kind, message, metaCopy)
	})
}

func (r *Runner) notifyActionRemoved(ctx context.Context, workflowID, stageID, actionName, createdBy string, logger rt.Logger) {
	r.notifyActionStatus(ctx, workflowID, stageID, actionName, StatusRemoved, logger)
	if r.broker != nil {
		callCtx := brokerContext(ctx)
		r.safeBrokerCall(logger, "ActionRemoved", func() error {
			return r.broker.ActionRemoved(callCtx, workflowID, stageID, actionName, createdBy)
		})
	}
}

func (r *Runner) notifyStageRemoved(ctx context.Context, workflowID, stageID, createdBy string, logger rt.Logger) {
	r.notifyStageStatus(ctx, workflowID, stageID, StatusRemoved, logger)
	if r.broker != nil {
		callCtx := brokerContext(ctx)
		r.safeBrokerCall(logger, "StageRemoved", func() error {
			return r.broker.StageRemoved(callCtx, workflowID, stageID, createdBy)
		})
	}
}

func (r *Runner) drainRemovedStages(execCtx core.ExecutionContext, tele *runtimeengine.Telemetry, ctx context.Context, workflow rt.Workflow, logger rt.Logger) {
	if execCtx == nil || workflow == nil {
		return
	}
	removed := execCtx.ConsumeRemovedStages()
	if len(removed) == 0 {
		return
	}
	for stageID, createdBy := range removed {
		r.updateStageTelemetryRemoval(tele, stageID, createdBy)
		r.notifyStageRemoved(ctx, workflow.ID(), stageID, createdBy, logger)
	}
}

func (r *Runner) safeBrokerCall(logger rt.Logger, op string, fn func() error) {
	if err := fn(); err != nil {
		if logger != nil {
			logger.Warn("runner broker %s failed: %v", op, err)
		}
	}
}

func brokerContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

func (r *Runner) newBrokerProxy(ctx context.Context, workflow rt.Workflow, logger rt.Logger) *runnerBrokerProxy {
	return &runnerBrokerProxy{
		runner:   r,
		workflow: workflow,
		runCtx:   ctx,
		logger:   logger,
	}
}

type runnerBrokerProxy struct {
	runner   *Runner
	workflow rt.Workflow
	exec     core.ExecutionContext
	runCtx   context.Context
	logger   rt.Logger
}

func (p *runnerBrokerProxy) attach(exec core.ExecutionContext) {
	p.exec = exec
}

func (p *runnerBrokerProxy) Progress(percent int, message string) error {
	if p == nil || p.runner == nil {
		return nil
	}

	var stageID, actionID string
	if p.exec != nil {
		if stage := p.exec.Stage(); stage != nil {
			stageID = stage.ID()
		}
		if action := p.exec.Action(); action != nil {
			actionID = action.Name()
		}
	}

	if p.workflow == nil || stageID == "" || actionID == "" {
		return nil
	}

	p.runner.notifyActionProgress(
		p.runCtx,
		p.workflow.ID(),
		stageID,
		actionID,
		percent,
		message,
		p.logger,
	)
	return nil
}

func (p *runnerBrokerProxy) Event(kind, message string, metadata map[string]any) error {
	if p == nil || p.runner == nil || p.workflow == nil {
		return nil
	}
	stageID, actionID := "", ""
	if p.exec != nil {
		if stage := p.exec.Stage(); stage != nil {
			stageID = stage.ID()
		}
		if action := p.exec.Action(); action != nil {
			actionID = action.Name()
		}
	}
	p.runner.notifyActionEvent(
		p.runCtx,
		p.workflow.ID(),
		stageID,
		actionID,
		kind,
		message,
		metadata,
		p.logger,
	)
	return nil
}

func (r *Runner) validateWorkflowRegistration(workflow rt.Workflow) error {
	if workflow == nil {
		return fmt.Errorf("runner: workflow is nil")
	}
	for _, stage := range workflow.Stages() {
		if stage == nil {
			continue
		}
		for _, action := range stage.ActionList() {
			if err := r.ensureActionRegistered(action); err != nil {
				return err
			}
		}
	}
	return nil
}

type actionWithRef interface {
	Ref() string
}

func (r *Runner) ensureActionRegistered(action rt.Action) error {
	if r.registry == nil {
		return fmt.Errorf("runner: registry is nil")
	}
	if action == nil {
		return fmt.Errorf("runner: action is nil")
	}
	id := action.Name()
	if id == "" {
		return fmt.Errorf("runner: empty action id")
	}
	if r.registry.HasAction(id) {
		return nil
	}
	if withRef, ok := action.(actionWithRef); ok {
		if ref := withRef.Ref(); ref != "" && r.registry.HasAction(ref) {
			return nil
		}
	}
	return fmt.Errorf("runner: action %s not registered", id)
}

func (r *Runner) updateActionTelemetryRemoval(tele *runtimeengine.Telemetry, stageID, actionName, createdBy string) {
	if tele == nil {
		return
	}
	tele.MarkActionRemoved(stageID, actionName, createdBy)
}

func (r *Runner) updateStageTelemetryRemoval(tele *runtimeengine.Telemetry, stageID, createdBy string) {
	if tele == nil {
		return
	}
	tele.MarkStageRemoved(stageID, createdBy)
}

func makeStageRecord(stage rt.Stage, dynamic bool, createdBy string) state.StageRecord {
	record := state.StageRecord{
		ID:          stage.ID(),
		Name:        stage.Name(),
		Description: stage.Description(),
		Tags:        copyStrings(stage.Tags()),
		Dynamic:     dynamic,
		CreatedBy:   createdBy,
		Status:      state.WorkflowPending,
		Actions:     make(map[string]*state.ActionRecord),
	}
	for _, action := range stage.ActionList() {
		actionRecord := makeActionRecord(action, dynamic, createdBy)
		recCopy := actionRecord
		record.Actions[action.Name()] = &recCopy
	}
	return record
}

func makeActionRecord(action rt.Action, dynamic bool, createdBy string) state.ActionRecord {
	ref := action.Name()
	if withRef, ok := action.(actionWithRef); ok {
		if candidate := withRef.Ref(); candidate != "" {
			ref = candidate
		}
	}
	return state.ActionRecord{
		Ref:         ref,
		Name:        action.Name(),
		Description: action.Description(),
		Tags:        copyStrings(action.Tags()),
		Dynamic:     dynamic,
		CreatedBy:   createdBy,
		Status:      state.WorkflowPending,
	}
}

func copyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	copyValues := make([]string, len(values))
	copy(copyValues, values)
	return copyValues
}

func copyMetadata(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	dup := make(map[string]interface{}, len(src))
	for k, v := range src {
		dup[k] = v
	}
	return dup
}

func toWorkflowState(status ExecutionStatus) state.WorkflowState {
	switch status {
	case StatusPending:
		return state.WorkflowPending
	case StatusRunning:
		return state.WorkflowRunning
	case StatusCompleted:
		return state.WorkflowCompleted
	case StatusFailed:
		return state.WorkflowFailed
	case StatusSkipped:
		return state.WorkflowSkipped
	case StatusRemoved:
		return state.WorkflowRemoved
	case StatusCancelled:
		return state.WorkflowCancelled
	default:
		return state.WorkflowPending
	}
}
