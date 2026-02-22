package gostage

import (
	"fmt"
	"reflect"
	"time"

	"github.com/davidroman0O/gostage/store"
)

// WorkflowBuilder constructs a workflow using a fluent builder pattern.
// It produces a standard *Workflow when Commit() is called.
//
//	wf := gostage.NewWorkflow("process-order").
//	    Step("validate.input").
//	    Step("payment.charge").
//	    Step("order.fulfill").
//	    Commit()
// StepCallback is called when a step completes.
type StepCallback func(stepName string, ctx *Ctx)

// ErrorCallback is called when an error occurs during execution.
type ErrorCallback func(err error)

type WorkflowBuilder struct {
	id              string
	name            string
	description     string
	tags            []string
	steps           []stepDef
	options         []WorkflowOption
	onStepComplete  StepCallback
	onError         ErrorCallback
	defaultRetries  int
	defaultRetryDelay time.Duration
}

// WorkflowOption configures a workflow during construction.
type WorkflowOption func(*WorkflowBuilder)

// stepKind describes what kind of step this is.
type stepKind int

const (
	stepSingle   stepKind = iota // A single task
	stepParallel                 // Multiple tasks in parallel
	stepBranch                   // Conditional branching
	stepForEach                  // Iteration over a collection
	stepStage                    // Named stage grouping
	stepMap                      // Data transformation
	stepDoUntil                  // Loop until condition
	stepDoWhile                  // Loop while condition
	stepSub                      // Nested sub-workflow
	stepSleep                    // Timed delay
)

// stepDef is an internal representation of a workflow step.
type stepDef struct {
	kind stepKind

	// stepSingle
	taskName string

	// stepStage
	stageName  string
	stageSteps []string

	// stepParallel
	parallelTasks []string

	// stepBranch
	branches    []BranchCase
	defaultCase *BranchCase

	// stepForEach
	collectionKey    string
	forEachTask      string
	forEachOpts      []ForEachOption
	forEachConcurrency int

	// stepMap
	mapFn func(*Ctx)

	// stepDoUntil / stepDoWhile
	loopTask string
	loopCond func(*Ctx) bool

	// stepSub
	subWorkflow *Workflow

	// stepSleep
	sleepDuration time.Duration
}

// NewWorkflow creates a new WorkflowBuilder with the given ID.
// Call .Step(), .Parallel(), .Branch(), etc. to add steps,
// then .Commit() to produce the final *Workflow.
//
//	wf := gostage.NewWorkflow("my-workflow").
//	    Step("task-a").
//	    Step("task-b").
//	    Commit()
func NewWorkflowBuilder(id string, opts ...WorkflowOption) *WorkflowBuilder {
	b := &WorkflowBuilder{
		id:   id,
		name: id,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// WithName sets the workflow's human-readable name.
func WithName(name string) WorkflowOption {
	return func(b *WorkflowBuilder) {
		b.name = name
	}
}

// WithWorkflowDescription sets the workflow description.
func WithWorkflowDescription(desc string) WorkflowOption {
	return func(b *WorkflowBuilder) {
		b.description = desc
	}
}

// WithWorkflowTags sets the workflow tags.
func WithWorkflowTags(tags ...string) WorkflowOption {
	return func(b *WorkflowBuilder) {
		b.tags = append(b.tags, tags...)
	}
}

// OnStepComplete sets a callback that fires after each step completes successfully.
func OnStepComplete(fn StepCallback) WorkflowOption {
	return func(b *WorkflowBuilder) {
		b.onStepComplete = fn
	}
}

// OnError sets a callback that fires when any step returns an error.
func OnError(fn ErrorCallback) WorkflowOption {
	return func(b *WorkflowBuilder) {
		b.onError = fn
	}
}

// WithDefaultRetry sets the default retry count for all tasks in the workflow.
// Per-task WithRetry overrides this.
func WithDefaultRetry(n int, delay time.Duration) WorkflowOption {
	return func(b *WorkflowBuilder) {
		b.defaultRetries = n
		b.defaultRetryDelay = delay
	}
}

// Step adds a single task step to the workflow.
// The taskName must match a previously registered Task().
func (b *WorkflowBuilder) Step(taskName string) *WorkflowBuilder {
	b.steps = append(b.steps, stepDef{
		kind:     stepSingle,
		taskName: taskName,
	})
	return b
}

// Stage groups multiple task steps under a named stage.
//
//	wf := gostage.NewWorkflow("pipeline").
//	    Stage("validation",
//	        "validate.input",
//	        "validate.rules",
//	    ).
//	    Stage("payment",
//	        "charge.card",
//	    ).
//	    Commit()
func (b *WorkflowBuilder) Stage(name string, taskNames ...string) *WorkflowBuilder {
	b.steps = append(b.steps, stepDef{
		kind:       stepStage,
		stageName:  name,
		stageSteps: taskNames,
	})
	return b
}

// Parallel adds multiple tasks that execute concurrently.
//
//	wf := gostage.NewWorkflow("order").
//	    Step("validate").
//	    Parallel("payment.charge", "inventory.reserve", "fraud.check").
//	    Step("fulfill").
//	    Commit()
func (b *WorkflowBuilder) Parallel(taskNames ...string) *WorkflowBuilder {
	b.steps = append(b.steps, stepDef{
		kind:          stepParallel,
		parallelTasks: taskNames,
	})
	return b
}

// BranchCase defines a conditional branch.
type BranchCase struct {
	condition func(*Ctx) bool
	taskName  string
	isDefault bool
}

// When creates a conditional branch case.
func When(cond func(*Ctx) bool, taskName string) BranchCase {
	return BranchCase{condition: cond, taskName: taskName}
}

// Default creates the default (fallback) branch case.
func Default(taskName string) BranchCase {
	return BranchCase{taskName: taskName, isDefault: true}
}

// Branch adds conditional branching to the workflow.
//
//	wf := gostage.NewWorkflow("process").
//	    Branch(
//	        gostage.When(func(ctx *gostage.Ctx) bool {
//	            return gostage.Get[string](ctx, "priority") == "high"
//	        }, "urgent.process"),
//	        gostage.Default("normal.process"),
//	    ).
//	    Commit()
func (b *WorkflowBuilder) Branch(cases ...BranchCase) *WorkflowBuilder {
	sd := stepDef{kind: stepBranch}
	for _, c := range cases {
		if c.isDefault {
			dc := c
			sd.defaultCase = &dc
		} else {
			sd.branches = append(sd.branches, c)
		}
	}
	b.steps = append(b.steps, sd)
	return b
}

// ForEachOption configures a ForEach step.
type ForEachOption func(*stepDef)

// WithConcurrency sets the concurrency level for ForEach.
func WithConcurrency(n int) ForEachOption {
	return func(sd *stepDef) {
		sd.forEachConcurrency = n
	}
}

// ForEach iterates over a collection in the store and runs a task for each item.
//
//	wf := gostage.NewWorkflow("batch").
//	    Step("discover.tracks").
//	    ForEach("tracks", "download.track", gostage.WithConcurrency(4)).
//	    Step("finalize").
//	    Commit()
func (b *WorkflowBuilder) ForEach(collectionKey string, taskName string, opts ...ForEachOption) *WorkflowBuilder {
	sd := stepDef{
		kind:          stepForEach,
		collectionKey: collectionKey,
		forEachTask:   taskName,
		forEachOpts:   opts,
	}
	for _, opt := range opts {
		opt(&sd)
	}
	b.steps = append(b.steps, sd)
	return b
}

// Map adds a data transformation step that doesn't execute a registered task.
// Instead it runs an inline function to reshape data between steps.
//
//	wf := gostage.NewWorkflow("transform").
//	    Step("fetch.raw").
//	    Map(func(ctx *gostage.Ctx) {
//	        raw := gostage.Get[[]byte](ctx, "raw")
//	        gostage.Set(ctx, "records", parseCSV(raw))
//	    }).
//	    Commit()
func (b *WorkflowBuilder) Map(fn func(*Ctx)) *WorkflowBuilder {
	b.steps = append(b.steps, stepDef{
		kind:  stepMap,
		mapFn: fn,
	})
	return b
}

// DoUntil repeats a task until the condition returns true.
//
//	wf := gostage.NewWorkflow("poll").
//	    DoUntil("check.status", func(ctx *gostage.Ctx) bool {
//	        return gostage.Get[string](ctx, "status") == "ready"
//	    }).
//	    Commit()
func (b *WorkflowBuilder) DoUntil(taskName string, cond func(*Ctx) bool) *WorkflowBuilder {
	b.steps = append(b.steps, stepDef{
		kind:     stepDoUntil,
		loopTask: taskName,
		loopCond: cond,
	})
	return b
}

// DoWhile repeats a task while the condition returns true.
//
//	wf := gostage.NewWorkflow("paginate").
//	    Step("fetch.first.page").
//	    DoWhile("fetch.next.page", func(ctx *gostage.Ctx) bool {
//	        return gostage.Get[bool](ctx, "has_more")
//	    }).
//	    Commit()
func (b *WorkflowBuilder) DoWhile(taskName string, cond func(*Ctx) bool) *WorkflowBuilder {
	b.steps = append(b.steps, stepDef{
		kind:     stepDoWhile,
		loopTask: taskName,
		loopCond: cond,
	})
	return b
}

// Sub embeds a nested sub-workflow as a step.
// The sub-workflow's stages are executed inline as part of the parent workflow.
//
//	downloadFlow := gostage.NewWorkflowBuilder("download-and-convert").
//	    Step("download.file").
//	    Step("convert.format").
//	    Commit()
//
//	albumFlow := gostage.NewWorkflowBuilder("process-album").
//	    Step("discover").
//	    Sub(downloadFlow).
//	    Commit()
func (b *WorkflowBuilder) Sub(wf *Workflow) *WorkflowBuilder {
	b.steps = append(b.steps, stepDef{
		kind:        stepSub,
		subWorkflow: wf,
	})
	return b
}

// Sleep adds a timed delay step to the workflow.
// In a persistent engine, the run is saved as Sleeping and resumed after the duration.
// In synchronous execution, this blocks the goroutine with time.Sleep.
//
//	wf := gostage.NewWorkflowBuilder("delayed").
//	    Step("prepare").
//	    Sleep(1 * time.Hour).
//	    Step("send.notification").
//	    Commit()
func (b *WorkflowBuilder) Sleep(d time.Duration) *WorkflowBuilder {
	b.steps = append(b.steps, stepDef{
		kind:          stepSleep,
		sleepDuration: d,
	})
	return b
}

// Commit finalizes the builder and produces a *Workflow.
// It resolves all task references against the registry and
// creates the underlying Workflow → Stage → Action structure.
//
// For now, each Step becomes its own Stage with a single Action.
// Parallel, Branch, ForEach, Map, and loop steps become stages
// with special execution semantics handled by the engine.
func (b *WorkflowBuilder) Commit() *Workflow {
	wf := NewWorkflow(b.id, b.name, b.description)
	if len(b.tags) > 0 {
		wf.Tags = b.tags
		wf.saveToStore()
	}

	for i, sd := range b.steps {
		switch sd.kind {
		case stepSingle:
			stage := b.buildSingleStep(sd, i)
			wf.AddStage(stage)

		case stepStage:
			stage := b.buildNamedStage(sd, i)
			wf.AddStage(stage)

		case stepParallel:
			// For now, parallel steps are represented as a single stage
			// with multiple actions. The engine will detect the parallel
			// marker and run them concurrently.
			stage := b.buildParallelStep(sd, i)
			wf.AddStage(stage)

		case stepBranch:
			stage := b.buildBranchStep(sd, i)
			wf.AddStage(stage)

		case stepForEach:
			stage := b.buildForEachStep(sd, i)
			wf.AddStage(stage)

		case stepMap:
			stage := b.buildMapStep(sd, i)
			wf.AddStage(stage)

		case stepDoUntil, stepDoWhile:
			stage := b.buildLoopStep(sd, i)
			wf.AddStage(stage)

		case stepSub:
			// Inline the sub-workflow's stages into the parent workflow.
			for _, subStage := range sd.subWorkflow.Stages {
				wf.AddStage(subStage)
			}

		case stepSleep:
			stage := b.buildSleepStep(sd, i)
			wf.AddStage(stage)
		}
	}

	return wf
}

// buildSingleStep creates a stage with one action for a single task step.
func (b *WorkflowBuilder) buildSingleStep(sd stepDef, index int) *Stage {
	stageID := fmt.Sprintf("%s:step:%d:%s", b.id, index, sd.taskName)
	stage := NewStage(stageID, sd.taskName, "")

	action := resolveTaskAction(sd.taskName)
	stage.AddAction(action)

	return stage
}

// buildNamedStage creates a stage with multiple actions.
func (b *WorkflowBuilder) buildNamedStage(sd stepDef, index int) *Stage {
	stageID := fmt.Sprintf("%s:stage:%d:%s", b.id, index, sd.stageName)
	stage := NewStage(stageID, sd.stageName, "")

	for _, taskName := range sd.stageSteps {
		action := resolveTaskAction(taskName)
		stage.AddAction(action)
	}

	return stage
}

// buildParallelStep creates a stage marked for parallel execution.
func (b *WorkflowBuilder) buildParallelStep(sd stepDef, index int) *Stage {
	stageID := fmt.Sprintf("%s:parallel:%d", b.id, index)
	stage := NewStageWithTags(stageID, "parallel", "", []string{"__parallel"})

	for _, taskName := range sd.parallelTasks {
		action := resolveTaskAction(taskName)
		stage.AddAction(action)
	}

	return stage
}

// buildBranchStep creates a stage that holds branch logic.
func (b *WorkflowBuilder) buildBranchStep(sd stepDef, index int) *Stage {
	stageID := fmt.Sprintf("%s:branch:%d", b.id, index)
	stage := NewStageWithTags(stageID, "branch", "", []string{"__branch"})

	// Store the branch logic as a special action
	branchAction := &branchActionImpl{
		BaseAction: NewBaseAction("__branch", "Branch decision"),
		branches:   sd.branches,
		defaultCase: sd.defaultCase,
	}
	stage.AddAction(branchAction)

	return stage
}

// buildForEachStep creates a stage that iterates over a collection.
func (b *WorkflowBuilder) buildForEachStep(sd stepDef, index int) *Stage {
	stageID := fmt.Sprintf("%s:foreach:%d:%s", b.id, index, sd.collectionKey)
	stage := NewStageWithTags(stageID, "foreach:"+sd.collectionKey, "", []string{"__foreach"})

	forEachAction := &forEachActionImpl{
		BaseAction:    NewBaseAction("__foreach", "ForEach over "+sd.collectionKey),
		collectionKey: sd.collectionKey,
		taskName:      sd.forEachTask,
		concurrency:   sd.forEachConcurrency,
	}
	stage.AddAction(forEachAction)

	return stage
}

// buildMapStep creates a stage with an inline transformation action.
func (b *WorkflowBuilder) buildMapStep(sd stepDef, index int) *Stage {
	stageID := fmt.Sprintf("%s:map:%d", b.id, index)
	stage := NewStageWithTags(stageID, "map", "", []string{"__map"})

	mapAction := &mapActionImpl{
		BaseAction: NewBaseAction("__map", "Data transformation"),
		fn:         sd.mapFn,
	}
	stage.AddAction(mapAction)

	return stage
}

// buildLoopStep creates a stage that loops.
func (b *WorkflowBuilder) buildLoopStep(sd stepDef, index int) *Stage {
	kindStr := "do-until"
	if sd.kind == stepDoWhile {
		kindStr = "do-while"
	}
	stageID := fmt.Sprintf("%s:%s:%d:%s", b.id, kindStr, index, sd.loopTask)
	stage := NewStageWithTags(stageID, kindStr+":"+sd.loopTask, "", []string{"__loop", "__" + kindStr})

	loopAction := &loopActionImpl{
		BaseAction: NewBaseAction("__loop", kindStr+" loop"),
		taskName:   sd.loopTask,
		cond:       sd.loopCond,
		isWhile:    sd.kind == stepDoWhile,
	}
	stage.AddAction(loopAction)

	return stage
}

// buildSleepStep creates a stage that pauses execution for a duration.
func (b *WorkflowBuilder) buildSleepStep(sd stepDef, index int) *Stage {
	stageID := fmt.Sprintf("%s:sleep:%d", b.id, index)
	stage := NewStageWithTags(stageID, "sleep", "", []string{"__sleep"})

	sleepAction := &sleepActionImpl{
		BaseAction: NewBaseAction("__sleep", fmt.Sprintf("Sleep %s", sd.sleepDuration)),
		duration:   sd.sleepDuration,
	}
	stage.AddAction(sleepAction)

	return stage
}

// resolveTaskAction looks up a task in the registry and returns an Action.
// If the task isn't registered, it creates a placeholder that will fail at execution time.
func resolveTaskAction(taskName string) Action {
	// Try function-based task registry first
	if def := lookupTask(taskName); def != nil {
		return &taskAction{
			BaseAction: NewBaseAction(def.name, def.description),
			fn:         def.fn,
			tags:       def.tags,
			retries:    def.retries,
			retryDelay: def.retryDelay,
		}
	}

	// Try legacy action registry
	if action, err := NewActionFromRegistry(taskName); err == nil {
		return action
	}

	// Return a placeholder that fails at execution time with a clear message
	return &missingTaskAction{
		BaseAction: NewBaseAction(taskName, "unresolved task"),
	}
}

// --- Internal action implementations ---

type missingTaskAction struct {
	BaseAction
}

func (m *missingTaskAction) Execute(ctx *ActionContext) error {
	return fmt.Errorf("task %q is not registered; call gostage.Task() before building the workflow", m.Name())
}

// branchActionImpl evaluates conditions and delegates to the matching task.
type branchActionImpl struct {
	BaseAction
	branches    []BranchCase
	defaultCase *BranchCase
}

func (a *branchActionImpl) Execute(actCtx *ActionContext) error {
	ctx := newCtxFromActionContext(actCtx)

	for _, bc := range a.branches {
		if bc.condition(ctx) {
			action := resolveTaskAction(bc.taskName)
			return action.Execute(actCtx)
		}
	}

	if a.defaultCase != nil {
		action := resolveTaskAction(a.defaultCase.taskName)
		return action.Execute(actCtx)
	}

	return nil // no branch matched, no default
}

// forEachActionImpl iterates over a collection and runs a task per item.
type forEachActionImpl struct {
	BaseAction
	collectionKey string
	taskName      string
	concurrency   int
}

func (a *forEachActionImpl) Execute(actCtx *ActionContext) error {
	// Get the collection from the store.
	// Try []any first (most common for dynamically built collections).
	items, err := store.Get[[]any](actCtx.Store(), a.collectionKey)
	if err != nil {
		// Fall back to reflection-based extraction for typed slices
		items, err = getSliceAsAny(actCtx.Store(), a.collectionKey)
		if err != nil {
			return fmt.Errorf("ForEach: collection key %q not found or not a slice: %w", a.collectionKey, err)
		}
	}

	action := resolveTaskAction(a.taskName)

	// TODO: When concurrency > 1, run items in parallel with a semaphore.
	// For now, sequential execution.
	for i, item := range items {
		actCtx.Store().Put("__foreach_item", item)
		actCtx.Store().Put("__foreach_index", i)

		if err := action.Execute(actCtx); err != nil {
			return fmt.Errorf("ForEach item %d: %w", i, err)
		}
	}

	// Clean up iteration keys
	actCtx.Store().Delete("__foreach_item")
	actCtx.Store().Delete("__foreach_index")

	return nil
}

// mapActionImpl runs an inline data transformation.
type mapActionImpl struct {
	BaseAction
	fn func(*Ctx)
}

func (a *mapActionImpl) Execute(actCtx *ActionContext) error {
	ctx := newCtxFromActionContext(actCtx)
	a.fn(ctx)
	return nil
}

// loopActionImpl runs a task repeatedly based on a condition.
type loopActionImpl struct {
	BaseAction
	taskName string
	cond     func(*Ctx) bool
	isWhile  bool
}

func (a *loopActionImpl) Execute(actCtx *ActionContext) error {
	ctx := newCtxFromActionContext(actCtx)
	action := resolveTaskAction(a.taskName)

	const maxIterations = 10000 // safety limit

	for i := 0; i < maxIterations; i++ {
		// DoWhile: check condition first (except first iteration)
		if a.isWhile && i > 0 {
			if !a.cond(ctx) {
				break
			}
		}

		if err := action.Execute(actCtx); err != nil {
			return fmt.Errorf("loop iteration %d: %w", i, err)
		}

		// DoUntil: check condition after execution
		if !a.isWhile {
			if a.cond(ctx) {
				break
			}
		}
	}

	return nil
}

// sleepActionImpl pauses execution for a duration.
// TODO: In persistent mode, save run as Sleeping and resume after duration
// instead of blocking the goroutine.
type sleepActionImpl struct {
	BaseAction
	duration time.Duration
}

func (a *sleepActionImpl) Execute(actCtx *ActionContext) error {
	time.Sleep(a.duration)
	return nil
}

// getSliceAsAny retrieves a value from the store and converts any slice type
// to []any using reflection. This handles typed slices like []string, []int, etc.
func getSliceAsAny(s *store.KVStore, key string) ([]any, error) {
	// Export the store and look up the key.
	// This is less efficient but works for any stored type.
	all := s.ExportAll()
	raw, ok := all[key]
	if !ok {
		return nil, fmt.Errorf("key %q not found", key)
	}

	rv := reflect.ValueOf(raw)
	if rv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("value is %s, not a slice", rv.Kind())
	}

	result := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		result[i] = rv.Index(i).Interface()
	}
	return result, nil
}
