package gostage

import (
	"fmt"
	"time"
)

// stepKind identifies the type of step in a workflow.
type stepKind int

const (
	stepSingle   stepKind = iota // single task execution
	stepParallel                 // parallel fan-out of multiple tasks
	stepBranch                   // conditional branching
	stepForEach                  // iteration over a collection
	stepMap                      // inline data transformation
	stepDoUntil                  // repeat-until loop
	stepDoWhile                  // while-do loop
	stepSub                      // nested sub-workflow
	stepSleep                    // timed delay
	stepStage                    // named group of sequential steps
)

// step is one unit of execution in a workflow.
type step struct {
	id       string
	kind     stepKind
	name     string
	disabled bool     // dynamically disabled via mutations
	tags     []string // tags for querying and conditional execution

	// stepSingle
	taskName string

	// stepParallel / stepStage
	refs []StepRef

	// stepBranch
	cases []BranchCase

	// stepForEach
	collectionKey string
	forEachRef    StepRef
	concurrency   int
	useSpawn      bool

	// stepMap
	mapFn     func(*Ctx)
	mapFnName string // named variant for serializable workflows

	// stepDoUntil / stepDoWhile
	loopRef      StepRef
	loopCond     func(*Ctx) bool
	loopCondName string // named variant for serializable workflows

	// stepSub
	subWorkflow *Workflow

	// stepSleep
	sleepDuration time.Duration
}

// Workflow is the compiled result of a builder chain.
type Workflow struct {
	ID         string
	Name       string
	Tags       []string
	steps      []step
	state      *runState
	cfg        workflowConfig
	mutations  *mutationQueue // runtime mutation queue (initialized on first execution)
	dynCounter int            // counter for generating dynamic step IDs
}

type workflowConfig struct {
	onStepComplete    StepCallback
	onError           ErrorCallback
	defaultRetries    int
	defaultRetryDelay time.Duration
	stepMiddleware    []StepMiddleware
	tags              []string
}

// StepCallback is called after each step completes successfully.
type StepCallback func(stepName string, ctx *Ctx)

// ErrorCallback is called when a step fails.
type ErrorCallback func(err error)

// --- StepRef: composable step descriptor ---

// StepRef is a step descriptor used in Parallel, ForEach, Branch, etc.
type StepRef struct {
	taskName    string
	subWorkflow *Workflow
}

// Step creates a StepRef pointing to a registered task.
//
//	gostage.Parallel(gostage.Step("task1"), gostage.Step("task2"))
func Step(taskName string) StepRef {
	return StepRef{taskName: taskName}
}

// Sub creates a StepRef pointing to a sub-workflow.
//
//	gostage.ForEach("items", gostage.Sub(otherWf))
func Sub(wf *Workflow) StepRef {
	return StepRef{subWorkflow: wf}
}

// --- Branch helpers ---

// BranchCase represents one arm of a Branch.
type BranchCase struct {
	condition func(*Ctx) bool
	condName  string // named variant for serializable workflows
	ref       StepRef
	isDefault bool
}

// WhenClause builds a conditional branch arm.
type WhenClause struct {
	condition func(*Ctx) bool
	condName  string // named variant for serializable workflows
}

// When starts a conditional branch arm.
//
//	gostage.When(func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[string](ctx, "priority") == "high"
//	}).Step("urgent.process")
func When(cond func(*Ctx) bool) *WhenClause {
	return &WhenClause{condition: cond}
}

// Step completes the WhenClause with a task to execute.
func (w *WhenClause) Step(taskName string) BranchCase {
	return BranchCase{condition: w.condition, condName: w.condName, ref: StepRef{taskName: taskName}}
}

// Sub completes the WhenClause with a sub-workflow to execute.
func (w *WhenClause) Sub(wf *Workflow) BranchCase {
	return BranchCase{condition: w.condition, condName: w.condName, ref: StepRef{subWorkflow: wf}}
}

// DefaultClause builds the default branch arm.
type DefaultClause struct{}

// Default starts the default branch arm (executed when no When matches).
func Default() *DefaultClause { return &DefaultClause{} }

// Step completes the DefaultClause with a task to execute.
func (d *DefaultClause) Step(taskName string) BranchCase {
	return BranchCase{ref: StepRef{taskName: taskName}, isDefault: true}
}

// Sub completes the DefaultClause with a sub-workflow to execute.
func (d *DefaultClause) Sub(wf *Workflow) BranchCase {
	return BranchCase{ref: StepRef{subWorkflow: wf}, isDefault: true}
}

// --- ForEach options ---

// ForEachOption configures a ForEach step.
type ForEachOption func(*step)

// WithConcurrency sets the max concurrent iterations for ForEach.
//
//	gostage.ForEach("tracks", gostage.Step("download"), gostage.WithConcurrency(4))
func WithConcurrency(n int) ForEachOption {
	return func(s *step) {
		s.concurrency = n
	}
}

// WithSpawn makes each ForEach iteration run in an isolated child process.
//
//	gostage.ForEach("tracks", gostage.Step("download"), gostage.WithSpawn())
func WithSpawn() ForEachOption {
	return func(s *step) {
		s.useSpawn = true
	}
}

// --- Workflow options ---

// WorkflowOption configures a workflow.
type WorkflowOption func(*workflowConfig)

// OnStepComplete registers a callback invoked after each step completes.
//
//	gostage.NewWorkflow("monitored", gostage.OnStepComplete(func(step string, ctx *gostage.Ctx) {
//	    log.Printf("Step %s done", step)
//	}))
func OnStepComplete(fn StepCallback) WorkflowOption {
	return func(cfg *workflowConfig) {
		cfg.onStepComplete = fn
	}
}

// OnError registers a callback invoked when a step fails.
//
//	gostage.NewWorkflow("monitored", gostage.OnError(func(err error) {
//	    alerting.Send(err.Error())
//	}))
func OnError(fn ErrorCallback) WorkflowOption {
	return func(cfg *workflowConfig) {
		cfg.onError = fn
	}
}

// WithDefaultRetry sets workflow-wide retry defaults.
// Individual task retries (via WithRetry) take precedence.
//
//	gostage.NewWorkflow("resilient", gostage.WithDefaultRetry(5, time.Second))
func WithDefaultRetry(n int, delay time.Duration) WorkflowOption {
	return func(cfg *workflowConfig) {
		cfg.defaultRetries = n
		cfg.defaultRetryDelay = delay
	}
}

// WithWorkflowTags attaches tags to the workflow for querying.
//
//	gostage.NewWorkflow("order", gostage.WithWorkflowTags("billing", "critical"))
func WithWorkflowTags(tags ...string) WorkflowOption {
	return func(cfg *workflowConfig) {
		cfg.tags = tags
	}
}

// WithWorkflowMiddleware adds step-level middleware scoped to this workflow.
//
//	gostage.NewWorkflow("monitored", gostage.WithWorkflowMiddleware(timingMW))
func WithWorkflowMiddleware(m StepMiddleware) WorkflowOption {
	return func(cfg *workflowConfig) {
		cfg.stepMiddleware = append(cfg.stepMiddleware, m)
	}
}

// --- WorkflowBuilder ---

// StepOption configures an individual step in the builder.
type StepOption func(*builderStep)

// WithStepTags attaches tags to a step for querying and conditional execution.
//
//	wf.Step("charge", gostage.WithStepTags("billing", "critical"))
func WithStepTags(tags ...string) StepOption {
	return func(bs *builderStep) {
		bs.tags = tags
	}
}

// builderStep is a deferred step captured by the builder before Commit.
type builderStep struct {
	kind stepKind
	name string
	tags []string

	// stepSingle
	taskName string

	// stepParallel / stepStage
	refs []StepRef

	// stepBranch
	cases []BranchCase

	// stepForEach
	collectionKey string
	forEachRef    StepRef
	forEachOpts   []ForEachOption

	// stepMap
	mapFn     func(*Ctx)
	mapFnName string // named variant for serializable workflows

	// stepDoUntil / stepDoWhile
	loopRef      StepRef
	loopCond     func(*Ctx) bool
	loopCondName string // named variant for serializable workflows

	// stepSub
	subWorkflow *Workflow

	// stepSleep
	sleepDuration time.Duration
}

// WorkflowBuilder constructs a Workflow using a fluent API.
type WorkflowBuilder struct {
	id    string
	name  string
	cfg   workflowConfig
	steps []builderStep
}

// NewWorkflow starts building a new workflow with the given ID and options.
//
//	wf := gostage.NewWorkflow("process-order").
//	    Step("validate").
//	    Step("charge").
//	    Commit()
func NewWorkflow(id string, opts ...WorkflowOption) *WorkflowBuilder {
	b := &WorkflowBuilder{id: id, name: id}
	for _, opt := range opts {
		opt(&b.cfg)
	}
	return b
}

// Step adds a single task step to the workflow.
//
//	wf.Step("charge", gostage.WithStepTags("billing"))
func (b *WorkflowBuilder) Step(taskName string, opts ...StepOption) *WorkflowBuilder {
	bs := builderStep{
		kind:     stepSingle,
		taskName: taskName,
		name:     taskName,
	}
	for _, opt := range opts {
		opt(&bs)
	}
	b.steps = append(b.steps, bs)
	return b
}

// Stage adds a named group of sequential steps.
//
//	wf.Stage("validation", gostage.Step("validate.input"), gostage.Step("validate.rules"))
func (b *WorkflowBuilder) Stage(name string, refs ...StepRef) *WorkflowBuilder {
	b.steps = append(b.steps, builderStep{
		kind: stepStage,
		name: name,
		refs: refs,
	})
	return b
}

// Parallel adds steps that execute concurrently.
//
//	wf.Parallel(gostage.Step("charge"), gostage.Step("reserve"), gostage.Step("check"))
func (b *WorkflowBuilder) Parallel(refs ...StepRef) *WorkflowBuilder {
	b.steps = append(b.steps, builderStep{
		kind: stepParallel,
		name: "parallel",
		refs: refs,
	})
	return b
}

// Branch adds conditional execution.
//
//	wf.Branch(
//	    gostage.When(isHighPriority).Step("urgent"),
//	    gostage.Default().Step("normal"),
//	)
func (b *WorkflowBuilder) Branch(cases ...BranchCase) *WorkflowBuilder {
	b.steps = append(b.steps, builderStep{
		kind:  stepBranch,
		name:  "branch",
		cases: cases,
	})
	return b
}

// ForEach iterates over a collection stored in the KV store.
//
//	wf.ForEach("tracks", gostage.Step("download"), gostage.WithConcurrency(4))
func (b *WorkflowBuilder) ForEach(key string, ref StepRef, opts ...ForEachOption) *WorkflowBuilder {
	b.steps = append(b.steps, builderStep{
		kind:          stepForEach,
		name:          "forEach:" + key,
		collectionKey: key,
		forEachRef:    ref,
		forEachOpts:   opts,
	})
	return b
}

// DoUntil repeats a step until the condition returns true.
// The step executes first, then the condition is checked (do-until).
//
//	wf.DoUntil(gostage.Step("poll"), func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[string](ctx, "status") == "ready"
//	})
func (b *WorkflowBuilder) DoUntil(ref StepRef, cond func(*Ctx) bool) *WorkflowBuilder {
	b.steps = append(b.steps, builderStep{
		kind:     stepDoUntil,
		name:     "doUntil",
		loopRef:  ref,
		loopCond: cond,
	})
	return b
}

// DoWhile repeats a step while the condition returns true.
// The condition is checked before each iteration (while-do).
//
//	wf.DoWhile(gostage.Step("fetch.page"), func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[bool](ctx, "has_more")
//	})
func (b *WorkflowBuilder) DoWhile(ref StepRef, cond func(*Ctx) bool) *WorkflowBuilder {
	b.steps = append(b.steps, builderStep{
		kind:     stepDoWhile,
		name:     "doWhile",
		loopRef:  ref,
		loopCond: cond,
	})
	return b
}

// Map adds an inline data transformation step.
//
//	wf.Map(func(ctx *gostage.Ctx) {
//	    raw := gostage.Get[[]byte](ctx, "raw")
//	    gostage.Set(ctx, "records", parseCSV(raw))
//	})
func (b *WorkflowBuilder) Map(fn func(*Ctx)) *WorkflowBuilder {
	b.steps = append(b.steps, builderStep{
		kind:  stepMap,
		name:  "map",
		mapFn: fn,
	})
	return b
}

// Sub adds a nested sub-workflow step.
//
//	wf.Sub(otherWf)
func (b *WorkflowBuilder) Sub(wf *Workflow) *WorkflowBuilder {
	b.steps = append(b.steps, builderStep{
		kind:        stepSub,
		name:        "sub:" + wf.ID,
		subWorkflow: wf,
	})
	return b
}

// Sleep adds a timed delay step.
// With persistence, the run is saved as Sleeping and no goroutine blocks.
// Without persistence, time.Sleep is used.
//
//	wf.Sleep(time.Hour)
func (b *WorkflowBuilder) Sleep(d time.Duration) *WorkflowBuilder {
	b.steps = append(b.steps, builderStep{
		kind:          stepSleep,
		name:          "sleep",
		sleepDuration: d,
	})
	return b
}

// --- Named variants for serializable workflows ---

// WhenNamed starts a serializable conditional branch arm using a registered condition.
// The condition must be registered with Condition() before building the workflow.
//
//	gostage.Condition("is-high-priority", func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[string](ctx, "priority") == "high"
//	})
//	wf.Branch(gostage.WhenNamed("is-high-priority").Step("urgent"))
func WhenNamed(condName string) *WhenClause {
	fn := lookupCondition(condName)
	if fn == nil {
		panic(fmt.Sprintf("gostage: condition %q not registered", condName))
	}
	return &WhenClause{condition: fn, condName: condName}
}

// MapNamed adds a serializable data transformation step using a registered map function.
// The function must be registered with MapFn() before building the workflow.
//
//	gostage.MapFn("parse-csv", func(ctx *gostage.Ctx) {
//	    raw := gostage.Get[[]byte](ctx, "raw")
//	    gostage.Set(ctx, "records", parseCSV(raw))
//	})
//	wf.MapNamed("parse-csv")
func (b *WorkflowBuilder) MapNamed(mapFnName string) *WorkflowBuilder {
	fn := lookupMapFn(mapFnName)
	if fn == nil {
		panic(fmt.Sprintf("gostage: map function %q not registered", mapFnName))
	}
	b.steps = append(b.steps, builderStep{
		kind:      stepMap,
		name:      "map",
		mapFn:     fn,
		mapFnName: mapFnName,
	})
	return b
}

// DoUntilNamed repeats a step until a registered condition returns true.
// The condition must be registered with Condition() before building the workflow.
//
//	gostage.Condition("is-ready", func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[string](ctx, "status") == "ready"
//	})
//	wf.DoUntilNamed(gostage.Step("poll"), "is-ready")
func (b *WorkflowBuilder) DoUntilNamed(ref StepRef, condName string) *WorkflowBuilder {
	fn := lookupCondition(condName)
	if fn == nil {
		panic(fmt.Sprintf("gostage: condition %q not registered", condName))
	}
	b.steps = append(b.steps, builderStep{
		kind:         stepDoUntil,
		name:         "doUntil",
		loopRef:      ref,
		loopCond:     fn,
		loopCondName: condName,
	})
	return b
}

// DoWhileNamed repeats a step while a registered condition returns true.
// The condition must be registered with Condition() before building the workflow.
//
//	gostage.Condition("has-more", func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[bool](ctx, "has_more")
//	})
//	wf.DoWhileNamed(gostage.Step("fetch.page"), "has-more")
func (b *WorkflowBuilder) DoWhileNamed(ref StepRef, condName string) *WorkflowBuilder {
	fn := lookupCondition(condName)
	if fn == nil {
		panic(fmt.Sprintf("gostage: condition %q not registered", condName))
	}
	b.steps = append(b.steps, builderStep{
		kind:         stepDoWhile,
		name:         "doWhile",
		loopRef:      ref,
		loopCond:     fn,
		loopCondName: condName,
	})
	return b
}

// Commit finalizes the builder and returns the compiled Workflow.
// Panics if any referenced task is not registered.
func (b *WorkflowBuilder) Commit() *Workflow {
	wf := &Workflow{
		ID:    b.id,
		Name:  b.name,
		Tags:  b.cfg.tags,
		state: newRunState("", nil),
		cfg:   b.cfg,
		steps: make([]step, 0, len(b.steps)),
	}

	for i, bs := range b.steps {
		s := step{
			id:   fmt.Sprintf("%s:%d", b.id, i),
			kind: bs.kind,
			name: bs.name,
			tags: bs.tags,
		}

		switch bs.kind {
		case stepSingle:
			s.taskName = bs.taskName
			validateTaskRef(bs.taskName)

		case stepParallel:
			s.refs = bs.refs
			for _, ref := range bs.refs {
				validateStepRef(ref)
			}

		case stepStage:
			s.refs = bs.refs
			for _, ref := range bs.refs {
				validateStepRef(ref)
			}

		case stepBranch:
			s.cases = bs.cases
			for _, c := range bs.cases {
				validateStepRef(c.ref)
			}

		case stepForEach:
			s.collectionKey = bs.collectionKey
			s.forEachRef = bs.forEachRef
			s.concurrency = 1 // default sequential
			validateStepRef(bs.forEachRef)
			for _, opt := range bs.forEachOpts {
				opt(&s)
			}

		case stepMap:
			s.mapFn = bs.mapFn
			s.mapFnName = bs.mapFnName

		case stepDoUntil, stepDoWhile:
			s.loopRef = bs.loopRef
			s.loopCond = bs.loopCond
			s.loopCondName = bs.loopCondName
			validateStepRef(bs.loopRef)

		case stepSub:
			s.subWorkflow = bs.subWorkflow

		case stepSleep:
			s.sleepDuration = bs.sleepDuration
		}

		wf.steps = append(wf.steps, s)
	}

	return wf
}

// clone creates an independent copy of the workflow for concurrent execution.
// Immutable fields (ID, Name, Tags, cfg) are shared. Mutable fields (store, steps, mutations)
// are deep-copied so concurrent runs don't interfere with each other.
func (wf *Workflow) clone() *Workflow {
	cloned := &Workflow{
		ID:         wf.ID,
		Name:       wf.Name,
		Tags:       wf.Tags,
		state:      wf.state.Clone(),
		cfg:        wf.cfg,
		mutations:  newMutationQueue(),
		dynCounter: 0,
	}
	// Deep copy steps so mutations (disable/enable) don't leak between runs
	cloned.steps = make([]step, len(wf.steps))
	copy(cloned.steps, wf.steps)
	return cloned
}

// validateTaskRef panics if a task name is not registered.
func validateTaskRef(name string) {
	if lookupTask(name) == nil {
		panic(fmt.Sprintf("gostage: task %q not registered", name))
	}
}

// validateStepRef panics if a StepRef references an unregistered task.
func validateStepRef(ref StepRef) {
	if ref.subWorkflow != nil {
		return // sub-workflows are already validated
	}
	validateTaskRef(ref.taskName)
}
