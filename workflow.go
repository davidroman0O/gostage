package gostage

import (
	"fmt"
	"time"
)

// StepKind identifies the structural type of a step in a workflow. Each kind
// determines how the engine executes the step and which fields of the
// internal step struct are populated.
type StepKind int

const (
	StepSingle   StepKind = iota // A single registered task execution.
	StepParallel                 // Concurrent fan-out; all refs execute simultaneously.
	StepBranch                   // Conditional routing; first matching When case executes.
	StepForEach                  // Collection iteration with optional concurrency and spawn.
	StepMap                      // Inline data transformation via a user-provided function.
	StepDoUntil                  // Post-condition loop: body executes, then condition is checked.
	StepDoWhile                  // Pre-condition loop: condition is checked, then body executes.
	StepSub                      // Nested sub-workflow sharing the parent's state.
	StepSleep                    // Timed delay; persistent or in-memory depending on engine config.
	StepStage                    // Named group of sequential steps for organizational clarity.
)

// step is one unit of execution in a workflow.
type step struct {
	id       string
	kind     StepKind
	name     string
	disabled bool     // dynamically disabled via mutations
	tags     []string // tags for querying and conditional execution

	// StepSingle
	taskName string

	// StepParallel / StepStage
	refs []StepRef

	// StepBranch
	cases []BranchCase

	// StepForEach
	collectionKey string
	forEachRef    StepRef
	concurrency   int
	useSpawn      bool

	// StepMap
	mapFn     func(*Ctx) error
	mapFnName string // named variant for serializable workflows

	// StepDoUntil / StepDoWhile
	loopRef      StepRef
	loopCond     func(*Ctx) bool
	loopCondName string // named variant for serializable workflows

	// StepSub
	subWorkflow *Workflow

	// StepSleep
	sleepDuration time.Duration
}

// info returns a public StepInfo snapshot for middleware consumption.
func (s *step) info() StepInfo {
	return StepInfo{
		ID:       s.id,
		Name:     s.name,
		Kind:     s.kind,
		Tags:     s.tags,
		Disabled: s.disabled,
		TaskName: s.taskName,
	}
}

// Workflow is the compiled, immutable definition produced by
// [WorkflowBuilder.Commit]. It describes the sequence of steps to execute
// but does not hold per-run state. The engine clones the workflow for each
// execution so that concurrent runs do not interfere with each other.
//
// A Workflow should be created once and reused across multiple [Engine.RunSync]
// or [Engine.Run] calls. Modifying a Workflow after Commit is not supported
// and may cause undefined behavior.
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
	onStepComplete       StepCallback
	onError              ErrorCallback
	defaultRetries       int
	defaultRetryDelay    time.Duration
	defaultRetryStrategy RetryStrategy
	stepMiddleware       []StepMiddleware
	tags                 []string
}

// StepCallback is invoked after each step completes successfully, receiving
// the step name and the execution context. Register it via [OnStepComplete]
// as a workflow option.
type StepCallback func(stepName string, ctx *Ctx)

// ErrorCallback is invoked when a step fails (after all retries are
// exhausted), receiving the final error. Register it via [OnError] as a
// workflow option.
type ErrorCallback func(err error)

// --- StepRef: composable step descriptor ---

// StepRef is a composable step descriptor that points to either a registered
// task (by name) or a committed sub-workflow. StepRef values are used as
// arguments to [WorkflowBuilder.Parallel], [WorkflowBuilder.ForEach],
// [WorkflowBuilder.Stage], [WorkflowBuilder.DoUntil], [WorkflowBuilder.DoWhile],
// and the branch clause completers ([WhenClause.Step], [WhenClause.Sub],
// [DefaultClause.Step], [DefaultClause.Sub]).
type StepRef struct {
	taskName    string
	subWorkflow *Workflow
}

// Step creates a [StepRef] that references a task registered in the task
// registry by name. The task does not need to be registered at workflow
// construction time; it is resolved at execution time.
//
//	gostage.Parallel(gostage.Step("task1"), gostage.Step("task2"))
func Step(taskName string) StepRef {
	return StepRef{taskName: taskName}
}

// Sub creates a [StepRef] that references a committed sub-workflow. The
// sub-workflow shares the parent's key-value store during execution.
//
//	gostage.ForEach("items", gostage.Sub(otherWf))
func Sub(wf *Workflow) StepRef {
	return StepRef{subWorkflow: wf}
}

// --- Branch helpers ---

// BranchCase represents one arm of a [WorkflowBuilder.Branch] step. Each case
// pairs a condition (or default marker) with a [StepRef] to execute when the
// condition is met. Create BranchCase values using [When], [WhenNamed], or
// [Default], followed by .Step() or .Sub().
type BranchCase struct {
	condition func(*Ctx) bool
	condName  string // named variant for serializable workflows
	ref       StepRef
	isDefault bool
}

// WhenClause is an intermediate builder for constructing a conditional
// [BranchCase]. Complete it by calling .Step(taskName) or .Sub(workflow).
type WhenClause struct {
	condition func(*Ctx) bool
	condName  string // named variant for serializable workflows
}

// When starts a conditional branch arm that evaluates cond at runtime. The
// first [BranchCase] whose condition returns true is executed; remaining cases
// are skipped. If no When condition matches and no [Default] case is provided,
// the Branch step is a no-op.
//
// For workflows that need to survive suspend/resume (persistence), use
// [WhenNamed] with a registered condition instead.
//
//	gostage.When(func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[string](ctx, "priority") == "high"
//	}).Step("urgent.process")
func When(cond func(*Ctx) bool) *WhenClause {
	return &WhenClause{condition: cond}
}

// Step completes the WhenClause by specifying the registered task to execute
// when the condition is met.
func (w *WhenClause) Step(taskName string) BranchCase {
	return BranchCase{condition: w.condition, condName: w.condName, ref: StepRef{taskName: taskName}}
}

// Sub completes the WhenClause by specifying the sub-workflow to execute
// when the condition is met.
func (w *WhenClause) Sub(wf *Workflow) BranchCase {
	return BranchCase{condition: w.condition, condName: w.condName, ref: StepRef{subWorkflow: wf}}
}

// DefaultClause is an intermediate builder for constructing the fallback
// [BranchCase] that executes when no [When] or [WhenNamed] condition matches.
type DefaultClause struct{}

// Default starts the default branch arm. It is executed only when no [When]
// or [WhenNamed] condition returns true. A Branch may have at most one
// Default case.
func Default() *DefaultClause { return &DefaultClause{} }

// Step completes the DefaultClause by specifying the registered task to
// execute as the fallback.
func (d *DefaultClause) Step(taskName string) BranchCase {
	return BranchCase{ref: StepRef{taskName: taskName}, isDefault: true}
}

// Sub completes the DefaultClause by specifying the sub-workflow to execute
// as the fallback.
func (d *DefaultClause) Sub(wf *Workflow) BranchCase {
	return BranchCase{ref: StepRef{subWorkflow: wf}, isDefault: true}
}

// --- ForEach options ---

// ForEachOption configures the behavior of a [WorkflowBuilder.ForEach] step.
type ForEachOption func(*step)

// WithConcurrency sets the maximum number of concurrent iterations for a
// ForEach step. The default is 1 (sequential). Each concurrent iteration
// shares the same workflow state, so tasks must write to unique keys to
// avoid data races.
//
//	wf.ForEach("tracks", gostage.Step("download"), gostage.WithConcurrency(4))
func WithConcurrency(n int) ForEachOption {
	return func(s *step) {
		s.concurrency = n
	}
}

// WithSpawn makes each ForEach iteration run in an isolated child process
// instead of a goroutine. Child processes receive a snapshot of the parent's
// state and return their dirty writes on completion. This provides process-level
// isolation at the cost of IPC overhead. Requires a [SpawnRunner] to be
// configured on the engine.
//
//	wf.ForEach("tracks", gostage.Step("download"), gostage.WithSpawn())
func WithSpawn() ForEachOption {
	return func(s *step) {
		s.useSpawn = true
	}
}

// --- Workflow options ---

// WorkflowOption configures a [Workflow] at construction time. Pass options
// to [NewWorkflow] to customize callbacks, retry defaults, middleware, and tags.
type WorkflowOption func(*workflowConfig)

// OnStepComplete registers a callback invoked after each step completes
// successfully. The callback receives the step name and the execution context,
// allowing inspection of the store state at that point. Only one callback can
// be registered; subsequent calls overwrite the previous one.
//
//	gostage.NewWorkflow("monitored", gostage.OnStepComplete(func(step string, ctx *gostage.Ctx) {
//	    log.Printf("Step %s done", step)
//	}))
func OnStepComplete(fn StepCallback) WorkflowOption {
	return func(cfg *workflowConfig) {
		cfg.onStepComplete = fn
	}
}

// OnError registers a callback invoked when a step fails after exhausting
// all retries. The callback receives the final error. Only one callback can
// be registered; subsequent calls overwrite the previous one.
//
//	gostage.NewWorkflow("monitored", gostage.OnError(func(err error) {
//	    alerting.Send(err.Error())
//	}))
func OnError(fn ErrorCallback) WorkflowOption {
	return func(cfg *workflowConfig) {
		cfg.onError = fn
	}
}

// WithDefaultRetry sets workflow-wide retry defaults applied to all tasks
// that do not have their own retry configuration (set via WithRetry on the
// task). The n parameter is the maximum number of retry attempts, and delay
// is the fixed delay between attempts unless overridden by a
// [RetryStrategy] via [WithDefaultRetryStrategy].
//
//	gostage.NewWorkflow("resilient", gostage.WithDefaultRetry(5, time.Second))
func WithDefaultRetry(n int, delay time.Duration) WorkflowOption {
	return func(cfg *workflowConfig) {
		cfg.defaultRetries = n
		cfg.defaultRetryDelay = delay
	}
}

// WithDefaultRetryStrategy sets a workflow-wide default [RetryStrategy]
// applied to all tasks that do not have their own strategy (set via
// WithRetryStrategy on the task). This overrides the fixed delay set by
// [WithDefaultRetry] but not per-task strategies.
//
//	gostage.NewWorkflow("resilient",
//	    gostage.WithDefaultRetry(5, 0),
//	    gostage.WithDefaultRetryStrategy(gostage.ExponentialBackoffWithJitter(100*time.Millisecond, 30*time.Second)),
//	)
func WithDefaultRetryStrategy(s RetryStrategy) WorkflowOption {
	return func(cfg *workflowConfig) {
		cfg.defaultRetryStrategy = s
	}
}

// WithWorkflowTags attaches metadata tags to the workflow. Tags can be used
// for organizational purposes and are stored in the [Workflow.Tags] field.
//
//	gostage.NewWorkflow("order", gostage.WithWorkflowTags("billing", "critical"))
func WithWorkflowTags(tags ...string) WorkflowOption {
	return func(cfg *workflowConfig) {
		cfg.tags = tags
	}
}

// WithWorkflowMiddleware adds step-level middleware scoped to this workflow.
// Workflow middleware runs in addition to any engine-level step middleware
// and is applied to every step within the workflow. Multiple calls append
// additional middleware to the chain.
//
//	gostage.NewWorkflow("monitored", gostage.WithWorkflowMiddleware(timingMW))
func WithWorkflowMiddleware(m StepMiddleware) WorkflowOption {
	return func(cfg *workflowConfig) {
		cfg.stepMiddleware = append(cfg.stepMiddleware, m)
	}
}

// --- WorkflowBuilder ---

// StepOption configures an individual step added via [WorkflowBuilder.Step].
type StepOption func(*builderStep)

// WithStepTags attaches metadata tags to a step. Tags enable runtime queries
// via [FindStepsByTag] and bulk mutations via [DisableByTag] and [EnableByTag].
//
//	wf.Step("charge", gostage.WithStepTags("billing", "critical"))
func WithStepTags(tags ...string) StepOption {
	return func(bs *builderStep) {
		bs.tags = tags
	}
}

// builderStep is a deferred step captured by the builder before Commit.
type builderStep struct {
	kind StepKind
	name string
	tags []string

	// StepSingle
	taskName string

	// StepParallel / StepStage
	refs []StepRef

	// StepBranch
	cases []BranchCase

	// StepForEach
	collectionKey string
	forEachRef    StepRef
	forEachOpts   []ForEachOption

	// StepMap
	mapFn     func(*Ctx) error
	mapFnName string // named variant for serializable workflows

	// StepDoUntil / StepDoWhile
	loopRef      StepRef
	loopCond     func(*Ctx) bool
	loopCondName string // named variant for serializable workflows

	// StepSub
	subWorkflow *Workflow

	// StepSleep
	sleepDuration time.Duration
}

// WorkflowBuilder constructs a [Workflow] using a fluent (method-chaining) API.
// Create one with [NewWorkflow], add steps via the builder methods (Step,
// Parallel, Branch, ForEach, etc.), and finalize with [WorkflowBuilder.Commit].
// After Commit, the builder is frozen and further mutations panic.
type WorkflowBuilder struct {
	id        string
	name      string
	cfg       workflowConfig
	steps     []builderStep
	committed bool
}

// NewWorkflow starts building a new workflow with the given ID and optional
// configuration. The ID is used as both the workflow identifier and its
// display name. It must not be empty; passing an empty string causes a panic.
//
//	wf, err := gostage.NewWorkflow("process-order").
//	    Step("validate").
//	    Step("charge").
//	    Commit()
func NewWorkflow(id string, opts ...WorkflowOption) *WorkflowBuilder {
	if id == "" {
		panic("gostage.NewWorkflow: " + ErrEmptyName.Error())
	}
	b := &WorkflowBuilder{id: id, name: id}
	for _, opt := range opts {
		opt(&b.cfg)
	}
	return b
}

// Step adds a single task step that executes the registered task identified
// by taskName. The task is resolved from the registry at execution time, not
// at build time. Panics if called after [WorkflowBuilder.Commit].
//
//	wf.Step("charge", gostage.WithStepTags("billing"))
func (b *WorkflowBuilder) Step(taskName string, opts ...StepOption) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	bs := builderStep{
		kind:     StepSingle,
		taskName: taskName,
		name:     taskName,
	}
	for _, opt := range opts {
		opt(&bs)
	}
	b.steps = append(b.steps, bs)
	return b
}

// Stage adds a named group of sequential steps. Stages provide organizational
// structure; the contained steps execute in order, one after another. Unlike
// [WorkflowBuilder.Parallel], there is no concurrency. Panics if called after
// [WorkflowBuilder.Commit].
//
//	wf.Stage("validation", gostage.Step("validate.input"), gostage.Step("validate.rules"))
func (b *WorkflowBuilder) Stage(name string, refs ...StepRef) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind: StepStage,
		name: name,
		refs: refs,
	})
	return b
}

// Parallel adds steps that execute concurrently in separate goroutines. All
// steps must complete before the workflow proceeds to the next step. If any
// parallel step fails, the overall Parallel step fails. Since parallel steps
// share the same store, each step should write to unique keys to avoid data
// races. Panics if called after [WorkflowBuilder.Commit].
//
//	wf.Parallel(gostage.Step("charge"), gostage.Step("reserve"), gostage.Step("check"))
func (b *WorkflowBuilder) Parallel(refs ...StepRef) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind: StepParallel,
		name: "parallel",
		refs: refs,
	})
	return b
}

// Branch adds conditional execution. Cases are evaluated in order; the first
// [BranchCase] whose condition returns true is executed and remaining cases
// are skipped. If no condition matches and a [Default] case is provided, the
// default executes. If no condition matches and no default exists, the Branch
// step is a no-op. Panics if called after [WorkflowBuilder.Commit].
//
//	wf.Branch(
//	    gostage.When(isHighPriority).Step("urgent"),
//	    gostage.Default().Step("normal"),
//	)
func (b *WorkflowBuilder) Branch(cases ...BranchCase) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind:  StepBranch,
		name:  "branch",
		cases: cases,
	})
	return b
}

// ForEach iterates over a collection stored in the workflow's key-value store
// under the given key. The collection must be a slice (e.g. []string, []any).
// For each element, the referenced task or sub-workflow executes with the
// current item accessible via [Item] and the index via [ItemIndex].
//
// By default, iterations run sequentially (concurrency 1). Use [WithConcurrency]
// to allow multiple iterations to run simultaneously, and [WithSpawn] to run
// each iteration in an isolated child process. Panics if called after
// [WorkflowBuilder.Commit].
//
//	wf.ForEach("tracks", gostage.Step("download"), gostage.WithConcurrency(4))
func (b *WorkflowBuilder) ForEach(key string, ref StepRef, opts ...ForEachOption) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind:          StepForEach,
		name:          "forEach:" + key,
		collectionKey: key,
		forEachRef:    ref,
		forEachOpts:   opts,
	})
	return b
}

// DoUntil repeats a step until the condition returns true. The body executes
// first, then the condition is evaluated (post-condition loop). This
// guarantees at least one execution. For workflows that need to survive
// suspend/resume, use [WorkflowBuilder.DoUntilNamed] with a registered
// condition. Panics if called after [WorkflowBuilder.Commit].
//
//	wf.DoUntil(gostage.Step("poll"), func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[string](ctx, "status") == "ready"
//	})
func (b *WorkflowBuilder) DoUntil(ref StepRef, cond func(*Ctx) bool) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind:     StepDoUntil,
		name:     "doUntil",
		loopRef:  ref,
		loopCond: cond,
	})
	return b
}

// DoWhile repeats a step while the condition returns true. The condition is
// evaluated before each iteration (pre-condition loop). If the condition is
// false on the first check, the body never executes. For workflows that need
// to survive suspend/resume, use [WorkflowBuilder.DoWhileNamed] with a
// registered condition. Panics if called after [WorkflowBuilder.Commit].
//
//	wf.DoWhile(gostage.Step("fetch.page"), func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[bool](ctx, "has_more")
//	})
func (b *WorkflowBuilder) DoWhile(ref StepRef, cond func(*Ctx) bool) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind:     StepDoWhile,
		name:     "doWhile",
		loopRef:  ref,
		loopCond: cond,
	})
	return b
}

// Map adds an inline data transformation step. The function receives the
// execution context and can read/write the store to transform data between
// steps. Unlike task steps, Map functions are not registered by name and run
// in-process. For workflows that need to survive suspend/resume, use
// [WorkflowBuilder.MapNamed] with a registered function. Panics if called
// after [WorkflowBuilder.Commit].
//
//	wf.Map(func(ctx *gostage.Ctx) error {
//	    raw := gostage.Get[[]byte](ctx, "raw")
//	    gostage.Set(ctx, "records", parseCSV(raw))
//	    return nil
//	})
func (b *WorkflowBuilder) Map(fn func(*Ctx) error) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind:  StepMap,
		name:  "map",
		mapFn: fn,
	})
	return b
}

// Sub adds a nested sub-workflow step. The sub-workflow's steps execute
// sequentially within the parent workflow, sharing the same key-value store.
// The sub-workflow must have been previously finalized with [WorkflowBuilder.Commit].
// Panics if called after [WorkflowBuilder.Commit].
//
//	wf.Sub(paymentWf)
func (b *WorkflowBuilder) Sub(wf *Workflow) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind:        StepSub,
		name:        "sub:" + wf.ID,
		subWorkflow: wf,
	})
	return b
}

// Sleep adds a timed delay step. The behavior depends on the engine's
// persistence configuration:
//
//   - With persistence: the run is saved with status [Sleeping] and no
//     goroutine blocks. The engine's timer scheduler wakes the workflow when
//     the duration elapses, even across process restarts.
//   - Without persistence (in-memory only): time.Sleep is called directly,
//     blocking the executing goroutine.
//
// Panics if called after [WorkflowBuilder.Commit].
//
//	wf.Sleep(time.Hour)
func (b *WorkflowBuilder) Sleep(d time.Duration) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind:          StepSleep,
		name:          "sleep",
		sleepDuration: d,
	})
	return b
}

// --- Named variants for serializable workflows ---

// WhenNamed starts a serializable conditional branch arm using a condition
// registered via [Condition]. Unlike [When], the condition is referenced by
// name rather than by closure, making the workflow definition serializable
// for persistence and resume. The condition must be registered before the
// workflow executes; it does not need to exist at build time.
//
//	gostage.Condition("is-high-priority", func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[string](ctx, "priority") == "high"
//	})
//	wf.Branch(gostage.WhenNamed("is-high-priority").Step("urgent"))
func WhenNamed(condName string) *WhenClause {
	return &WhenClause{condName: condName}
}

// MapNamed adds a serializable data transformation step using a function
// registered via [MapFn]. Unlike [WorkflowBuilder.Map], the function is
// referenced by name rather than by closure, making the workflow definition
// serializable for persistence and resume. The function must be registered
// before the workflow executes; it does not need to exist at build time.
// Panics if called after [WorkflowBuilder.Commit].
//
//	gostage.MapFn("parse-csv", func(ctx *gostage.Ctx) error {
//	    raw := gostage.Get[[]byte](ctx, "raw")
//	    gostage.Set(ctx, "records", parseCSV(raw))
//	    return nil
//	})
//	wf.MapNamed("parse-csv")
func (b *WorkflowBuilder) MapNamed(mapFnName string) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind:      StepMap,
		name:      "map",
		mapFnName: mapFnName,
	})
	return b
}

// DoUntilNamed repeats a step until a condition registered via [Condition]
// returns true. Like [WorkflowBuilder.DoUntil], this is a post-condition loop
// that guarantees at least one execution. Unlike DoUntil, the condition is
// referenced by name, making the workflow serializable for persistence and
// resume. Panics if called after [WorkflowBuilder.Commit].
//
//	gostage.Condition("is-ready", func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[string](ctx, "status") == "ready"
//	})
//	wf.DoUntilNamed(gostage.Step("poll"), "is-ready")
func (b *WorkflowBuilder) DoUntilNamed(ref StepRef, condName string) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind:         StepDoUntil,
		name:         "doUntil",
		loopRef:      ref,
		loopCondName: condName,
	})
	return b
}

// DoWhileNamed repeats a step while a condition registered via [Condition]
// returns true. Like [WorkflowBuilder.DoWhile], this is a pre-condition loop
// that may execute zero times if the condition is initially false. Unlike
// DoWhile, the condition is referenced by name, making the workflow
// serializable for persistence and resume. Panics if called after
// [WorkflowBuilder.Commit].
//
//	gostage.Condition("has-more", func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[bool](ctx, "has_more")
//	})
//	wf.DoWhileNamed(gostage.Step("fetch.page"), "has-more")
func (b *WorkflowBuilder) DoWhileNamed(ref StepRef, condName string) *WorkflowBuilder {
	if b.committed {
		panic("gostage: cannot modify workflow after Commit()")
	}
	b.steps = append(b.steps, builderStep{
		kind:         StepDoWhile,
		name:         "doWhile",
		loopRef:      ref,
		loopCondName: condName,
	})
	return b
}

// Commit finalizes the builder and returns the compiled [Workflow]. After
// Commit, the builder is frozen; any subsequent calls to builder methods
// panic. Commit performs structural validation only -- it checks for nil
// functions on Map steps without a named alternative, nil conditions on loop
// steps without a named alternative, and nil sub-workflows on Sub steps. It
// does not verify that referenced task names or condition names are registered
// in the registry; those are resolved at execution time.
func (b *WorkflowBuilder) Commit() (*Workflow, error) {
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
		case StepSingle:
			s.taskName = bs.taskName

		case StepParallel:
			s.refs = bs.refs

		case StepStage:
			s.refs = bs.refs

		case StepBranch:
			s.cases = bs.cases

		case StepForEach:
			s.collectionKey = bs.collectionKey
			s.forEachRef = bs.forEachRef
			s.concurrency = 1 // default sequential
			for _, opt := range bs.forEachOpts {
				opt(&s)
			}

		case StepMap:
			if bs.mapFn == nil && bs.mapFnName == "" {
				return nil, fmt.Errorf("gostage: Map step has nil function — use Map(fn) or MapNamed(name)")
			}
			s.mapFn = bs.mapFn
			s.mapFnName = bs.mapFnName

		case StepDoUntil, StepDoWhile:
			if bs.loopCond == nil && bs.loopCondName == "" {
				return nil, fmt.Errorf("gostage: loop step has nil condition — use DoUntil/DoWhile with a condition or DoUntilNamed/DoWhileNamed")
			}
			s.loopRef = bs.loopRef
			s.loopCond = bs.loopCond
			s.loopCondName = bs.loopCondName

		case StepSub:
			if bs.subWorkflow == nil {
				return nil, fmt.Errorf("gostage: Sub step has nil workflow")
			}
			s.subWorkflow = bs.subWorkflow

		case StepSleep:
			s.sleepDuration = bs.sleepDuration
		}

		wf.steps = append(wf.steps, s)
	}

	b.committed = true
	return wf, nil
}

// clone creates an independent copy of the workflow for concurrent execution.
// Immutable fields (ID, Name, Tags, cfg) are shared. Mutable fields (store, steps, mutations)
// are deep-copied so concurrent runs don't interfere with each other.
func (wf *Workflow) clone() *Workflow {
	cloned := &Workflow{
		ID:         wf.ID,
		Name:       wf.Name,
		Tags:       append([]string(nil), wf.Tags...),
		state:      wf.state.Clone(),
		cfg:        wf.cfg,
		mutations:  newMutationQueue(),
		dynCounter: 0,
	}
	// Deep copy steps so mutations (disable/enable) don't leak between runs
	cloned.steps = make([]step, len(wf.steps))
	copy(cloned.steps, wf.steps)
	// Deep copy sub-workflow pointers so concurrent runs with Sub steps
	// do not share mutable step state when dynamic mutations are active.
	for i := range cloned.steps {
		if cloned.steps[i].subWorkflow != nil {
			cloned.steps[i].subWorkflow = cloned.steps[i].subWorkflow.clone()
		}
	}
	return cloned
}

