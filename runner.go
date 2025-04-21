package gostage

import (
	"context"
	"fmt"
	"time"

	"github.com/davidroman0O/gostage/store"
)

// Middleware represents a function that wraps workflow execution.
// Middleware can perform actions before and after workflow execution,
// inject data into the workflow store, modify the context, or even
// skip execution entirely.
type Middleware func(next RunnerFunc) RunnerFunc

// RunnerFunc is the core function type for executing a workflow.
type RunnerFunc func(ctx context.Context, workflow *Workflow, logger Logger) error

// Runner executes workflows and manages the execution pipeline.
// It can be composed into other structures and supports middleware
// for adding cross-cutting concerns to workflow execution.
type Runner struct {
	// Middleware chain to apply during workflow execution
	middleware []Middleware
	// defaultLogger used when no logger is provided
	defaultLogger Logger
	// Options for workflow execution
	options RunOptions
}

// RunnerOption is a function that configures a Runner
type RunnerOption func(*Runner)

// WithMiddleware adds middleware to the runner
func WithMiddleware(middleware ...Middleware) RunnerOption {
	return func(r *Runner) {
		r.middleware = append(r.middleware, middleware...)
	}
}

// WithLogger sets the default logger for the runner
func WithLogger(logger Logger) RunnerOption {
	return func(r *Runner) {
		r.defaultLogger = logger
	}
}

// WithOptions sets the default run options for the runner
func WithOptions(options RunOptions) RunnerOption {
	return func(r *Runner) {
		r.options = options
	}
}

// NewRunner creates a new workflow runner with the given options
func NewRunner(opts ...RunnerOption) *Runner {
	runner := &Runner{
		middleware:    []Middleware{},
		defaultLogger: NewDefaultLogger(),
		options:       DefaultRunOptions(),
	}

	for _, opt := range opts {
		opt(runner)
	}

	return runner
}

// Use adds middleware to the runner's middleware chain
func (r *Runner) Use(middleware ...Middleware) {
	r.middleware = append(r.middleware, middleware...)
}

// Execute runs a workflow with the configured middleware chain
func (r *Runner) Execute(ctx context.Context, workflow *Workflow, logger Logger) error {
	if logger == nil {
		logger = r.defaultLogger
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// Build the middleware chain
	var handler RunnerFunc = r.executeWorkflow

	// Apply middleware in reverse order
	for i := len(r.middleware) - 1; i >= 0; i-- {
		handler = r.middleware[i](handler)
	}

	// Execute the workflow with middleware chain
	return handler(ctx, workflow, logger)
}

// executeWorkflow is the core workflow execution logic
func (r *Runner) executeWorkflow(ctx context.Context, w *Workflow, logger Logger) error {
	if len(w.Stages) == 0 {
		return fmt.Errorf("workflow '%s' has no stages to execute", w.ID)
	}

	logger.Info("Starting workflow: %s (%s)", w.Name, w.ID)

	// Update workflow status in store
	workflowKey := PrefixWorkflow + w.ID
	w.Store.SetProperty(workflowKey, PropStatus, StatusRunning)

	// Initialize the disabled stages map if it doesn't exist
	if _, ok := w.Context["disabledStages"]; !ok {
		w.Context["disabledStages"] = make(map[string]bool)
	}

	disabledStages, ok := w.Context["disabledStages"].(map[string]bool)
	if !ok {
		disabledStages = make(map[string]bool)
		w.Context["disabledStages"] = disabledStages
	}

	// We need to execute stages one by one, as dynamic stages can be inserted during execution
	for i := 0; i < len(w.Stages); i++ {
		stage := w.Stages[i]
		stageKey := PrefixStage + stage.ID

		// Update stage status in store
		w.Store.SetProperty(stageKey, PropStatus, StatusRunning)

		// Skip disabled stages
		if disabledStages[stage.ID] {
			logger.Debug("Skipping disabled stage: %s", stage.Name)
			continue
		}

		// Execute the stage
		logger.Debug("Executing stage %d/%d: %s", i+1, len(w.Stages), stage.Name)
		if err := r.executeStage(ctx, stage, w, logger); err != nil {
			w.Store.SetProperty(stageKey, PropStatus, StatusFailed)
			w.Store.SetProperty(workflowKey, PropStatus, StatusFailed)
			return fmt.Errorf("stage '%s' failed: %w", stage.Name, err)
		}

		// Check if any dynamic stages were generated
		if dynamicStages, ok := w.Context["dynamicStages"]; ok {
			if stages, ok := dynamicStages.([]*Stage); ok && len(stages) > 0 {
				logger.Debug("Found %d dynamic stages to insert after stage %s", len(stages), stage.ID)

				// Insert the new stages after the current one
				newStages := make([]*Stage, 0, len(w.Stages)+len(stages))
				newStages = append(newStages, w.Stages[:i+1]...)

				// Add each dynamic stage to the store
				for _, dynStage := range stages {
					// Add dynamic tag to these stages
					if !dynStage.HasTag(TagDynamic) {
						dynStage.AddTag(TagDynamic)
					}

					// Store in KV store
					dynStageKey := PrefixStage + dynStage.ID
					dynStageInfo := dynStage.toStageInfo()

					meta := store.NewMetadata()
					meta.Tags = append(meta.Tags, dynStage.Tags...)
					meta.Description = dynStage.Description
					meta.SetProperty(PropOrder, i+1+len(newStages)-len(w.Stages[:i+1]))
					meta.SetProperty(PropStatus, StatusPending)
					meta.SetProperty(PropCreatedBy, "stage:"+stage.ID)

					w.Store.PutWithMetadata(dynStageKey, dynStageInfo, meta)
				}

				newStages = append(newStages, stages...)
				if i+1 < len(w.Stages) {
					newStages = append(newStages, w.Stages[i+1:]...)
				}
				w.Stages = newStages

				// Remove the dynamic stages from context to avoid re-processing
				delete(w.Context, "dynamicStages")

				// Update workflow in store
				w.saveToStore()
			}
		}

		logger.Info("Completed stage %d/%d: %s", i+1, len(w.Stages), stage.Name)
		w.Store.SetProperty(stageKey, PropStatus, StatusCompleted)
	}

	logger.Info("Workflow completed successfully: %s", w.Name)
	w.Store.SetProperty(workflowKey, PropStatus, StatusCompleted)
	return nil
}

// executeStage runs all actions in a stage sequentially.
// If dynamic actions are generated during execution, they are inserted after
// the current action and executed in the same stage.
// If dynamic stages are generated, they are stored for execution after this stage.
func (r *Runner) executeStage(ctx context.Context, s *Stage, workflow *Workflow, logger Logger) error {
	if len(s.Actions) == 0 {
		logger.Warn("Stage '%s' has no actions to execute", s.ID)
		return nil
	}

	// Copy the stage's initial store data to the workflow's store
	if s.initialStore != nil && workflow.Store != nil {
		logger.Debug("Merging stage's initialStore into workflow store. Stage: %s, Keys in initialStore: %d",
			s.ID, s.initialStore.Count())
		copied, overwritten, err := workflow.Store.CopyFromWithOverwrite(s.initialStore)
		if err != nil {
			logger.Error("Failed to copy stage's initialStore: %v", err)
		} else {
			logger.Debug("Copied %d keys, overwrote %d keys from stage's initialStore", copied, overwritten)
		}
	}

	// Initialize the action context with disabled maps
	actionCtx := &ActionContext{
		GoContext:       ctx,
		Workflow:        workflow,
		Stage:           s,
		Action:          nil,
		Logger:          logger,
		dynamicActions:  []Action{},
		dynamicStages:   []*Stage{},
		disabledActions: make(map[string]bool),
		disabledStages:  make(map[string]bool),
	}

	// Check if the disabled maps exist in workflow context
	if disabled, ok := workflow.Context["disabledActions"]; ok {
		if disabledMap, ok := disabled.(map[string]bool); ok {
			actionCtx.disabledActions = disabledMap
		}
	}

	if disabled, ok := workflow.Context["disabledStages"]; ok {
		if disabledMap, ok := disabled.(map[string]bool); ok {
			actionCtx.disabledStages = disabledMap
		}
	}

	// We need to execute actions one by one, as dynamic actions can be inserted during execution
	for i := 0; i < len(s.Actions); i++ {
		action := s.Actions[i]
		actionKey := PrefixAction + s.ID + ":" + action.Name()

		// Update action status in store
		workflow.Store.SetProperty(actionKey, PropStatus, StatusRunning)

		// Skip disabled actions
		if actionCtx.disabledActions[action.Name()] {
			logger.Debug("Skipping disabled action: %s", action.Name())
			workflow.Store.SetProperty(actionKey, PropStatus, StatusSkipped)
			continue
		}

		logger.Debug("Executing action %d/%d: %s", i+1, len(s.Actions), action.Name())

		// Update the context with the current action
		actionCtx.Action = action

		// Execute the action
		if err := action.Execute(actionCtx); err != nil {
			workflow.Store.SetProperty(actionKey, PropStatus, StatusFailed)
			return fmt.Errorf("action '%s' failed: %w", action.Name(), err)
		}

		// Check if the action generated new actions to be inserted
		if len(actionCtx.dynamicActions) > 0 {
			logger.Debug("Action generated %d new actions", len(actionCtx.dynamicActions))

			// Insert the new actions after the current one
			newActions := make([]Action, 0, len(s.Actions)+len(actionCtx.dynamicActions))
			newActions = append(newActions, s.Actions[:i+1]...)

			// Store each dynamic action in the KV store
			for _, dynAction := range actionCtx.dynamicActions {
				// Create a key for the action
				dynActionKey := PrefixAction + s.ID + ":" + dynAction.Name()

				// Create metadata for the action
				meta := store.NewMetadata()
				for _, tag := range dynAction.Tags() {
					meta.AddTag(tag)
				}
				meta.AddTag(TagDynamic)
				meta.Description = dynAction.Description()
				meta.SetProperty(PropCreatedBy, "action:"+action.Name())
				meta.SetProperty(PropStatus, StatusPending)

				// Store action metadata - since we can't easily serialize the actual action,
				// we just store its metadata and track it through the in-memory struct
				workflow.Store.PutWithMetadata(dynActionKey, dynAction.Description(), meta)
			}

			newActions = append(newActions, actionCtx.dynamicActions...)
			if i+1 < len(s.Actions) {
				newActions = append(newActions, s.Actions[i+1:]...)
			}
			s.Actions = newActions

			// Clear dynamic actions for the next iteration
			actionCtx.dynamicActions = []Action{}
		}

		// Check if the action generated new stages to be inserted
		if len(actionCtx.dynamicStages) > 0 {
			logger.Debug("Action generated %d new stages", len(actionCtx.dynamicStages))

			// Store the stages to be added to the workflow after this stage completes
			workflow.Context["dynamicStages"] = actionCtx.dynamicStages

			// Clear dynamic stages for the next iteration
			actionCtx.dynamicStages = []*Stage{}
		}

		logger.Debug("Completed action %d/%d: %s", i+1, len(s.Actions), action.Name())
		workflow.Store.SetProperty(actionKey, PropStatus, StatusCompleted)
	}

	// Store the updated disabled maps back in the workflow context
	workflow.Context["disabledActions"] = actionCtx.disabledActions
	workflow.Context["disabledStages"] = actionCtx.disabledStages

	return nil
}

// RunResult contains the result of a workflow execution
type RunResult struct {
	WorkflowID    string
	Success       bool
	Error         error
	ExecutionTime time.Duration
}

// RunOptions contains options for workflow execution
type RunOptions struct {
	// Logger to use for the workflow execution
	Logger Logger

	// Context to use for the workflow execution
	Context context.Context

	// Whether to ignore workflow errors and continue execution
	IgnoreErrors bool
}

// DefaultRunOptions returns the default options for running a workflow
func DefaultRunOptions() RunOptions {
	return RunOptions{
		Logger:       NewDefaultLogger(),
		Context:      context.Background(),
		IgnoreErrors: false,
	}
}

// ExecuteWithOptions runs a workflow with the given options
func (r *Runner) ExecuteWithOptions(workflow *Workflow, options RunOptions) RunResult {
	startTime := time.Now()

	// Use options from the runner if not provided
	logger := options.Logger
	if logger == nil {
		logger = r.defaultLogger
	}

	// Use options context if provided
	ctx := options.Context
	if ctx == nil {
		ctx = context.Background()
	}

	// Execute the workflow
	err := r.Execute(ctx, workflow, logger)

	// Create result
	result := RunResult{
		WorkflowID:    workflow.ID,
		Success:       err == nil,
		Error:         err,
		ExecutionTime: time.Since(startTime),
	}

	return result
}

// RunWorkflow executes a workflow with the provided options
// This is a convenience function for backward compatibility
func RunWorkflow(workflow *Workflow, options RunOptions) RunResult {
	runner := NewRunner()
	return runner.ExecuteWithOptions(workflow, options)
}

// ExecuteWorkflows runs multiple workflows in sequence
func (r *Runner) ExecuteWorkflows(workflows []*Workflow, options RunOptions) []RunResult {
	results := make([]RunResult, 0, len(workflows))

	for i, wf := range workflows {
		// Run the current workflow
		result := r.ExecuteWithOptions(wf, options)
		results = append(results, result)

		// Stop after executing a failing workflow if we're not ignoring errors
		if !result.Success && !options.IgnoreErrors && i < len(workflows)-1 {
			break
		}
	}

	return results
}

// RunWorkflows executes multiple workflows in sequence
// This is a convenience function for backward compatibility
func RunWorkflows(workflows []*Workflow, options RunOptions) []RunResult {
	runner := NewRunner()
	return runner.ExecuteWorkflows(workflows, options)
}

// FormatResults returns a human-readable summary of the workflow execution results
func FormatResults(results []RunResult) string {
	if len(results) == 0 {
		return "No workflows executed"
	}

	var summary string
	successCount := 0

	for i, result := range results {
		status := "FAILED"
		if result.Success {
			status = "SUCCESS"
			successCount++
		}

		summary += fmt.Sprintf("Workflow %d: %s - %s (%s)\n",
			i+1,
			result.WorkflowID,
			status,
			result.ExecutionTime.Round(time.Millisecond),
		)

		if result.Error != nil {
			summary += fmt.Sprintf("  Error: %v\n", result.Error)
		}
	}

	summary += fmt.Sprintf("\nSummary: %d/%d workflows succeeded\n",
		successCount,
		len(results),
	)

	return summary
}

// Some example middleware functions

// LoggingMiddleware creates a middleware that logs workflow execution steps
func LoggingMiddleware() Middleware {
	return func(next RunnerFunc) RunnerFunc {
		return func(ctx context.Context, workflow *Workflow, logger Logger) error {
			logger.Info("Middleware: Starting workflow %s", workflow.ID)

			start := time.Now()
			err := next(ctx, workflow, logger)
			duration := time.Since(start)

			if err != nil {
				logger.Error("Middleware: Workflow %s failed after %v: %v",
					workflow.ID, duration.Round(time.Millisecond), err)
			} else {
				logger.Info("Middleware: Workflow %s completed in %v",
					workflow.ID, duration.Round(time.Millisecond))
			}

			return err
		}
	}
}

// StoreInjectionMiddleware creates a middleware that injects values into the workflow store
func StoreInjectionMiddleware(keyValues map[string]interface{}) Middleware {
	return func(next RunnerFunc) RunnerFunc {
		return func(ctx context.Context, workflow *Workflow, logger Logger) error {
			// Inject values into the store
			for key, value := range keyValues {
				workflow.Store.Put(key, value)
			}

			// Continue execution
			return next(ctx, workflow, logger)
		}
	}
}

// TimeLimitMiddleware creates a middleware that enforces a time limit on workflow execution
func TimeLimitMiddleware(limit time.Duration) Middleware {
	return func(next RunnerFunc) RunnerFunc {
		return func(ctx context.Context, workflow *Workflow, logger Logger) error {
			// Create a context with timeout
			ctx, cancel := context.WithTimeout(ctx, limit)
			defer cancel()

			// Execute with the timeout context
			return next(ctx, workflow, logger)
		}
	}
}
