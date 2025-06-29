package gostage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
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

// Runner coordinates workflow execution and provides middleware support
type Runner struct {
	// Middleware chain to apply during workflow execution
	middleware []Middleware
	// defaultLogger used when no logger is provided
	defaultLogger Logger
	// Options for workflow execution
	options RunOptions
	// Broker handles IPC via gRPC
	Broker *RunnerBroker
	// Spawn middleware for process lifecycle and communication
	spawnMiddleware []SpawnMiddleware
}

// ChildRunner embeds Runner and adds child-specific methods for spawned processes
type ChildRunner struct {
	*Runner // Inherits all Runner methods (Execute, etc.)
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

// WithGRPCTransport configures the runner to use gRPC transport
// If no address/port provided, uses localhost with a random available port
func WithGRPCTransport(addressAndPort ...interface{}) RunnerOption {
	return func(r *Runner) {
		address := "localhost"
		port := 0 // Let the system pick an available port

		// Parse optional address and port parameters
		if len(addressAndPort) >= 1 {
			if addr, ok := addressAndPort[0].(string); ok {
				address = addr
			}
		}
		if len(addressAndPort) >= 2 {
			if p, ok := addressAndPort[1].(int); ok {
				port = p
			}
		}

		// Create gRPC transport - will pick random port if port is 0
		grpcTransport, err := NewGRPCTransport(address, port)
		if err != nil {
			// For now, fallback but log the issue
			r.defaultLogger.Error("Failed to create gRPC transport: %v", err)
			panic(fmt.Sprintf("Failed to create gRPC transport: %v", err))
		}

		r.Broker = NewRunnerBrokerWithTransport(grpcTransport)
	}
}

// NewRunner creates a new Runner with the given options
// Default is gRPC transport with automatic port assignment
func NewRunner(opts ...RunnerOption) *Runner {
	r := &Runner{
		middleware:      make([]Middleware, 0),
		spawnMiddleware: make([]SpawnMiddleware, 0),
		defaultLogger:   NewDefaultLogger(),
		options:         DefaultRunOptions(),
	}

	// Default gRPC transport with automatic port selection
	grpcTransport, err := NewGRPCTransport("localhost", 0) // 0 = auto-assign port
	if err != nil {
		// Fallback should not happen, but just in case
		r.defaultLogger.Error("Failed to create default gRPC transport: %v", err)
		panic(fmt.Sprintf("Failed to create default gRPC transport: %v", err))
	}

	r.Broker = NewRunnerBrokerWithTransport(grpcTransport)

	// Apply options
	for _, opt := range opts {
		opt(r)
	}

	return r
}

// DEPRECATED: NewRunnerWithBroker creates a new Runner with the specified broker.
// This is kept for backwards compatibility but is deprecated.
// Use NewRunner with transport options instead.
func NewRunnerWithBroker(broker *RunnerBroker, opts ...RunnerOption) *Runner {
	// Create the runner with the broker option first
	allOpts := append([]RunnerOption{WithBroker(broker)}, opts...)
	return NewRunner(allOpts...)
}

// DEPRECATED: WithBroker sets the broker for the runner
// This is kept for backwards compatibility but is deprecated.
// Use WithGRPCTransport() instead.
func WithBroker(broker *RunnerBroker) RunnerOption {
	return func(r *Runner) {
		r.Broker = broker
	}
}

// SpawnWorkflow spawns a child process to execute the given workflow definition
func (r *Runner) SpawnWorkflow(ctx context.Context, def SubWorkflowDef) error {
	// Use the existing Spawn method which handles the rest
	return r.Spawn(ctx, def)
}

// SpawnWorkflowWithStore spawns a child process with initial store data
func (r *Runner) SpawnWorkflowWithStore(ctx context.Context, def SubWorkflowDef, initialStore map[string]interface{}) SpawnResult {
	// Use the existing SpawnWithStore method
	return r.SpawnWithStore(ctx, def, initialStore)
}

// Use adds middleware to the runner's middleware chain
func (r *Runner) Use(middleware ...Middleware) {
	r.middleware = append(r.middleware, middleware...)
}

// Execute runs a workflow and its stages/actions.
// It applies any configured middleware.
func (r *Runner) Execute(ctx context.Context, workflow *Workflow, logger Logger) error {
	// If no logger is provided, use the runner's default.
	if logger == nil {
		logger = r.defaultLogger
	}

	// Build the middleware chain and the core execution function
	chain := r.executeWorkflow
	for i := len(r.middleware) - 1; i >= 0; i-- {
		chain = r.middleware[i](chain)
	}

	// Execute the chain
	return chain(ctx, workflow, logger)
}

// executeWorkflow is the core workflow execution logic
func (r *Runner) executeWorkflow(ctx context.Context, w *Workflow, logger Logger) error {
	w.Context["runner"] = r // Expose runner to the context

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

	// Define a core function that executes a stage with workflow middleware
	executeStageWithMiddleware := func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error {
		// Skip disabled stages
		if disabledStages[stage.ID] {
			logger.Debug("Skipping disabled stage: %s", stage.Name)
			return nil
		}

		// Update stage status in store
		stageKey := PrefixStage + stage.ID
		workflow.Store.SetProperty(stageKey, PropStatus, StatusRunning)

		// Execute the stage
		logger.Debug("Executing stage: %s", stage.Name)
		if err := r.executeStage(ctx, stage, workflow, logger); err != nil {
			workflow.Store.SetProperty(stageKey, PropStatus, StatusFailed)
			workflow.Store.SetProperty(workflowKey, PropStatus, StatusFailed)
			return fmt.Errorf("stage '%s' failed: %w", stage.Name, err)
		}

		logger.Info("Completed stage: %s", stage.Name)
		workflow.Store.SetProperty(stageKey, PropStatus, StatusCompleted)
		return nil
	}

	// We need to execute stages one by one, as dynamic stages can be inserted during execution
	for i := 0; i < len(w.Stages); i++ {
		stage := w.Stages[i]

		// Create a base stage runner function
		stageRunner := executeStageWithMiddleware

		// Apply workflow middleware in reverse order (so first middleware is outermost)
		if w.middleware != nil && len(w.middleware) > 0 {
			for j := len(w.middleware) - 1; j >= 0; j-- {
				stageRunner = w.middleware[j](stageRunner)
			}
		}

		// Execute stage with workflow middleware
		if err := stageRunner(ctx, stage, w, logger); err != nil {
			return err
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

	// Define the core stage execution function
	executeStageCore := func(ctx context.Context, stage *Stage, wf *Workflow, logger Logger) error {
		// We need to execute actions one by one, as dynamic actions can be inserted during execution
		for i := 0; i < len(stage.Actions); i++ {
			action := stage.Actions[i]
			actionKey := PrefixAction + stage.ID + ":" + action.Name()

			// Update action status in store
			wf.Store.SetProperty(actionKey, PropStatus, StatusRunning)

			// Skip disabled actions
			if actionCtx.disabledActions[action.Name()] {
				logger.Debug("Skipping disabled action: %s", action.Name())
				wf.Store.SetProperty(actionKey, PropStatus, StatusSkipped)
				continue
			}

			logger.Debug("Executing action %d/%d: %s", i+1, len(stage.Actions), action.Name())

			// Update the context with the current action and position info
			actionCtx.Action = action
			actionCtx.ActionIndex = i
			actionCtx.IsLastAction = (i == len(stage.Actions)-1)

			// Define the core action execution function
			executeActionCore := func(ctx *ActionContext, act Action, index int, isLast bool) error {
				return act.Execute(ctx)
			}

			// Create a function for running through any workflow-level action middleware
			// We can add this feature later if needed

			// Execute the action
			err := executeActionCore(actionCtx, action, i, actionCtx.IsLastAction)
			if err != nil {
				wf.Store.SetProperty(actionKey, PropStatus, StatusFailed)
				return fmt.Errorf("action '%s' failed: %w", action.Name(), err)
			}

			// Check if the action generated new actions to be inserted
			if len(actionCtx.dynamicActions) > 0 {
				logger.Debug("Action generated %d new actions", len(actionCtx.dynamicActions))

				// Insert the new actions after the current one
				newActions := make([]Action, 0, len(stage.Actions)+len(actionCtx.dynamicActions))
				newActions = append(newActions, stage.Actions[:i+1]...)

				// Store each dynamic action in the KV store
				for _, dynAction := range actionCtx.dynamicActions {
					// Create a key for the action
					dynActionKey := PrefixAction + stage.ID + ":" + dynAction.Name()

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
					wf.Store.PutWithMetadata(dynActionKey, dynAction.Description(), meta)
				}

				newActions = append(newActions, actionCtx.dynamicActions...)
				if i+1 < len(stage.Actions) {
					newActions = append(newActions, stage.Actions[i+1:]...)
				}
				stage.Actions = newActions

				// Clear dynamic actions for the next iteration
				actionCtx.dynamicActions = []Action{}
			}

			// Check if the action generated new stages to be inserted
			if len(actionCtx.dynamicStages) > 0 {
				logger.Debug("Action generated %d new stages", len(actionCtx.dynamicStages))

				// Store the stages to be added to the workflow after this stage completes
				wf.Context["dynamicStages"] = actionCtx.dynamicStages

				// Clear dynamic stages for the next iteration
				actionCtx.dynamicStages = []*Stage{}
			}

			logger.Debug("Completed action %d/%d: %s", i+1, len(stage.Actions), action.Name())
			wf.Store.SetProperty(actionKey, PropStatus, StatusCompleted)
		}

		return nil
	}

	// Apply stage middleware
	var stageHandler StageRunnerFunc = executeStageCore

	// Apply middleware in reverse order (so the first middleware is the outermost wrapper)
	if s.middleware != nil {
		for i := len(s.middleware) - 1; i >= 0; i-- {
			stageHandler = s.middleware[i](stageHandler)
		}
	}

	// Execute stage with middleware chain
	err := stageHandler(ctx, s, workflow, logger)

	// Store the updated disabled maps back in the workflow context
	workflow.Context["disabledActions"] = actionCtx.disabledActions
	workflow.Context["disabledStages"] = actionCtx.disabledStages

	return err
}

// RunResult contains the result of a workflow execution
type RunResult struct {
	WorkflowID    string
	Success       bool
	Error         error
	ExecutionTime time.Duration
	// FinalStore contains the workflow's store state after execution
	FinalStore map[string]interface{}
}

// RunOptions contains options for workflow execution
type RunOptions struct {
	// Logger to use for the workflow execution
	Logger Logger

	// Context to use for the workflow execution
	Context context.Context

	// Whether to ignore workflow errors and continue execution
	IgnoreErrors bool

	// InitialStore contains key-value pairs to populate the workflow store before execution
	InitialStore map[string]interface{}
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

	// Populate the initial store if provided
	if options.InitialStore != nil {
		for key, value := range options.InitialStore {
			if err := workflow.Store.Put(key, value); err != nil {
				// Log the error but continue
				logger.Warn("Failed to set initial store value %s: %v", key, err)
			}
		}
	}

	// Execute the workflow
	err := r.Execute(ctx, workflow, logger)

	// Capture the final store state
	finalStore := make(map[string]interface{})
	if workflow.Store != nil {
		// Export all store data
		finalStore = workflow.Store.ExportAll()
	}

	// Create result
	result := RunResult{
		WorkflowID:    workflow.ID,
		Success:       err == nil,
		Error:         err,
		ExecutionTime: time.Since(startTime),
		FinalStore:    finalStore,
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

// Example middleware functions for stages

// LoggingStageMiddleware creates a middleware that logs stage execution steps
func LoggingStageMiddleware() StageMiddleware {
	return func(next StageRunnerFunc) StageRunnerFunc {
		return func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error {
			logger.Info("Stage middleware: Starting stage %s", stage.Name)

			start := time.Now()
			err := next(ctx, stage, workflow, logger)
			duration := time.Since(start)

			if err != nil {
				logger.Error("Stage middleware: Stage %s failed after %v: %v",
					stage.Name, duration.Round(time.Millisecond), err)
			} else {
				logger.Info("Stage middleware: Stage %s completed in %v",
					stage.Name, duration.Round(time.Millisecond))
			}

			return err
		}
	}
}

// ContainerStageMiddleware creates a middleware that "pops" a container at the start
// of a stage and closes it at the end. This is a placeholder that demonstrates
// the pattern - in a real implementation you would add your container logic.
func ContainerStageMiddleware(containerImage string, containerName string) StageMiddleware {
	return func(next StageRunnerFunc) StageRunnerFunc {
		return func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error {
			// Start the container
			logger.Info("Starting container %s (image: %s) for stage %s",
				containerName, containerImage, stage.Name)

			// Here you would add your actual container startup logic:
			// - Docker API calls
			// - Command execution
			// - Container configuration

			// Execute the stage
			err := next(ctx, stage, workflow, logger)

			// Always stop the container, even if the stage failed
			logger.Info("Stopping container %s for stage %s", containerName, stage.Name)

			// Here you would add your container cleanup logic:
			// - Stop container
			// - Remove container
			// - Cleanup resources

			// Return any error from the stage execution
			return err
		}
	}
}

// StoreInjectionStageMiddleware creates a middleware that injects values into the stage's initialStore
func StoreInjectionStageMiddleware(keyValues map[string]interface{}) StageMiddleware {
	return func(next StageRunnerFunc) StageRunnerFunc {
		return func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error {
			// Inject values into the initial store
			for key, value := range keyValues {
				stage.SetInitialData(key, value)
			}

			// Continue execution
			return next(ctx, stage, workflow, logger)
		}
	}
}

// ActionProgressMiddleware creates a middleware that reports on action execution progress
// This demonstrates how to implement middleware that runs at the action level
func ActionProgressMiddleware() StageMiddleware {
	return func(next StageRunnerFunc) StageRunnerFunc {
		return func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error {
			// Store the total action count for progress reporting
			totalActions := len(stage.Actions)
			logger.Info("Starting execution of %d actions in stage %s", totalActions, stage.Name)

			// Save the current action count before execution
			// (this accounts for dynamic actions that might be added)
			beforeCount := len(stage.Actions)

			// Execute the stage
			err := next(ctx, stage, workflow, logger)

			// Report on actions completed and any dynamically added
			afterCount := len(stage.Actions)
			dynamicCount := afterCount - beforeCount

			if dynamicCount > 0 {
				logger.Info("Completed stage %s with %d original actions plus %d dynamic actions",
					stage.Name, beforeCount, dynamicCount)
			} else {
				logger.Info("Completed stage %s with %d actions", stage.Name, afterCount)
			}

			return err
		}
	}
}

// Example workflow middleware functions

// LoggingStageExecutionMiddleware creates a workflow middleware that logs individual stage execution
func LoggingStageExecutionMiddleware() WorkflowMiddleware {
	return func(next WorkflowStageRunnerFunc) WorkflowStageRunnerFunc {
		return func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error {
			logger.Info("Workflow middleware: Starting stage %s in workflow %s", stage.Name, workflow.Name)

			start := time.Now()
			err := next(ctx, stage, workflow, logger)
			duration := time.Since(start)

			if err != nil {
				logger.Error("Workflow middleware: Stage %s in workflow %s failed after %v: %v",
					stage.Name, workflow.Name, duration.Round(time.Millisecond), err)
			} else {
				logger.Info("Workflow middleware: Stage %s in workflow %s completed in %v",
					stage.Name, workflow.Name, duration.Round(time.Millisecond))
			}

			return err
		}
	}
}

// StageFilterMiddleware creates a workflow middleware that can conditionally skip stages
func StageFilterMiddleware(filter func(*Stage) bool) WorkflowMiddleware {
	return func(next WorkflowStageRunnerFunc) WorkflowStageRunnerFunc {
		return func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error {
			// Skip the stage if it doesn't pass the filter
			if !filter(stage) {
				logger.Info("Workflow middleware: Skipping stage %s based on filter criteria", stage.Name)
				return nil
			}

			// Stage passes the filter, execute it
			return next(ctx, stage, workflow, logger)
		}
	}
}

// StageDataInjectionMiddleware creates a workflow middleware that injects data into each stage's initialStore
func StageDataInjectionMiddleware(getData func(*Stage) map[string]interface{}) WorkflowMiddleware {
	return func(next WorkflowStageRunnerFunc) WorkflowStageRunnerFunc {
		return func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error {
			// Get the data to inject for this specific stage
			data := getData(stage)

			// Inject the data into the stage's initialStore
			for key, value := range data {
				stage.SetInitialData(key, value)
			}

			// Continue with stage execution
			return next(ctx, stage, workflow, logger)
		}
	}
}

// StageNotificationMiddleware creates a workflow middleware that sends notifications before and after stage execution
func StageNotificationMiddleware(
	beforeNotify func(*Stage, *Workflow),
	afterNotify func(*Stage, *Workflow, error)) WorkflowMiddleware {
	return func(next WorkflowStageRunnerFunc) WorkflowStageRunnerFunc {
		return func(ctx context.Context, stage *Stage, workflow *Workflow, logger Logger) error {
			// Send notification before stage execution
			if beforeNotify != nil {
				beforeNotify(stage, workflow)
			}

			// Execute the stage
			err := next(ctx, stage, workflow, logger)

			// Send notification after stage execution, including any error
			if afterNotify != nil {
				afterNotify(stage, workflow, err)
			}

			return err
		}
	}
}

// SpawnResult contains the result of a spawned workflow execution
type SpawnResult struct {
	Success    bool
	Error      error
	FinalStore map[string]interface{}
}

// SpawnWithStore executes a sub-workflow in a new child process with an initial store.
// It returns a SpawnResult that includes the final store state from the child process.
func (r *Runner) SpawnWithStore(ctx context.Context, def SubWorkflowDef, initialStore map[string]interface{}) SpawnResult {
	// Add initial store to the definition
	if initialStore != nil {
		if def.InitialStore == nil {
			def.InitialStore = make(map[string]interface{})
		}
		// Merge initial store into the definition
		for key, value := range initialStore {
			def.InitialStore[key] = value
		}
	}

	// Track the final store state
	var finalStore map[string]interface{}

	// Add a message handler to capture the final store
	if r.Broker != nil {
		r.Broker.RegisterHandler(MessageTypeFinalStore, func(msgType MessageType, payload json.RawMessage) error {
			var storeData map[string]interface{}
			if err := json.Unmarshal(payload, &storeData); err != nil {
				return fmt.Errorf("failed to unmarshal final store: %w", err)
			}
			finalStore = storeData
			return nil
		})
	}

	// Execute the spawn
	err := r.Spawn(ctx, def)

	return SpawnResult{
		Success:    err == nil,
		Error:      err,
		FinalStore: finalStore,
	}
}

// Spawn executes a sub-workflow in a new child process with middleware support.
// It sets up IPC pipes for communication and waits for the child to complete.
func (r *Runner) Spawn(ctx context.Context, def SubWorkflowDef) error {
	// Apply BeforeSpawn middleware
	currentCtx := ctx
	currentDef := def
	var err error

	for _, mw := range r.spawnMiddleware {
		currentCtx, currentDef, err = mw.BeforeSpawn(currentCtx, currentDef)
		if err != nil {
			return fmt.Errorf("spawn middleware BeforeSpawn error: %w", err)
		}
	}

	// Execute the actual spawn process
	spawnErr := r.executeSpawn(currentCtx, currentDef)

	// Apply AfterSpawn middleware (always run, even on error)
	for _, mw := range r.spawnMiddleware {
		if afterErr := mw.AfterSpawn(currentCtx, currentDef, spawnErr); afterErr != nil {
			// Log the middleware error but don't override the original spawn error
			fmt.Fprintf(os.Stderr, "spawn middleware AfterSpawn error: %v\n", afterErr)
		}
	}

	return spawnErr
}

// executeSpawn contains the core spawn logic, separated for middleware integration
func (r *Runner) executeSpawn(ctx context.Context, def SubWorkflowDef) error {
	// Get the gRPC transport from the broker
	grpcTransport := r.Broker.GetTransport()

	// Only start the server if it's not already started
	if !grpcTransport.IsServerReady() {
		if err := grpcTransport.StartServer(); err != nil {
			return fmt.Errorf("failed to start gRPC server: %w", err)
		}

		// Wait for server to be ready
		serverCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := grpcTransport.WaitForServerReady(serverCtx); err != nil {
			return fmt.Errorf("gRPC server not ready: %w", err)
		}
	}

	// Get the actual assigned port from the transport
	actualPort := grpcTransport.GetActualPort()
	grpcAddress := grpcTransport.address

	// Get the path to the current executable
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to find executable path: %w", err)
	}

	// Create the command to run the child process with gRPC connection info
	cmd := exec.CommandContext(ctx, exePath,
		"--gostage-child",
		fmt.Sprintf("--grpc-address=%s", grpcAddress),
		fmt.Sprintf("--grpc-port=%d", actualPort))

	// Redirect child's stderr to the parent's for logging
	cmd.Stderr = os.Stderr

	// Start the child process
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start child process: %w", err)
	}

	// Register OnChildMessage callbacks from spawn middleware
	for _, mw := range r.spawnMiddleware {
		r.Broker.AddMessageCallback(mw.OnChildMessage)
	}

	// Wait for child to signal it's ready to receive workflow definitions
	childId := fmt.Sprintf("child-%d", cmd.Process.Pid)

	// Store the workflow definition for the child to request
	if err := grpcTransport.SetPendingWorkflowDef(childId, def); err != nil {
		cmd.Process.Kill()
		return fmt.Errorf("failed to set pending workflow definition: %w", err)
	}

	if err := grpcTransport.WaitForChildReady(childId, 10*time.Second); err != nil {
		// Kill the child process if it doesn't become ready
		cmd.Process.Kill()
		return fmt.Errorf("child process not ready: %w", err)
	}

	// Wait for the child process to finish
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("child process exited with error: %w", err)
	}

	return nil
}

// UseSpawnMiddleware adds spawn middleware to the runner
func (r *Runner) UseSpawnMiddleware(middleware ...SpawnMiddleware) {
	r.spawnMiddleware = append(r.spawnMiddleware, middleware...)
}

// AddIPCMiddleware adds IPC middleware to the runner's broker
func (r *Runner) AddIPCMiddleware(middleware ...IPCMiddleware) {
	r.Broker.AddIPCMiddleware(middleware...)
}

// NewChildRunner creates a child runner with automatic gRPC setup and logger
// This is the seamless API for child processes - no manual argument parsing needed
func NewChildRunner() (*ChildRunner, Logger, error) {
	// Automatically parse gRPC connection arguments from command line
	var grpcAddress string = "localhost"
	var grpcPort int = 50051

	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "--grpc-address=") {
			grpcAddress = strings.TrimPrefix(arg, "--grpc-address=")
		} else if strings.HasPrefix(arg, "--grpc-port=") {
			if port, err := strconv.Atoi(strings.TrimPrefix(arg, "--grpc-port=")); err == nil {
				grpcPort = port
			}
		}
	}

	// Create gRPC transport and connect to parent
	grpcTransport, err := NewGRPCTransport(grpcAddress, grpcPort)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC transport: %w", err)
	}

	// Connect as client to parent's gRPC server
	if err := grpcTransport.ConnectClient(); err != nil {
		return nil, nil, fmt.Errorf("failed to connect to parent gRPC server: %w", err)
	}

	// Wait for client to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := grpcTransport.WaitForClientReady(ctx); err != nil {
		return nil, nil, fmt.Errorf("gRPC client not ready: %w", err)
	}

	// Create runner broker with gRPC transport
	broker := NewRunnerBrokerWithTransport(grpcTransport)

	// Create base runner
	runner := &Runner{
		Broker:     broker,
		middleware: []Middleware{},
	}

	// Create child runner that embeds the base runner
	childRunner := &ChildRunner{
		Runner: runner,
	}

	// Create logger that automatically sends to parent via gRPC
	logger := &GRPCLogger{broker: broker}

	return childRunner, logger, nil
}

// RequestWorkflowDefinitionFromParent requests the workflow definition using our PID
// No need to manually construct child IDs - parent tracks by PID automatically
func (cr *ChildRunner) RequestWorkflowDefinitionFromParent(ctx context.Context) (*SubWorkflowDef, error) {
	childPID := os.Getpid()
	grpcTransport := cr.Broker.GetTransport()
	return grpcTransport.RequestWorkflowDefinitionFromParent(ctx, fmt.Sprintf("child-%d", childPID))
}

// Close closes the child runner and cleans up gRPC connections
func (cr *ChildRunner) Close() error {
	return cr.Broker.Close()
}

// GRPCLogger implements Logger interface and sends all logs to parent via gRPC
type GRPCLogger struct {
	broker *RunnerBroker
}

func (l *GRPCLogger) send(level, format string, args ...interface{}) {
	payload := map[string]string{
		"level":   level,
		"message": fmt.Sprintf(format, args...),
	}
	// Send log message to parent via gRPC
	l.broker.Send(MessageTypeLog, payload)
}

func (l *GRPCLogger) Debug(format string, args ...interface{}) { l.send("DEBUG", format, args...) }
func (l *GRPCLogger) Info(format string, args ...interface{})  { l.send("INFO", format, args...) }
func (l *GRPCLogger) Warn(format string, args ...interface{})  { l.send("WARN", format, args...) }
func (l *GRPCLogger) Error(format string, args ...interface{}) { l.send("ERROR", format, args...) }
