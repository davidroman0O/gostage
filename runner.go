package gostage

import (
	"context"
	"fmt"
	"time"
)

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

// RunWorkflow executes a workflow with the provided options
func RunWorkflow(workflow *Workflow, options RunOptions) RunResult {
	startTime := time.Now()

	// Use default logger if none provided
	logger := options.Logger
	if logger == nil {
		logger = NewDefaultLogger()
	}

	// Use background context if none provided
	ctx := options.Context
	if ctx == nil {
		ctx = context.Background()
	}

	// Execute the workflow
	err := workflow.Execute(ctx, logger)

	// Create result
	result := RunResult{
		WorkflowID:    workflow.ID,
		Success:       err == nil,
		Error:         err,
		ExecutionTime: time.Since(startTime),
	}

	return result
}

// RunWorkflows executes multiple workflows in sequence
func RunWorkflows(workflows []*Workflow, options RunOptions) []RunResult {
	results := make([]RunResult, 0, len(workflows))

	for i, wf := range workflows {
		// Run the current workflow
		result := RunWorkflow(wf, options)
		results = append(results, result)

		// Stop after executing a failing workflow if we're not ignoring errors
		if !result.Success && !options.IgnoreErrors && i < len(workflows)-1 {
			break
		}
	}

	return results
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
