package gostate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunWorkflow(t *testing.T) {
	// Create a simple test workflow
	wf := NewWorkflow("test-runner", "Test Runner", "Test workflow for runner")

	// Create a stage with a simple action
	stage := NewStage("test-stage", "Test Stage", "Test stage for runner")

	// Add an action that succeeds
	action := NewTestAction("test-action", "Test Action", func(ctx *ActionContext) error {
		return nil
	})

	stage.AddAction(action)
	wf.AddStage(stage)

	// Run the workflow with default options
	result := RunWorkflow(wf, DefaultRunOptions())

	// Check results
	assert.True(t, result.Success)
	assert.NoError(t, result.Error)
	assert.Equal(t, "test-runner", result.WorkflowID)
	assert.Greater(t, result.ExecutionTime.Nanoseconds(), int64(0))
}

func TestRunWorkflowWithError(t *testing.T) {
	// Create a simple test workflow
	wf := NewWorkflow("error-workflow", "Error Workflow", "Test workflow that fails")

	// Create a stage with a simple action
	stage := NewStage("error-stage", "Error Stage", "Test stage that fails")

	// Add an action that fails
	expectedErr := errors.New("test error")
	action := NewTestAction("error-action", "Error Action", func(ctx *ActionContext) error {
		return expectedErr
	})

	stage.AddAction(action)
	wf.AddStage(stage)

	// Run the workflow with default options
	result := RunWorkflow(wf, DefaultRunOptions())

	// Check results
	assert.False(t, result.Success)
	assert.Error(t, result.Error)
	assert.True(t, errors.Is(result.Error, expectedErr) || strings.Contains(result.Error.Error(), expectedErr.Error()))
	assert.Equal(t, "error-workflow", result.WorkflowID)
}

func TestRunMultipleWorkflows(t *testing.T) {
	// Create a successful workflow
	wf1 := NewWorkflow("success-1", "Success 1", "First successful workflow")
	stage1 := NewStage("stage-1", "Stage 1", "First stage")
	action1 := NewTestAction("action-1", "Action 1", func(ctx *ActionContext) error {
		return nil
	})
	stage1.AddAction(action1)
	wf1.AddStage(stage1)

	// Create another successful workflow
	wf2 := NewWorkflow("success-2", "Success 2", "Second successful workflow")
	stage2 := NewStage("stage-2", "Stage 2", "Second stage")
	action2 := NewTestAction("action-2", "Action 2", func(ctx *ActionContext) error {
		return nil
	})
	stage2.AddAction(action2)
	wf2.AddStage(stage2)

	// Create a failing workflow
	wf3 := NewWorkflow("failure", "Failure", "Failing workflow")
	stage3 := NewStage("stage-3", "Stage 3", "Third stage")
	action3 := NewTestAction("action-3", "Action 3", func(ctx *ActionContext) error {
		return errors.New("test error")
	})
	stage3.AddAction(action3)
	wf3.AddStage(stage3)

	// Run workflows with default options (stop on first error)
	results := RunWorkflows([]*Workflow{wf1, wf2, wf3}, DefaultRunOptions())

	// Only three workflows should have run, with the third one failing
	assert.Equal(t, 3, len(results))
	assert.True(t, results[0].Success)
	assert.True(t, results[1].Success)
	assert.False(t, results[2].Success)

	// Run with ignoring errors
	options := DefaultRunOptions()
	options.IgnoreErrors = true
	results = RunWorkflows([]*Workflow{wf1, wf2, wf3}, options)

	// All workflows should have run
	assert.Equal(t, 3, len(results))
	assert.True(t, results[0].Success)
	assert.True(t, results[1].Success)
	assert.False(t, results[2].Success)

	// Check formatting
	summary := FormatResults(results)
	assert.Contains(t, summary, "SUCCESS")
	assert.Contains(t, summary, "FAILED")
	assert.Contains(t, summary, "Summary: 2/3 workflows succeeded")
}

func TestRunWorkflowWithContext(t *testing.T) {
	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Create a workflow that checks if the context is done
	wf := NewWorkflow("context-test", "Context Test", "Test context cancellation")
	stage := NewStage("context-stage", "Context Stage", "Test stage for context")

	contextChecked := false
	action := NewTestAction("context-action", "Context Action", func(ctx *ActionContext) error {
		// Cancel the context
		cancel()

		// Check if the context is done
		select {
		case <-ctx.GoContext.Done():
			contextChecked = true
			return nil
		default:
			return errors.New("context not cancelled")
		}
	})

	stage.AddAction(action)
	wf.AddStage(stage)

	// Run with custom context
	options := DefaultRunOptions()
	options.Context = ctx

	result := RunWorkflow(wf, options)

	// Should succeed because we check that the context is cancelled
	assert.True(t, result.Success)
	assert.True(t, contextChecked)
}
