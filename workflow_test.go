package gostage

import (
	"context"
	"fmt"
	"testing"

	"github.com/davidroman0O/gostage/store"
	"github.com/stretchr/testify/assert"
)

// TestLogger is a simple logger implementation for testing
type TestLogger struct {
	t *testing.T
}

func (l *TestLogger) Debug(format string, args ...interface{}) {
	l.t.Logf("[DEBUG] "+format, args...)
}

func (l *TestLogger) Info(format string, args ...interface{}) {
	l.t.Logf("[INFO] "+format, args...)
}

func (l *TestLogger) Warn(format string, args ...interface{}) {
	l.t.Logf("[WARN] "+format, args...)
}

func (l *TestLogger) Error(format string, args ...interface{}) {
	l.t.Logf("[ERROR] "+format, args...)
}

// TestAction is a simple action implementation for testing
type TestAction struct {
	BaseAction
	executeFunc func(ctx *ActionContext) error
	customTags  []string
}

// NewTestAction creates a new test action with a name, description, and execution function.
// It's a convenient helper for creating mock actions in tests.
func NewTestAction(name, description string, executeFunc func(ctx *ActionContext) error) *TestAction {
	return &TestAction{
		BaseAction:  NewBaseAction(name, description),
		executeFunc: executeFunc,
		customTags:  []string{},
	}
}

// NewTestActionWithTags creates a new test action with tags
func NewTestActionWithTags(name, description string, tags []string, executeFunc func(ctx *ActionContext) error) *TestAction {
	return &TestAction{
		BaseAction:  NewBaseActionWithTags(name, description, tags),
		executeFunc: executeFunc,
		customTags:  tags,
	}
}

// Execute implements Action.Execute
func (a *TestAction) Execute(ctx *ActionContext) error {
	if a.executeFunc != nil {
		return a.executeFunc(ctx)
	}
	return nil
}

// Tags overrides BaseAction.Tags to allow for custom tags
func (a *TestAction) Tags() []string {
	if len(a.customTags) > 0 {
		return a.customTags
	}
	return a.BaseAction.Tags()
}

// AddTag adds a tag to the test action
func (a *TestAction) AddTag(tag string) {
	a.customTags = append(a.customTags, tag)
}

func TestWorkflowExecution(t *testing.T) {
	// Create a new workflow
	workflow := NewWorkflow("test-workflow", "Test Workflow", "A test workflow")

	// Add some data to the workflow store
	err := workflow.Store.Put("workflow-key", "workflow-value")
	assert.NoError(t, err)

	// Create a stage with initial store data
	stage := NewStage("test-stage", "Test Stage", "A test stage")
	err = stage.SetInitialData("stage-key", "stage-value")
	assert.NoError(t, err)

	// Add an action that checks the stores
	action := NewTestAction("test-action", "Test Action", func(ctx *ActionContext) error {
		// Check workflow key exists
		val, err := store.Get[string](ctx.Store(), "workflow-key")
		if err != nil {
			return fmt.Errorf("workflow key not found: %w", err)
		}
		if val != "workflow-value" {
			return fmt.Errorf("unexpected workflow key value: %s", val)
		}

		// Check stage key exists (should be merged into workflow store)
		val, err = store.Get[string](ctx.Store(), "stage-key")
		if err != nil {
			return fmt.Errorf("stage key not found: %w", err)
		}
		if val != "stage-value" {
			return fmt.Errorf("unexpected stage key value: %s", val)
		}

		// Set a new key
		err = ctx.Store().Put("action-key", "action-value")
		if err != nil {
			return fmt.Errorf("failed to set action key: %w", err)
		}

		return nil
	})

	stage.AddAction(action)
	workflow.AddStage(stage)

	// Execute the workflow
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err = runner.Execute(context.Background(), workflow, logger)
	assert.NoError(t, err)

	// Verify store state after execution
	val, err := store.Get[string](workflow.Store, "action-key")
	assert.NoError(t, err)
	assert.Equal(t, "action-value", val)
}

func TestDynamicActions(t *testing.T) {
	// Create a new workflow
	workflow := NewWorkflow("dynamic-workflow", "Dynamic Workflow", "A workflow with dynamic actions")

	// Create a stage
	stage := NewStage("dynamic-stage", "Dynamic Stage", "A stage with dynamic actions")

	// Add an action that generates more actions
	counter := 0
	generatorAction := NewTestAction("generator", "Generates more actions", func(ctx *ActionContext) error {
		// Add two more actions
		ctx.AddDynamicAction(NewTestAction("dynamic-1", "Generated Action 1", func(innerCtx *ActionContext) error {
			counter++
			return nil
		}))

		ctx.AddDynamicAction(NewTestAction("dynamic-2", "Generated Action 2", func(innerCtx *ActionContext) error {
			counter++
			return nil
		}))

		return nil
	})

	stage.AddAction(generatorAction)
	workflow.AddStage(stage)

	// Execute the workflow
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(context.Background(), workflow, logger)
	assert.NoError(t, err)

	// Both dynamic actions should have executed
	assert.Equal(t, 2, counter)
}

func TestDynamicStages(t *testing.T) {
	// Create a new workflow
	workflow := NewWorkflow("dynamic-stages", "Dynamic Stages", "A workflow with dynamic stages")

	// Create initial stage
	initialStage := NewStage("initial-stage", "Initial Stage", "First stage that generates another stage")

	// Counter to track execution
	stageCounter := 0
	actionCounter := 0

	// Add an action that generates a new stage
	generatorAction := NewTestAction("stage-generator", "Generates a new stage", func(ctx *ActionContext) error {
		actionCounter++

		// Create a new stage with an action
		newStage := NewStage("generated-stage", "Generated Stage", "Dynamically generated stage")

		// Add an action to the new stage
		newStage.AddAction(NewTestAction("generated-action", "Generated Action", func(innerCtx *ActionContext) error {
			stageCounter++
			return nil
		}))

		// Add the stage dynamically
		ctx.AddDynamicStage(newStage)
		return nil
	})

	initialStage.AddAction(generatorAction)
	workflow.AddStage(initialStage)

	// Execute the workflow
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(context.Background(), workflow, logger)
	assert.NoError(t, err)

	// Verify that both the generator action and the generated stage executed
	assert.Equal(t, 1, actionCounter, "Generator action should have executed once")
	assert.Equal(t, 1, stageCounter, "Generated stage should have executed once")
}

func TestActionTags(t *testing.T) {
	// Create workflow
	workflow := NewWorkflow("tag-workflow", "Tag Workflow", "A workflow for testing tags")

	// Create stage
	stage := NewStage("tag-stage", "Tag Stage", "A stage for testing tags")

	// Add actions with various tags
	action1 := NewTestActionWithTags("action1", "Action 1", []string{"tag1", "common"}, nil)
	action2 := NewTestActionWithTags("action2", "Action 2", []string{"tag2", "common"}, nil)
	action3 := NewTestActionWithTags("action3", "Action 3", []string{"tag3"}, nil)

	stage.AddAction(action1)
	stage.AddAction(action2)
	stage.AddAction(action3)
	workflow.AddStage(stage)

	// Run the workflow with a context that we'll check in testing
	logger := &TestLogger{t: t}
	ctx := context.Background()
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)
	assert.NoError(t, err)

	// Create an action context for testing
	actionCtx := &ActionContext{
		GoContext: ctx,
		Workflow:  workflow,
		Stage:     stage,
		Action:    nil,
		Logger:    logger,
	}

	// Test various tag-related functions
	byTag1 := actionCtx.FindActionsByTag("tag1")
	assert.Len(t, byTag1, 1)
	assert.Equal(t, "action1", byTag1[0].Name())

	byCommon := actionCtx.FindActionsByTag("common")
	assert.Len(t, byCommon, 2)

	byMultiple := actionCtx.FindActionsByTags([]string{"tag1", "common"})
	assert.Len(t, byMultiple, 1)
	assert.Equal(t, "action1", byMultiple[0].Name())

	byAny := actionCtx.FindActionsByAnyTag([]string{"tag1", "tag3"})
	assert.Len(t, byAny, 2)
}

func TestStageAndWorkflowTags(t *testing.T) {
	// Create workflow with tags
	workflow := NewWorkflowWithTags("tag-workflow", "Tag Workflow", "A workflow with tags", []string{"workflow-tag"})

	// Create stage with tags
	stage1 := NewStageWithTags("stage1", "Stage 1", "First stage", []string{"stage-tag", "common"})
	stage2 := NewStageWithTags("stage2", "Stage 2", "Second stage", []string{"unique-tag", "common"})

	workflow.AddStage(stage1)
	workflow.AddStage(stage2)

	// Add some actions to make the workflow valid
	stage1.AddAction(NewTestAction("action1", "Action 1", nil))
	stage2.AddAction(NewTestAction("action2", "Action 2", nil))

	// Execute the workflow to populate stage info in the store
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(context.Background(), workflow, logger)
	assert.NoError(t, err)

	// Test various tag-related functions
	assert.True(t, workflow.HasTag("workflow-tag"))
	assert.True(t, stage1.HasTag("stage-tag"))
	assert.True(t, stage2.HasTag("unique-tag"))

	// Test stage retrieval by tag
	stagesByTag := workflow.ListStagesByTag("common")
	assert.Len(t, stagesByTag, 2)

	stagesByUniqueTag := workflow.ListStagesByTag("unique-tag")
	assert.Len(t, stagesByUniqueTag, 1)
	assert.Equal(t, "stage2", stagesByUniqueTag[0].ID)
}

// TestWorkflowWithStage tests creating a workflow with a stage that has initial data
func TestWorkflowWithStage(t *testing.T) {
	// Create a workflow
	workflow := NewWorkflow("test-wf", "Test Workflow", "A test workflow")
	workflow.Store.Put("workflow-key", "workflow-value")

	// Create a stage with initial data
	stage := NewStage("test-stage", "Test Stage", "A test stage")
	stage.SetInitialData("stage-key", "stage-value")

	// Add an action to the stage
	stage.AddAction(NewTestAction("test-action", "Test Action", func(ctx *ActionContext) error {
		// Check workflow key exists
		val, err := store.Get[string](ctx.Store(), "workflow-key")
		if err != nil {
			return fmt.Errorf("workflow key not found: %w", err)
		}
		if val != "workflow-value" {
			return fmt.Errorf("expected workflow-value, got %s", val)
		}

		// Check stage key exists (should be merged into workflow store)
		val, err = store.Get[string](ctx.Store(), "stage-key")
		if err != nil {
			return fmt.Errorf("stage key not found: %w", err)
		}
		if val != "stage-value" {
			return fmt.Errorf("expected stage-value, got %s", val)
		}

		// Set a new key
		err = ctx.Store().Put("action-key", "action-value")
		if err != nil {
			return fmt.Errorf("failed to set action key: %w", err)
		}

		return nil
	}))

	// Add the stage to the workflow
	workflow.AddStage(stage)

	// Create a runner and execute the workflow
	runner := NewRunner()
	options := DefaultRunOptions()
	options.Logger = &TestLogger{t: t}

	result := runner.ExecuteWithOptions(workflow, options)
	assert.True(t, result.Success)

	// Verify action key was set
	val, err := store.Get[string](workflow.Store, "action-key")
	assert.NoError(t, err)
	assert.Equal(t, "action-value", val)
}
