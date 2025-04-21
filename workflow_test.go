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

// NewTestAction creates a new test action
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
	err = stage.InitialStore.Put("stage-key", "stage-value")
	assert.NoError(t, err)

	// Add an action that checks the stores
	action := NewTestAction("test-action", "Test Action", func(ctx *ActionContext) error {
		// Check workflow key exists
		val, err := store.Get[string](ctx.Store, "workflow-key")
		if err != nil {
			return fmt.Errorf("workflow key not found: %w", err)
		}
		if val != "workflow-value" {
			return fmt.Errorf("unexpected workflow key value: %s", val)
		}

		// Check stage key exists (should be merged into workflow store)
		val, err = store.Get[string](ctx.Store, "stage-key")
		if err != nil {
			return fmt.Errorf("stage key not found: %w", err)
		}
		if val != "stage-value" {
			return fmt.Errorf("unexpected stage key value: %s", val)
		}

		// Set a new key
		err = ctx.Store.Put("action-key", "action-value")
		if err != nil {
			return fmt.Errorf("failed to set action key: %w", err)
		}

		return nil
	})

	stage.AddAction(action)
	workflow.AddStage(stage)

	// Execute the workflow
	logger := &TestLogger{t: t}
	err = workflow.Execute(context.Background(), logger)
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
	err := workflow.Execute(context.Background(), logger)
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
	err := workflow.Execute(context.Background(), logger)
	assert.NoError(t, err)

	// Verify that both the generator action and the generated stage executed
	assert.Equal(t, 1, actionCounter, "Generator action should have executed once")
	assert.Equal(t, 1, stageCounter, "Generated stage action should have executed once")
	assert.Equal(t, 2, len(workflow.Stages), "Workflow should have two stages after execution")
}

func TestActionTags(t *testing.T) {
	// Create a workflow with actions that have tags
	workflow := NewWorkflow("tag-workflow", "Tag Workflow", "Testing action tags")

	// Create a stage
	stage := NewStage("tag-stage", "Tag Stage", "Stage with tagged actions")

	// Add actions with tags
	action1 := NewTestActionWithTags("action1", "Action 1", []string{"core", "setup"}, func(ctx *ActionContext) error {
		return nil
	})

	action2 := NewTestActionWithTags("action2", "Action 2", []string{"optional", "cleanup"}, func(ctx *ActionContext) error {
		return nil
	})

	action3 := NewTestActionWithTags("action3", "Action 3", []string{"core", "processing"}, func(ctx *ActionContext) error {
		// Test tag-based filtering
		coreActions := ctx.FindActionsByTag("core")
		assert.Equal(t, 2, len(coreActions), "Should find 2 actions with the 'core' tag")

		optionalActions := ctx.FindActionsByTag("optional")
		assert.Equal(t, 1, len(optionalActions), "Should find 1 action with the 'optional' tag")

		setupAndCore := ctx.FindActionsByTags([]string{"core", "setup"})
		assert.Equal(t, 1, len(setupAndCore), "Should find 1 action with both 'core' and 'setup' tags")

		return nil
	})

	// Add actions to the stage
	stage.AddAction(action1)
	stage.AddAction(action2)
	stage.AddAction(action3)

	// Add stage to workflow
	workflow.AddStage(stage)

	// Execute the workflow
	logger := &TestLogger{t: t}
	err := workflow.Execute(context.Background(), logger)
	assert.NoError(t, err)
}

func TestStageAndWorkflowTags(t *testing.T) {
	// Test creating a workflow with tags
	workflowTags := []string{"deployment", "production"}
	workflow := NewWorkflowWithTags("tagged-workflow", "Tagged Workflow", "Workflow with tags", workflowTags)

	// Verify workflow tags
	assert.Equal(t, 2, len(workflow.Tags))
	assert.True(t, workflow.HasTag("deployment"))
	assert.True(t, workflow.HasTag("production"))
	assert.True(t, workflow.HasAllTags([]string{"deployment", "production"}))
	assert.True(t, workflow.HasAnyTag([]string{"deployment", "staging"}))
	assert.False(t, workflow.HasTag("staging"))

	// Add a tag
	workflow.AddTag("critical")
	assert.Equal(t, 3, len(workflow.Tags))
	assert.True(t, workflow.HasTag("critical"))

	// Test creating stages with tags
	setupStageTags := []string{"setup", "preparation"}
	setupStage := NewStageWithTags("setup-stage", "Setup Stage", "Initial setup stage", setupStageTags)

	deploymentStageTags := []string{"deployment", "execution"}
	deployStage := NewStageWithTags("deploy-stage", "Deployment Stage", "Main deployment stage", deploymentStageTags)

	cleanupStageTags := []string{"cleanup", "post-execution"}
	cleanupStage := NewStageWithTags("cleanup-stage", "Cleanup Stage", "Final cleanup stage", cleanupStageTags)

	// Verify stage tags
	assert.Equal(t, 2, len(setupStage.Tags))
	assert.True(t, setupStage.HasTag("setup"))
	assert.True(t, deployStage.HasTag("deployment"))
	assert.True(t, cleanupStage.HasTag("cleanup"))

	// Add stages to workflow
	workflow.AddStage(setupStage)
	workflow.AddStage(deployStage)
	workflow.AddStage(cleanupStage)

	// Add test actions
	setupCheckerAction := NewTestAction("setup-checker", "Setup Checker", func(ctx *ActionContext) error {
		// Find stages by tag
		setupStages := ctx.FindStagesByTag("setup")
		assert.Equal(t, 1, len(setupStages))
		assert.Equal(t, "setup-stage", setupStages[0].ID)

		// Find stages by any tag
		stagesWithAnyTag := ctx.FindStagesByAnyTag([]string{"setup", "deployment"})
		assert.Equal(t, 2, len(stagesWithAnyTag))

		// Disable stages by tag
		disabledCount := ctx.DisableStagesByTag("cleanup")
		assert.Equal(t, 1, disabledCount)

		// Verify stage is disabled
		assert.False(t, ctx.IsStageEnabled("cleanup-stage"))

		// Re-enable the stage
		enabledCount := ctx.EnableStagesByTag("cleanup")
		assert.Equal(t, 1, enabledCount)

		// Verify stage is enabled again
		assert.True(t, ctx.IsStageEnabled("cleanup-stage"))

		return nil
	})

	setupStage.AddAction(setupCheckerAction)

	// Execute the workflow
	logger := &TestLogger{t: t}
	err := workflow.Execute(context.Background(), logger)
	assert.NoError(t, err)
}
