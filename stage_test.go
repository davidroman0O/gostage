package gostage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/davidroman0O/gostage/store"
	"github.com/stretchr/testify/assert"
)

func TestStageExecution(t *testing.T) {
	// Create a workflow with a stage
	workflow := NewWorkflow("stage-test", "Stage Test", "Testing stage execution")
	stage := NewStage("test-stage", "Test Stage", "A simple test stage")

	// Create a counter to track execution
	counter := 0

	// Add actions to the stage
	stage.AddAction(NewTestAction("action1", "Action 1", func(ctx *ActionContext) error {
		counter++
		return nil
	}))

	stage.AddAction(NewTestAction("action2", "Action 2", func(ctx *ActionContext) error {
		counter++
		return nil
	}))

	// Add the stage to the workflow
	workflow.AddStage(stage)

	// Create a context and logger
	ctx := context.Background()
	logger := &TestLogger{t: t}

	// Execute using a runner
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)

	// Check that there were no errors and both actions executed
	assert.NoError(t, err)
	assert.Equal(t, 2, counter, "Both actions should have executed")
}

func TestStageWithFailingAction(t *testing.T) {
	// Create a workflow with a stage
	workflow := NewWorkflow("failing-stage", "Failing Stage", "Stage with a failing action")
	stage := NewStage("test-stage", "Test Stage", "Stage with a failing action")

	// Create a counter to track execution
	counter := 0

	// Add a successful action
	stage.AddAction(NewTestAction("success-action", "Success Action", func(ctx *ActionContext) error {
		counter++
		return nil
	}))

	// Add a failing action
	expectedErr := errors.New("action failed")
	stage.AddAction(NewTestAction("failing-action", "Failing Action", func(ctx *ActionContext) error {
		counter++
		return expectedErr
	}))

	// Add another action that shouldn't execute due to the failure
	stage.AddAction(NewTestAction("never-executed", "Never Executed", func(ctx *ActionContext) error {
		counter++
		return nil
	}))

	// Add the stage to the workflow
	workflow.AddStage(stage)

	// Create a context and logger
	ctx := context.Background()
	logger := &TestLogger{t: t}

	// Execute using a runner
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)

	// The execution should fail with the expected error
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), expectedErr.Error()))

	// Only the first two actions should have executed
	assert.Equal(t, 2, counter, "Only the first two actions should have executed")
}

func TestStageWithInitialStore(t *testing.T) {
	// Create a workflow
	workflow := NewWorkflow("store-workflow", "Store Workflow", "Testing stage initial store")

	// Create a stage with initial store data
	stage := NewStage("store-stage", "Store Stage", "Stage with initial store")
	err := stage.InitialStore.Put("stage-key", "stage-value")
	assert.NoError(t, err)

	// Add an action that verifies the store contents
	storeChecked := false
	stage.AddAction(NewTestAction("store-checker", "Store Checker", func(ctx *ActionContext) error {
		// Verify that the stage's initial store was merged into the workflow store
		value, err := store.Get[string](ctx.Store, "stage-key")
		if err != nil {
			return err
		}
		if value != "stage-value" {
			return fmt.Errorf("expected stage-value, got %s", value)
		}
		storeChecked = true
		return nil
	}))

	workflow.AddStage(stage)

	// Create a context and logger
	ctx := context.Background()
	logger := &TestLogger{t: t}

	// Execute using a runner
	runner := NewRunner(WithLogger(logger))
	err = runner.Execute(ctx, workflow, logger)

	// Check results
	assert.NoError(t, err)
	assert.True(t, storeChecked, "Store should have been checked")
}

func TestDisabledStage(t *testing.T) {
	// Create a workflow with a disabled stage
	workflow := NewWorkflow("disabled-workflow", "Disabled Stage", "Testing disabled stages")

	// Create two stages
	stage1 := NewStage("stage1", "Stage 1", "First stage")
	stage2 := NewStage("stage2", "Stage 2", "Second stage (will be disabled)")

	// Track execution with counters
	stage1Executed := false
	stage2Executed := false

	// Add actions to track execution
	stage1.AddAction(NewTestAction("action1", "Action 1", func(ctx *ActionContext) error {
		stage1Executed = true
		return nil
	}))

	stage2.AddAction(NewTestAction("action2", "Action 2", func(ctx *ActionContext) error {
		stage2Executed = true
		return nil
	}))

	// Add stages to workflow
	workflow.AddStage(stage1)
	workflow.AddStage(stage2)

	// Disable the second stage
	workflow.DisableStage("stage2")

	// Execute the workflow
	ctx := context.Background()
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)

	// Check results
	assert.NoError(t, err)
	assert.True(t, stage1Executed, "First stage should have executed")
	assert.False(t, stage2Executed, "Second stage should not have executed")
}

func TestEnableDisableStages(t *testing.T) {
	// Create a workflow with multiple stages
	workflow := NewWorkflow("multi-stage", "Multi-Stage", "Testing stage enabling/disabling")

	// Create three stages
	stage1 := NewStage("stage1", "Stage 1", "First stage")
	stage2 := NewStage("stage2", "Stage 2", "Second stage")
	stage3 := NewStage("stage3", "Stage 3", "Third stage")

	// Track execution with counters
	stageExecutions := map[string]bool{}

	// Add actions to track execution
	stage1.AddAction(NewTestAction("action1", "Action 1", func(ctx *ActionContext) error {
		stageExecutions["stage1"] = true
		return nil
	}))

	stage2.AddAction(NewTestAction("action2", "Action 2", func(ctx *ActionContext) error {
		stageExecutions["stage2"] = true
		return nil
	}))

	stage3.AddAction(NewTestAction("action3", "Action 3", func(ctx *ActionContext) error {
		stageExecutions["stage3"] = true
		return nil
	}))

	// Add stages to workflow
	workflow.AddStage(stage1)
	workflow.AddStage(stage2)
	workflow.AddStage(stage3)

	// Disable stage2 and stage3
	workflow.DisableStage("stage2")
	workflow.DisableStage("stage3")

	// Re-enable stage3
	workflow.EnableStage("stage3")

	// Execute the workflow
	ctx := context.Background()
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)

	// Check results
	assert.NoError(t, err)
	assert.True(t, stageExecutions["stage1"], "First stage should have executed")
	assert.False(t, stageExecutions["stage2"], "Second stage should not have executed")
	assert.True(t, stageExecutions["stage3"], "Third stage should have executed")

	// Check stage enabled status
	assert.True(t, workflow.IsStageEnabled("stage1"), "First stage should be enabled")
	assert.False(t, workflow.IsStageEnabled("stage2"), "Second stage should be disabled")
	assert.True(t, workflow.IsStageEnabled("stage3"), "Third stage should be enabled")
}

func TestEnableAllStages(t *testing.T) {
	// Create a workflow with multiple stages
	workflow := NewWorkflow("enable-all", "Enable All", "Testing enableAllStages")

	// Create three stages
	stage1 := NewStage("stage1", "Stage 1", "First stage")
	stage2 := NewStage("stage2", "Stage 2", "Second stage")
	stage3 := NewStage("stage3", "Stage 3", "Third stage")

	// Add simple actions to make the stages valid
	stage1.AddAction(NewTestAction("action1", "Action 1", nil))
	stage2.AddAction(NewTestAction("action2", "Action 2", nil))
	stage3.AddAction(NewTestAction("action3", "Action 3", nil))

	// Add stages to workflow
	workflow.AddStage(stage1)
	workflow.AddStage(stage2)
	workflow.AddStage(stage3)

	// Disable all stages
	workflow.DisableStage("stage1")
	workflow.DisableStage("stage2")
	workflow.DisableStage("stage3")

	// Verify all stages are disabled
	assert.False(t, workflow.IsStageEnabled("stage1"))
	assert.False(t, workflow.IsStageEnabled("stage2"))
	assert.False(t, workflow.IsStageEnabled("stage3"))

	// Enable all stages
	workflow.EnableAllStages()

	// Verify all stages are now enabled
	assert.True(t, workflow.IsStageEnabled("stage1"))
	assert.True(t, workflow.IsStageEnabled("stage2"))
	assert.True(t, workflow.IsStageEnabled("stage3"))

	// Execute the workflow to make sure all stages run
	ctx := context.Background()
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)
	assert.NoError(t, err)
}

func TestStageTagFiltering(t *testing.T) {
	// Create a workflow with tagged stages
	workflow := NewWorkflow("tag-filter", "Tag Filter", "Testing stage tag filtering")

	// Create stages with different tags
	stage1 := NewStageWithTags("stage1", "Stage 1", "First stage", []string{"setup", "common"})
	stage2 := NewStageWithTags("stage2", "Stage 2", "Second stage", []string{"main", "common"})
	stage3 := NewStageWithTags("stage3", "Stage 3", "Third stage", []string{"cleanup", "optional"})

	// Add a dummy action to each stage
	stage1.AddAction(NewTestAction("action1", "Action 1", nil))
	stage2.AddAction(NewTestAction("action2", "Action 2", nil))
	stage3.AddAction(NewTestAction("action3", "Action 3", nil))

	// Add stages to workflow
	workflow.AddStage(stage1)
	workflow.AddStage(stage2)
	workflow.AddStage(stage3)

	// Execute the workflow
	ctx := context.Background()
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)
	assert.NoError(t, err)

	// Test stage retrieval by tag
	setupStages := workflow.ListStagesByTag("setup")
	assert.Equal(t, 1, len(setupStages))
	assert.Equal(t, "stage1", setupStages[0].ID)

	// Test retrieve stages with common tag
	commonStages := workflow.ListStagesByTag("common")
	assert.Equal(t, 2, len(commonStages))

	// Test retrieve setup or main stages
	setupOrMainStages := workflow.ListStagesByTag("setup")
	setupOrMainStages = append(setupOrMainStages, workflow.ListStagesByTag("main")...)
	assert.Equal(t, 2, len(setupOrMainStages))
	assert.Contains(t, []string{setupOrMainStages[0].ID, setupOrMainStages[1].ID}, "stage1")
	assert.Contains(t, []string{setupOrMainStages[0].ID, setupOrMainStages[1].ID}, "stage2")
}

func TestStageActionTagFiltering(t *testing.T) {
	// Create a workflow with a stage that has tagged actions
	workflow := NewWorkflow("tag-filter", "Tag Filter", "Testing action tag filtering")
	stage := NewStage("test-stage", "Test Stage", "Stage with tagged actions")

	// Add actions with different tags
	action1 := NewTestActionWithTags("action1", "Action 1", []string{"tag1", "common"}, nil)
	action2 := NewTestActionWithTags("action2", "Action 2", []string{"tag2", "common"}, nil)
	action3 := NewTestActionWithTags("action3", "Action 3", []string{"tag3"}, nil)

	stage.AddAction(action1)
	stage.AddAction(action2)
	stage.AddAction(action3)
	workflow.AddStage(stage)

	// Create a context for the test
	ctx := context.Background()
	logger := &TestLogger{t: t}

	// Execute the workflow
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)
	assert.NoError(t, err)

	// Test tag filtering
	// First we need to create an ActionContext since we don't have access to it directly
	actionCtx := &ActionContext{
		GoContext: ctx,
		Workflow:  workflow,
		Stage:     stage,
		Store:     workflow.Store,
		Logger:    logger,
	}

	// Test finding by tag
	byTag1 := actionCtx.FindActionsByTag("tag1")
	assert.Equal(t, 1, len(byTag1))
	assert.Equal(t, "action1", byTag1[0].Name())

	// Test finding by common tag
	byCommon := actionCtx.FindActionsByTag("common")
	assert.Equal(t, 2, len(byCommon))

	// Test finding by multiple tags
	byTagsCommonAndTag1 := actionCtx.FindActionsByTags([]string{"common", "tag1"})
	assert.Equal(t, 1, len(byTagsCommonAndTag1))
	assert.Equal(t, "action1", byTagsCommonAndTag1[0].Name())

	// Test finding by any tag
	byAnyTag := actionCtx.FindActionsByAnyTag([]string{"tag1", "tag3"})
	assert.Equal(t, 2, len(byAnyTag))
}

func TestStageDynamicActions(t *testing.T) {
	// Create a workflow with a stage that adds actions dynamically
	workflow := NewWorkflow("dynamic-workflow", "Dynamic Workflow", "Workflow with dynamic actions")
	stage := NewStage("dynamic-stage", "Dynamic Stage", "Stage with dynamic actions")

	// Track action execution
	executed := map[string]bool{}

	// Add an action that will add more actions
	stage.AddAction(NewTestAction("generator", "Generator Action", func(ctx *ActionContext) error {
		executed["generator"] = true

		// Add two dynamic actions
		ctx.AddDynamicAction(NewTestAction("dynamic1", "Dynamic 1", func(innerCtx *ActionContext) error {
			executed["dynamic1"] = true
			return nil
		}))

		ctx.AddDynamicAction(NewTestAction("dynamic2", "Dynamic 2", func(innerCtx *ActionContext) error {
			executed["dynamic2"] = true
			return nil
		}))

		return nil
	}))

	// Add the stage to the workflow
	workflow.AddStage(stage)

	// Execute the workflow
	ctx := context.Background()
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)
	assert.NoError(t, err)

	// Verify all actions were executed
	assert.True(t, executed["generator"], "Generator action should have executed")
	assert.True(t, executed["dynamic1"], "Dynamic1 action should have executed")
	assert.True(t, executed["dynamic2"], "Dynamic2 action should have executed")
}

func TestStageActionEnableDisable(t *testing.T) {
	// Create a stage with actions where some are dynamically disabled
	stage := NewStage("control-stage", "Control Stage", "Stage with action enabling/disabling")

	// Execution tracking
	executionCount := make(map[string]int)

	// Add actions with different behaviors
	controlAction := &TestAction{
		BaseAction: NewBaseAction("control", "Control Action"),
		executeFunc: func(ctx *ActionContext) error {
			executionCount["control"]++

			// Disable the second action
			ctx.DisableAction("target")
			return nil
		},
	}

	targetAction := &TestAction{
		BaseAction: NewBaseAction("target", "Target Action"),
		executeFunc: func(ctx *ActionContext) error {
			executionCount["target"]++
			return nil
		},
	}

	finalAction := &TestAction{
		BaseAction: NewBaseAction("final", "Final Action"),
		executeFunc: func(ctx *ActionContext) error {
			executionCount["final"]++
			return nil
		},
	}

	// Add actions to the stage
	stage.AddAction(controlAction)
	stage.AddAction(targetAction)
	stage.AddAction(finalAction)

	// Create a workflow and execute it
	workflow := NewWorkflow("control-workflow", "Control Workflow", "Workflow with action control")
	workflow.AddStage(stage)

	ctx := context.Background()
	logger := &TestLogger{t: t}

	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)
	assert.NoError(t, err)

	// Verify execution counts
	assert.Equal(t, 1, executionCount["control"])
	assert.Equal(t, 0, executionCount["target"]) // Should be disabled and not executed
	assert.Equal(t, 1, executionCount["final"])
}

func TestStageActionForEach(t *testing.T) {
	// Create a workflow
	workflow := NewWorkflow("foreach-workflow", "ForEach Workflow", "Testing action for-each functionality")
	stage := NewStage("foreach-stage", "ForEach Stage", "Stage with for-each action")

	// Define items to process
	items := []string{"item1", "item2", "item3"}
	workflow.Store.Put("items", items)

	// Track processed items
	processedItems := []string{}

	// Add a for-each action
	stage.AddAction(NewTestAction("foreach", "ForEach Action", func(ctx *ActionContext) error {
		// Get items from the store
		items, err := store.Get[[]string](ctx.Store, "items")
		if err != nil {
			return err
		}

		// Process each item
		for _, item := range items {
			processedItems = append(processedItems, item)
		}

		return nil
	}))

	// Add the stage to the workflow
	workflow.AddStage(stage)

	// Execute the workflow
	ctx := context.Background()
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)
	assert.NoError(t, err)

	// Verify all items were processed
	assert.Equal(t, 3, len(processedItems))
	assert.Equal(t, "item1", processedItems[0])
	assert.Equal(t, "item2", processedItems[1])
	assert.Equal(t, "item3", processedItems[2])
}

func TestStageDynamicGeneration(t *testing.T) {
	// Create a workflow that generates another stage dynamically
	workflow := NewWorkflow("dynamic-stage-gen", "Dynamic Stage Generation", "Workflow that creates stages dynamically")

	// Add initial stage that will add a new stage
	generatorStage := NewStage("generator", "Generator Stage", "Stage that adds another stage")

	// Track if stages are executed
	generatorRan := false
	dynamicRan := false

	// The generator action adds a new stage
	generatorStage.AddAction(NewTestAction("generator-action", "Generator Action", func(ctx *ActionContext) error {
		generatorRan = true

		// Create a new dynamic stage
		dynamicStage := NewStage("dynamic", "Dynamic Stage", "Dynamically generated stage")

		// Add an action to the dynamic stage
		dynamicStage.AddAction(NewTestAction("dynamic-action", "Dynamic Action", func(dynCtx *ActionContext) error {
			dynamicRan = true
			return nil
		}))

		// Add the dynamic stage to the workflow
		ctx.AddDynamicStage(dynamicStage)

		return nil
	}))

	// Add the generator stage to the workflow
	workflow.AddStage(generatorStage)

	// Execute the workflow
	ctx := context.Background()
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)
	assert.NoError(t, err)

	// Verify both stages ran
	assert.True(t, generatorRan, "Generator stage should have run")
	assert.True(t, dynamicRan, "Dynamic stage should have run")

	// Verify the dynamic stage was added to the workflow
	assert.Equal(t, 2, len(workflow.Stages), "Workflow should have 2 stages")
	assert.Equal(t, "dynamic", workflow.Stages[1].ID)
}

// StageContext tests

func TestStageContext(t *testing.T) {
	// Create a workflow with a stage that uses context
	workflow := NewWorkflow("context-workflow", "Context Workflow", "Workflow for context testing")
	stage := NewStage("context-stage", "Context Stage", "Stage for context testing")

	// Add an action that checks the context
	contextChecked := false
	stage.AddAction(NewTestAction("context-check", "Context Check", func(ctx *ActionContext) error {
		// Check that the context has the expected values
		assert.Equal(t, workflow, ctx.Workflow)
		assert.Equal(t, stage, ctx.Stage)
		assert.NotNil(t, ctx.Store)
		contextChecked = true
		return nil
	}))

	workflow.AddStage(stage)

	// Execute the workflow
	ctx := context.Background()
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)
	assert.NoError(t, err)
	assert.True(t, contextChecked)
}

// Custom stage test

type CustomStage struct {
	*Stage
	setupRun    bool
	cleanupRun  bool
	executeFunc func(ctx context.Context, workflow *Workflow, logger Logger) error
}

func NewCustomStage(id, name, description string) *CustomStage {
	return &CustomStage{
		Stage:      NewStage(id, name, description),
		setupRun:   false,
		cleanupRun: false,
	}
}

func (s *CustomStage) Setup() {
	s.setupRun = true
}

func (s *CustomStage) Cleanup() {
	s.cleanupRun = true
}

func (s *CustomStage) Execute(ctx context.Context, workflow *Workflow, logger Logger) error {
	if s.executeFunc != nil {
		return s.executeFunc(ctx, workflow, logger)
	}
	return s.Stage.Execute(ctx, workflow, logger)
}

func TestCustomStage(t *testing.T) {
	// Create a workflow
	workflow := NewWorkflow("custom-workflow", "Custom Workflow", "Workflow with custom stage")

	// Create a custom stage
	customStage := NewCustomStage("custom-stage", "Custom Stage", "A custom stage with setup/cleanup")
	customStage.executeFunc = func(ctx context.Context, workflow *Workflow, logger Logger) error {
		// Call setup
		customStage.Setup()

		// Execute the underlying stage
		err := customStage.Stage.Execute(ctx, workflow, logger)

		// Call cleanup
		customStage.Cleanup()

		return err
	}

	// Add an action to the stage
	customStage.AddAction(NewTestAction("test-action", "Test Action", func(ctx *ActionContext) error {
		// Verify setup has run but cleanup hasn't yet
		assert.True(t, customStage.setupRun)
		assert.False(t, customStage.cleanupRun)
		return nil
	}))

	// Define the logger
	logger := &TestLogger{t: t}

	// Create a custom middleware to run before and after stage execution
	runner := NewRunner(WithLogger(logger))
	runner.Use(func(next RunnerFunc) RunnerFunc {
		return func(ctx context.Context, workflow *Workflow, logger Logger) error {
			// Setup before execution
			customStage.Setup()

			// Execute the workflow
			err := next(ctx, workflow, logger)

			// Cleanup after execution
			customStage.Cleanup()

			return err
		}
	})

	// Add the stage to the workflow
	workflow.AddStage(customStage.Stage)

	// Execute the workflow
	err := runner.Execute(context.Background(), workflow, logger)
	assert.NoError(t, err)

	// Verify both setup and cleanup have run
	assert.True(t, customStage.setupRun)
	assert.True(t, customStage.cleanupRun)
}

// Pipeline test

func TestStagePipeline(t *testing.T) {
	// Create a workflow for a simple data pipeline
	workflow := NewWorkflow("pipeline", "Pipeline", "Workflow for data pipeline testing")

	// Create stages for the pipeline
	inputStage := NewStage("input", "Input", "Input stage")
	processStage := NewStage("process", "Process", "Processing stage")
	outputStage := NewStage("output", "Output", "Output stage")

	// Define the input data in the workflow store
	workflow.Store.Put("input", []string{"item1", "item2", "item3"})

	// First stage: read from input
	inputStage.AddAction(NewTestAction("read-input", "Read Input", func(ctx *ActionContext) error {
		// Read input from the store
		input, err := store.Get[[]string](ctx.Store, "input")
		if err != nil {
			return err
		}

		// Store for the next stage
		ctx.Store.Put("items", input)
		return nil
	}))

	// Second stage: process the data
	processStage.AddAction(NewTestAction("process-items", "Process Items", func(ctx *ActionContext) error {
		// Get items from previous stage
		items, err := store.Get[[]string](ctx.Store, "items")
		if err != nil {
			return err
		}

		// Process each item (just uppercase in this test)
		processed := make([]string, len(items))
		for i, item := range items {
			processed[i] = strings.ToUpper(item)
		}

		// Store for the next stage
		ctx.Store.Put("processed", processed)
		return nil
	}))

	// Third stage: output the results
	outputStage.AddAction(NewTestAction("write-output", "Write Output", func(ctx *ActionContext) error {
		// Get processed items
		processed, err := store.Get[[]string](ctx.Store, "processed")
		if err != nil {
			return err
		}

		// Store the final result
		ctx.Store.Put("output", processed)
		return nil
	}))

	// Add stages to the workflow
	workflow.AddStage(inputStage)
	workflow.AddStage(processStage)
	workflow.AddStage(outputStage)

	// Execute the workflow
	ctx := context.Background()
	logger := &TestLogger{t: t}
	runner := NewRunner(WithLogger(logger))
	err := runner.Execute(ctx, workflow, logger)
	assert.NoError(t, err)

	// Verify the pipeline worked as expected
	output, err := store.Get[[]string](workflow.Store, "output")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(output))
	assert.Equal(t, "ITEM1", output[0])
	assert.Equal(t, "ITEM2", output[1])
	assert.Equal(t, "ITEM3", output[2])
}
