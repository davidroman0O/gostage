package gostage

import (
	"context"
	"testing"

	"github.com/davidroman0O/gostage/store"
	"github.com/stretchr/testify/assert"
)

func TestStageActionExecution(t *testing.T) {
	// Create a stage
	stage := NewStage("test-stage", "Test Stage", "A test stage")

	// Track action execution
	executionOrder := []string{}

	// Create some test actions with different behaviors
	successAction := &TestAction{
		BaseAction: NewBaseAction("success-action", "Success Action"),
		executeFunc: func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "success-action")
			return nil
		},
	}

	failAction := &TestAction{
		BaseAction: NewBaseAction("fail-action", "Failing Action"),
		executeFunc: func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "fail-action")
			return &CustomError{
				Code:    "TEST_FAILURE",
				Message: "Deliberate test failure",
			}
		},
	}

	// Add actions to the stage
	stage.AddAction(successAction)
	stage.AddAction(failAction)

	// Create a workflow and context for execution
	workflow := NewWorkflow("test-workflow", "Test Workflow", "A test workflow")
	workflow.AddStage(stage)

	ctx := context.Background()
	logger := &TestLogger{t: t}

	// Execute the workflow - should fail at the failing action
	err := workflow.Execute(ctx, logger)
	assert.Error(t, err)

	// Verify execution order - both actions should have executed
	assert.Len(t, executionOrder, 2)
	assert.Equal(t, "success-action", executionOrder[0])
	assert.Equal(t, "fail-action", executionOrder[1])
}

func TestStageInitialStore(t *testing.T) {
	// Create a stage with initial store data
	stage := NewStage("test-stage", "Test Stage", "A test stage")

	// Add some data to the stage's initial store
	err := stage.InitialStore.Put("key1", "value1")
	assert.NoError(t, err)

	err = stage.InitialStore.Put("key2", 123)
	assert.NoError(t, err)

	// Create a test action that verifies the store data
	verifyAction := &TestAction{
		BaseAction: NewBaseAction("verify-action", "Verify Action"),
		executeFunc: func(ctx *ActionContext) error {
			// Verify key1
			val1, err := store.Get[string](ctx.Store, "key1")
			if err != nil {
				return err
			}
			if val1 != "value1" {
				return &CustomError{
					Code:    "STORE_ERROR",
					Message: "key1 has wrong value",
				}
			}

			// Verify key2
			val2, err := store.Get[int](ctx.Store, "key2")
			if err != nil {
				return err
			}
			if val2 != 123 {
				return &CustomError{
					Code:    "STORE_ERROR",
					Message: "key2 has wrong value",
				}
			}

			return nil
		},
	}

	stage.AddAction(verifyAction)

	// Create a workflow and execute it
	workflow := NewWorkflow("test-workflow", "Test Workflow", "A test workflow")
	workflow.AddStage(stage)

	ctx := context.Background()
	logger := &TestLogger{t: t}

	err = workflow.Execute(ctx, logger)
	assert.NoError(t, err)
}

func TestStageTagFiltering(t *testing.T) {
	// Create a workflow with multiple stages having different tags
	workflow := NewWorkflow("tag-workflow", "Tag Workflow", "A workflow for testing tag filtering")

	// Create stages with different tag combinations
	stage1 := NewStageWithTags("stage1", "Stage 1", "First stage", []string{"setup", "critical"})
	stage2 := NewStageWithTags("stage2", "Stage 2", "Second stage", []string{"main", "critical"})
	stage3 := NewStageWithTags("stage3", "Stage 3", "Third stage", []string{"cleanup", "optional"})

	// Add stages to the workflow
	workflow.AddStage(stage1)
	workflow.AddStage(stage2)
	workflow.AddStage(stage3)

	// Create a dummy action context
	context := &ActionContext{
		Workflow: workflow,
	}

	// Test filtering stages by a single tag
	criticalStages := context.FindStagesByTag("critical")
	assert.Len(t, criticalStages, 2)
	assert.Equal(t, "stage1", criticalStages[0].ID)
	assert.Equal(t, "stage2", criticalStages[1].ID)

	// Test filtering by another tag
	optionalStages := context.FindStagesByTag("optional")
	assert.Len(t, optionalStages, 1)
	assert.Equal(t, "stage3", optionalStages[0].ID)

	// Test filtering by multiple tags (ALL tags must match)
	setupCriticalStages := context.FindStagesByAllTags([]string{"setup", "critical"})
	assert.Len(t, setupCriticalStages, 1)
	assert.Equal(t, "stage1", setupCriticalStages[0].ID)

	// Test filtering by ANY of multiple tags
	setupOrMainStages := context.FindStagesByAnyTag([]string{"setup", "main"})
	assert.Len(t, setupOrMainStages, 2)
	assert.Contains(t, []string{setupOrMainStages[0].ID, setupOrMainStages[1].ID}, "stage1")
	assert.Contains(t, []string{setupOrMainStages[0].ID, setupOrMainStages[1].ID}, "stage2")
}

func TestStageEnableDisable(t *testing.T) {
	// Create a workflow with multiple stages
	workflow := NewWorkflow("enable-disable", "Enable/Disable", "Testing stage enabling/disabling")

	// Create some stages
	stage1 := NewStage("stage1", "Stage 1", "First stage")
	stage2 := NewStage("stage2", "Stage 2", "Second stage")
	stage3 := NewStage("stage3", "Stage 3", "Third stage")

	// Create a counter to track execution
	executionCount := make(map[string]int)

	// Add a test action to each stage
	stage1.AddAction(&TestAction{
		BaseAction: NewBaseAction("action1", "Action 1"),
		executeFunc: func(ctx *ActionContext) error {
			executionCount["stage1"]++
			// Disable the third stage
			ctx.DisableStage("stage3")
			return nil
		},
	})

	stage2.AddAction(&TestAction{
		BaseAction: NewBaseAction("action2", "Action 2"),
		executeFunc: func(ctx *ActionContext) error {
			executionCount["stage2"]++
			return nil
		},
	})

	stage3.AddAction(&TestAction{
		BaseAction: NewBaseAction("action3", "Action 3"),
		executeFunc: func(ctx *ActionContext) error {
			executionCount["stage3"]++
			return nil
		},
	})

	// Add stages to the workflow
	workflow.AddStage(stage1)
	workflow.AddStage(stage2)
	workflow.AddStage(stage3)

	// Execute the workflow
	ctx := context.Background()
	logger := &TestLogger{t: t}

	err := workflow.Execute(ctx, logger)
	assert.NoError(t, err)

	// Check execution counts - stage3 should not have executed
	assert.Equal(t, 1, executionCount["stage1"])
	assert.Equal(t, 1, executionCount["stage2"])
	assert.Equal(t, 0, executionCount["stage3"])
}

func TestStageActionTagFiltering(t *testing.T) {
	// Create a stage with actions having different tags
	stage := NewStage("filter-stage", "Filter Stage", "Stage for action filtering tests")

	// Add actions with different tag combinations
	action1 := NewTestActionWithTags("action1", "Action 1", []string{"init", "required"},
		func(ctx *ActionContext) error { return nil })

	action2 := NewTestActionWithTags("action2", "Action 2", []string{"process", "required"},
		func(ctx *ActionContext) error { return nil })

	action3 := NewTestActionWithTags("action3", "Action 3", []string{"cleanup", "optional"},
		func(ctx *ActionContext) error { return nil })

	stage.AddAction(action1)
	stage.AddAction(action2)
	stage.AddAction(action3)

	// Create workflow and dummy context
	workflow := NewWorkflow("tag-workflow", "Tag Workflow", "Workflow for tag filtering")
	workflow.AddStage(stage)

	actionContext := &ActionContext{
		Workflow: workflow,
		Stage:    stage,
	}

	// Test filtering by a single tag
	requiredActions := actionContext.FindActionsByTag("required")
	assert.Len(t, requiredActions, 2)
	assert.Equal(t, "action1", requiredActions[0].Name())
	assert.Equal(t, "action2", requiredActions[1].Name())

	// Test filtering by another tag
	optionalActions := actionContext.FindActionsByTag("optional")
	assert.Len(t, optionalActions, 1)
	assert.Equal(t, "action3", optionalActions[0].Name())

	// Test filtering by multiple tags (any tag matches)
	processingActions := actionContext.FindActionsByAnyTag([]string{"process", "cleanup"})
	assert.Len(t, processingActions, 2)
	assert.Contains(t, []string{processingActions[0].Name(), processingActions[1].Name()}, "action2")
	assert.Contains(t, []string{processingActions[0].Name(), processingActions[1].Name()}, "action3")
}

func TestStageDynamicActions(t *testing.T) {
	// Create a stage with an action that dynamically adds more actions
	stage := NewStage("dynamic-stage", "Dynamic Stage", "Stage with dynamic actions")

	// Execution tracking
	executionOrder := []string{}

	// Add a generator action that will add more actions
	generatorAction := &TestAction{
		BaseAction: NewBaseAction("generator", "Generator Action"),
		executeFunc: func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "generator")

			// Add two dynamic actions
			ctx.AddDynamicAction(&TestAction{
				BaseAction: NewBaseAction("dynamic1", "Dynamic Action 1"),
				executeFunc: func(innerCtx *ActionContext) error {
					executionOrder = append(executionOrder, "dynamic1")
					return nil
				},
			})

			ctx.AddDynamicAction(&TestAction{
				BaseAction: NewBaseAction("dynamic2", "Dynamic Action 2"),
				executeFunc: func(innerCtx *ActionContext) error {
					executionOrder = append(executionOrder, "dynamic2")
					return nil
				},
			})

			return nil
		},
	}

	// Add a final action to verify execution order
	finalAction := &TestAction{
		BaseAction: NewBaseAction("final", "Final Action"),
		executeFunc: func(ctx *ActionContext) error {
			executionOrder = append(executionOrder, "final")
			return nil
		},
	}

	// Add actions to the stage
	stage.AddAction(generatorAction)
	stage.AddAction(finalAction)

	// Create a workflow and execute it
	workflow := NewWorkflow("dynamic-workflow", "Dynamic Workflow", "Workflow with dynamic actions")
	workflow.AddStage(stage)

	ctx := context.Background()
	logger := &TestLogger{t: t}

	err := workflow.Execute(ctx, logger)
	assert.NoError(t, err)

	// Verify execution order
	assert.Len(t, executionOrder, 4)
	assert.Equal(t, "generator", executionOrder[0])
	assert.Equal(t, "dynamic1", executionOrder[1])
	assert.Equal(t, "dynamic2", executionOrder[2])
	assert.Equal(t, "final", executionOrder[3])
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

	err := workflow.Execute(ctx, logger)
	assert.NoError(t, err)

	// Verify execution counts
	assert.Equal(t, 1, executionCount["control"])
	assert.Equal(t, 0, executionCount["target"]) // Should be disabled and not executed
	assert.Equal(t, 1, executionCount["final"])
}

func TestStageActionForEach(t *testing.T) {
	// Create a stage for testing ForEach functionality on a collection
	stage := NewStage("foreach-stage", "ForEach Stage", "Stage testing ForEach action functionality")

	// Define test data
	testItems := []string{"item1", "item2", "item3"}

	// Add data to the stage's initial store
	stage.InitialStore.Put("items", testItems)

	// Track processed items
	processedItems := []string{}

	// Add a forEach action that processes each item
	forEachAction := &TestAction{
		BaseAction: NewBaseAction("for-each", "ForEach Action"),
		executeFunc: func(ctx *ActionContext) error {
			// Get the items from the store
			items, err := store.Get[[]string](ctx.Store, "items")
			if err != nil {
				return err
			}

			// Process each item
			for _, item := range items {
				processedItems = append(processedItems, item)
			}

			return nil
		},
	}

	stage.AddAction(forEachAction)

	// Create a workflow and execute it
	workflow := NewWorkflow("foreach-workflow", "ForEach Workflow", "Workflow testing ForEach")
	workflow.AddStage(stage)

	ctx := context.Background()
	logger := &TestLogger{t: t}

	err := workflow.Execute(ctx, logger)
	assert.NoError(t, err)

	// Verify all items were processed
	assert.Len(t, processedItems, 3)
	assert.Equal(t, "item1", processedItems[0])
	assert.Equal(t, "item2", processedItems[1])
	assert.Equal(t, "item3", processedItems[2])
}
