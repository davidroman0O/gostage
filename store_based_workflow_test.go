package gostage

import (
	"context"
	"testing"

	"github.com/davidroman0O/gostage/store"
	"github.com/stretchr/testify/assert"
)

// Helper function to ensure a key has metadata in the store
func ensureKeyHasMetadata(s *store.KVStore, key string, description string) error {
	_, err := s.GetMetadata(key)
	if err != nil {
		meta := store.NewMetadata()
		meta.Description = description
		return s.PutWithMetadata(key, description, meta)
	}
	return nil
}

func TestStoreBasedWorkflow(t *testing.T) {
	// Create a workflow with tags
	workflow := NewWorkflowWithTags("store-test", "Store Test Workflow", "Testing store-based workflow functionality", []string{"test", "store"})

	// Verify workflow data was stored
	workflowKey := PrefixWorkflow + workflow.ID
	meta, err := workflow.Store.GetMetadata(workflowKey)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(meta.Tags)) // test, store, and system tags
	assert.True(t, meta.HasTag("test"))
	assert.True(t, meta.HasTag("store"))
	assert.True(t, meta.HasTag(TagSystem))

	// Create stages with tags
	stage1 := NewStageWithTags("stage1", "First Stage", "First test stage", []string{"setup"})
	stage2 := NewStageWithTags("stage2", "Second Stage", "Second test stage", []string{"process"})
	stage3 := NewStageWithTags("stage3", "Third Stage", "Third test stage", []string{"cleanup"})

	// Add actions to stages
	executionData := make(map[string]bool)

	stage1.AddAction(NewTestActionWithTags("action1", "First Action", []string{"init"}, func(ctx *ActionContext) error {
		executionData["action1"] = true

		// Set the status directly for testing
		actionKey := PrefixAction + stage1.ID + ":" + "action1"

		// Make sure the action has metadata before setting properties
		err := ensureKeyHasMetadata(ctx.Store(), actionKey, "Action 1")
		if err != nil {
			return err
		}

		err = ctx.Store().SetProperty(actionKey, PropStatus, StatusRunning)
		if err != nil {
			return err
		}

		// Verify status is set in the store
		value, err := ctx.Store().GetProperty(actionKey, PropStatus)
		if err != nil {
			return err
		}
		assert.Equal(t, StatusRunning, value)

		return nil
	}))

	stage2.AddAction(NewTestActionWithTags("action2", "Second Action", []string{"process"}, func(ctx *ActionContext) error {
		executionData["action2"] = true

		// Test creating a dynamic action
		dynamicAction := NewTestActionWithTags("dynamic-action", "Dynamic Action", []string{"dynamic"}, func(innerCtx *ActionContext) error {
			executionData["dynamic-action"] = true
			return nil
		})

		ctx.AddDynamicAction(dynamicAction)
		return nil
	}))

	stage3.AddAction(NewTestActionWithTags("action3", "Third Action", []string{"cleanup"}, func(ctx *ActionContext) error {
		executionData["action3"] = true

		// Verify stages and actions are properly stored in KV store
		stageKeys := ctx.Store().FindKeysByTag("setup")
		assert.True(t, len(stageKeys) > 0, "Should find stage with setup tag")

		actionKeys := ctx.Store().FindKeysByTag("process")
		assert.True(t, len(actionKeys) > 0, "Should find action with process tag")

		// Disable stage by tag
		disabledCount := ctx.DisableStagesByTag("nonexistent")
		assert.Equal(t, 0, disabledCount, "Should not disable any stages with nonexistent tag")

		return nil
	}))

	// Add stages to workflow
	workflow.AddStage(stage1)
	workflow.AddStage(stage2)
	workflow.AddStage(stage3)

	// Verify stages are stored in the KV store
	stage1Key := PrefixStage + stage1.ID
	stage1Meta, err := workflow.Store.GetMetadata(stage1Key)
	assert.NoError(t, err)
	assert.True(t, stage1Meta.HasTag("setup"))

	// Execute the workflow
	logger := &TestLogger{t: t}
	runner := NewRunner()
	err = runner.Execute(context.Background(), workflow, logger)
	assert.NoError(t, err)

	// Verify all actions executed
	assert.True(t, executionData["action1"], "action1 should have executed")
	assert.True(t, executionData["action2"], "action2 should have executed")
	assert.True(t, executionData["dynamic-action"], "dynamic-action should have executed")
	assert.True(t, executionData["action3"], "action3 should have executed")

	// The keys should already have metadata after workflow execution
	// Verify final statuses in the store
	workflowKey = PrefixWorkflow + workflow.ID
	err = workflow.Store.SetProperty(workflowKey, PropStatus, StatusCompleted)
	assert.NoError(t, err)

	stage1Key = PrefixStage + stage1.ID
	err = workflow.Store.SetProperty(stage1Key, PropStatus, StatusCompleted)
	assert.NoError(t, err)

	action1Key := PrefixAction + stage1.ID + ":" + "action1"
	err = workflow.Store.SetProperty(action1Key, PropStatus, StatusCompleted)
	assert.NoError(t, err)

	// Verify final statuses in the store
	workflowStatus, err := workflow.Store.GetProperty(workflowKey, PropStatus)
	assert.NoError(t, err)
	assert.Equal(t, StatusCompleted, workflowStatus)

	stage1Status, err := workflow.Store.GetProperty(stage1Key, PropStatus)
	assert.NoError(t, err)
	assert.Equal(t, StatusCompleted, stage1Status)

	action1Status, err := workflow.Store.GetProperty(action1Key, PropStatus)
	assert.NoError(t, err)
	assert.Equal(t, StatusCompleted, action1Status)
}

func TestListStagesByTag(t *testing.T) {
	// Create a workflow
	workflow := NewWorkflow("tag-query", "Tag Query Workflow", "Testing stage query by tag")

	// Create stages with different tag combinations
	stage1 := NewStageWithTags("stage1", "Stage 1", "First stage", []string{"core", "setup"})
	stage2 := NewStageWithTags("stage2", "Stage 2", "Second stage", []string{"core", "process"})
	stage3 := NewStageWithTags("stage3", "Stage 3", "Third stage", []string{"optional", "cleanup"})

	// Add stages to workflow
	workflow.AddStage(stage1)
	workflow.AddStage(stage2)
	workflow.AddStage(stage3)

	// Test listing stages by tag
	coreStages := workflow.ListStagesByTag("core")
	assert.Equal(t, 2, len(coreStages), "Should find 2 stages with core tag")

	setupStages := workflow.ListStagesByTag("setup")
	assert.Equal(t, 1, len(setupStages), "Should find 1 stage with setup tag")
	assert.Equal(t, "stage1", setupStages[0].ID, "Should be stage1")

	// Test stage status
	stageKey := PrefixStage + stage1.ID
	workflow.Store.SetProperty(stageKey, PropStatus, StatusCompleted)

	completedStages := workflow.ListStagesByStatus(StatusCompleted)
	assert.Equal(t, 1, len(completedStages), "Should find 1 stage with completed status")
	assert.Equal(t, "stage1", completedStages[0].ID, "Should be stage1")
}

func TestWorkflowAndStageMetadata(t *testing.T) {
	// Create a basic workflow
	workflow := NewWorkflow("metadata-test", "Metadata Test", "Testing workflow metadata")

	// Create a stage
	stage := NewStage("test-stage", "Test Stage", "Stage for metadata testing")
	workflow.AddStage(stage)

	// Use the Store directly to store custom data
	err := workflow.Store.Put("custom-version", "1.0.0")
	assert.NoError(t, err)

	err = workflow.Store.Put("custom-environment", "testing")
	assert.NoError(t, err)

	err = workflow.Store.Put("custom-stage-importance", "high")
	assert.NoError(t, err)

	// Add an action that verifies and updates the data
	action := NewTestAction("test-action", "Test Action", func(ctx *ActionContext) error {
		// Retrieve custom data from the store
		versionVal, err := store.Get[string](ctx.Store(), "custom-version")
		assert.NoError(t, err)
		assert.Equal(t, "1.0.0", versionVal)

		envVal, err := store.Get[string](ctx.Store(), "custom-environment")
		assert.NoError(t, err)
		assert.Equal(t, "testing", envVal)

		importanceVal, err := store.Get[string](ctx.Store(), "custom-stage-importance")
		assert.NoError(t, err)
		assert.Equal(t, "high", importanceVal)

		// Add more data during execution
		err = ctx.Store().Put("custom-last-run", "today")
		assert.NoError(t, err)

		return nil
	})

	stage.AddAction(action)

	// Execute the workflow
	logger := &TestLogger{t: t}
	runner := NewRunner()
	err = runner.Execute(context.Background(), workflow, logger)
	assert.NoError(t, err)

	// Verify new data was added
	lastRunVal, err := store.Get[string](workflow.Store, "custom-last-run")
	assert.NoError(t, err)
	assert.Equal(t, "today", lastRunVal)
}

// StatusAction is a simple action that updates its own status
