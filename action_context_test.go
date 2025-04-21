package gostage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// NOTE: TestLogger and TestAction/NewTestAction* helpers are defined in workflow_test.go

func TestExtendedActionFiltering(t *testing.T) {
	// Create a workflow with various actions to test the extended filtering capabilities
	workflow := NewWorkflow("filter-workflow", "Filter Workflow", "Testing extended action filtering")

	// Create a stage
	stage := NewStage("filter-stage", "Filter Stage", "Stage with varied actions for filtering tests")

	// Add actions with different properties
	setupAction := NewTestActionWithTags("setup-db", "Setup Database", []string{"setup", "database"}, func(ctx *ActionContext) error {
		return nil
	})

	cleanupAction := NewTestActionWithTags("cleanup-temp", "Clean Temporary Files", []string{"cleanup", "filesystem"}, func(ctx *ActionContext) error {
		return nil
	})

	processAction := NewTestActionWithTags("process-data", "Process User Data", []string{"processing", "core"}, func(ctx *ActionContext) error {
		return nil
	})

	validationAction := NewTestActionWithTags("validate-config", "Validate Configuration", []string{"validation", "core"}, func(ctx *ActionContext) error {
		// Test all the filtering methods

		// Test filtering by any tag
		setupOrCleanup := ctx.FindActionsByAnyTag([]string{"setup", "cleanup"})
		assert.Equal(t, 2, len(setupOrCleanup), "Should find 2 actions with either 'setup' or 'cleanup' tags")

		// Test filtering by name
		dbActions := ctx.FindActionsByName("db")
		assert.Equal(t, 1, len(dbActions), "Should find 1 action with 'db' in the name")

		// Test filtering by exact name
		exactNameActions := ctx.FindActionsByExactName("process-data")
		assert.Equal(t, 1, len(exactNameActions), "Should find exactly 1 action with name 'process-data'")

		// Test filtering by description
		configActions := ctx.FindActionsByDescription("Configuration")
		assert.Equal(t, 1, len(configActions), "Should find 1 action with 'Configuration' in the description")

		userActions := ctx.FindActionsByDescription("User")
		assert.Equal(t, 1, len(userActions), "Should find 1 action with 'User' in the description")

		// Test filtering by type
		testActions := ctx.FindActionsByType(&TestAction{})
		assert.Equal(t, 4, len(testActions), "Should find 4 actions of TestAction type")

		return nil
	})

	// Add actions to the stage
	stage.AddAction(setupAction)
	stage.AddAction(cleanupAction)
	stage.AddAction(processAction)
	stage.AddAction(validationAction)

	// Add stage to workflow
	workflow.AddStage(stage)

	// Execute the workflow to populate the action context
	logger := &TestLogger{t: t}
	runner := NewRunner()
	_ = runner.Execute(context.Background(), workflow, logger)
}

// Helper to create a standard workflow setup for context tests
func setupActionContextTest(t *testing.T) (*Workflow, *ActionContext) {
	// Create a workflow with stages and actions
	wf := NewWorkflow("test-wf", "Test Workflow", "Test workflow for action context")

	// Create stages with appropriate tags
	stageSetup := NewStageWithTags("stage-setup", "Setup Stage", "Setup stage", []string{"setup", "core"})
	stageProcess := NewStageWithTags("stage-process", "Processing Stage", "Process stage", []string{"process", "core"})
	stageCleanup := NewStageWithTags("stage-cleanup", "Cleanup Stage", "Cleanup stage", []string{"cleanup"})

	// Add actions to the setup stage
	stageSetup.AddAction(NewTestActionWithTags("action-s1-init", "Initialize", []string{"init", "core"}, nil))
	stageSetup.AddAction(NewTestActionWithTags("action-s1-db", "DB Setup", []string{"db"}, nil))

	// Add actions to the process stage
	stageProcess.AddAction(NewTestActionWithTags("action-s2-main", "Process Data", []string{"main"}, nil))
	stageProcess.AddAction(NewTestActionWithTags("action-s2-optional", "Optional Processing", []string{"optional"}, nil))

	// Add actions to the cleanup stage
	stageCleanup.AddAction(NewTestActionWithTags("action-s3-db", "DB Cleanup", []string{"cleanup", "db"}, nil))
	stageCleanup.AddAction(NewTestActionWithTags("action-s3-files", "File Cleanup", []string{"cleanup", "files"}, nil))

	// Add the stages to the workflow
	wf.AddStage(stageSetup)
	wf.AddStage(stageProcess)
	wf.AddStage(stageCleanup)

	// Create an action context
	ctx := &ActionContext{
		GoContext:       context.Background(),
		Workflow:        wf,
		Stage:           stageSetup,
		Action:          stageSetup.Actions[0],
		Logger:          &TestLogger{t: t},
		disabledActions: make(map[string]bool),
		disabledStages:  make(map[string]bool),
	}

	// Add some data to the store
	wf.Store.Put("key1", "value1")
	wf.Store.Put("key2", "value2")

	return wf, ctx
}

func TestActionContextFinding(t *testing.T) {
	_, ctx := setupActionContextTest(t)

	t.Run("FindStage", func(t *testing.T) {
		stage := ctx.FindStage("stage-process")
		assert.NotNil(t, stage)
		assert.Equal(t, "stage-process", stage.ID)

		stage = ctx.FindStage("non-existent")
		assert.Nil(t, stage)
	})

	t.Run("FindAction", func(t *testing.T) {
		action, stage := ctx.FindAction("action-s2-main")
		assert.NotNil(t, action)
		assert.NotNil(t, stage)
		assert.Equal(t, "action-s2-main", action.Name())
		assert.Equal(t, "stage-process", stage.ID)

		action, stage = ctx.FindAction("non-existent")
		assert.Nil(t, action)
		assert.Nil(t, stage)
	})

	t.Run("FindActionInStage", func(t *testing.T) {
		action := ctx.FindActionInStage("stage-cleanup", "action-s3-db")
		assert.NotNil(t, action)
		assert.Equal(t, "action-s3-db", action.Name())

		action = ctx.FindActionInStage("stage-setup", "action-s3-db") // Wrong stage
		assert.Nil(t, action)

		action = ctx.FindActionInStage("stage-cleanup", "non-existent")
		assert.Nil(t, action)

		action = ctx.FindActionInStage("non-existent-stage", "action-s3-db")
		assert.Nil(t, action)
	})
}

// --- Helper Types for Filtering Tests ---
// Define another action type locally for testing FindActionsByType
type OtherAction struct{ BaseAction }

func (o OtherAction) Execute(ctx *ActionContext) error { return nil }

// --- Filtering Tests ---

func TestActionContextFiltering(t *testing.T) {
	_, ctx := setupActionContextTest(t)

	t.Run("FilterStagesByTag", func(t *testing.T) {
		coreStages := ctx.FindStagesByTag("core")
		assert.Len(t, coreStages, 2)
		ids := []string{coreStages[0].ID, coreStages[1].ID}
		assert.Contains(t, ids, "stage-setup")
		assert.Contains(t, ids, "stage-process")

		cleanupStages := ctx.FindStagesByTag("cleanup")
		assert.Len(t, cleanupStages, 1)
		assert.Equal(t, "stage-cleanup", cleanupStages[0].ID)

		none := ctx.FindStagesByTag("non-existent")
		assert.Empty(t, none)
	})

	t.Run("FilterStagesByAllTags", func(t *testing.T) {
		stages := ctx.FindStagesByAllTags([]string{"setup", "core"})
		assert.Len(t, stages, 1)
		assert.Equal(t, "stage-setup", stages[0].ID)

		stages = ctx.FindStagesByAllTags([]string{"core", "non-existent"})
		assert.Empty(t, stages)

		stages = ctx.FindStagesByAllTags([]string{"core"}) // Should still work
		assert.Len(t, stages, 2)
	})

	t.Run("FilterStagesByAnyTag", func(t *testing.T) {
		stages := ctx.FindStagesByAnyTag([]string{"setup", "cleanup"})
		assert.Len(t, stages, 2)
		ids := []string{stages[0].ID, stages[1].ID}
		assert.Contains(t, ids, "stage-setup")
		assert.Contains(t, ids, "stage-cleanup")

		stages = ctx.FindStagesByAnyTag([]string{"non-existent", "process"})
		assert.Len(t, stages, 1)
		assert.Equal(t, "stage-process", stages[0].ID)

		stages = ctx.FindStagesByAnyTag([]string{"non-existent1", "non-existent2"})
		assert.Empty(t, stages)
	})

	t.Run("FilterStagesByName", func(t *testing.T) {
		stages := ctx.FindStagesByName("Stage") // Should match all
		assert.Len(t, stages, 3)

		stages = ctx.FindStagesByName("process") // Case-insensitive partial match
		assert.Len(t, stages, 1)
		assert.Equal(t, "stage-process", stages[0].ID)
	})

	t.Run("FilterStagesByExactName", func(t *testing.T) {
		stages := ctx.FindStagesByExactName("Processing Stage")
		assert.Len(t, stages, 1)
		assert.Equal(t, "stage-process", stages[0].ID)

		stages = ctx.FindStagesByExactName("processing stage") // Wrong case
		assert.Empty(t, stages)
	})

	t.Run("FilterStagesByDescription", func(t *testing.T) {
		stages := ctx.FindStagesByDescription("cleanup") // Case-insensitive partial match
		assert.Len(t, stages, 1)
		assert.Equal(t, "stage-cleanup", stages[0].ID)
	})

	// --- Action Filtering ---

	t.Run("FilterActionsByTag", func(t *testing.T) {
		dbActions := ctx.FindActionsByTag("db")
		assert.Len(t, dbActions, 2)
		names := []string{dbActions[0].Name(), dbActions[1].Name()}
		assert.Contains(t, names, "action-s1-db")
		assert.Contains(t, names, "action-s3-db")

		coreActions := ctx.FindActionsByTag("core")
		assert.Len(t, coreActions, 1)
		assert.Equal(t, "action-s1-init", coreActions[0].Name())

		none := ctx.FindActionsByTag("non-existent")
		assert.Empty(t, none)
	})

	t.Run("FilterActionsByAllTags", func(t *testing.T) {
		actions := ctx.FindActionsByTags([]string{"cleanup", "db"})
		assert.Len(t, actions, 1)
		assert.Equal(t, "action-s3-db", actions[0].Name())

		actions = ctx.FindActionsByTags([]string{"db", "non-existent"})
		assert.Empty(t, actions)
	})

	t.Run("FilterActionsByAnyTag", func(t *testing.T) {
		actions := ctx.FindActionsByAnyTag([]string{"init", "files"})
		assert.Len(t, actions, 2)
		names := []string{actions[0].Name(), actions[1].Name()}
		assert.Contains(t, names, "action-s1-init")
		assert.Contains(t, names, "action-s3-files")

		actions = ctx.FindActionsByAnyTag([]string{"non-existent", "main"})
		assert.Len(t, actions, 1)
		assert.Equal(t, "action-s2-main", actions[0].Name())
	})

	t.Run("FilterActionsByName", func(t *testing.T) {
		actions := ctx.FindActionsByName("-db") // Matches suffix
		assert.Len(t, actions, 2)

		actions = ctx.FindActionsByName("s2") // Changed search term from "Process" to "s2"
		assert.Len(t, actions, 2)
		names := []string{actions[0].Name(), actions[1].Name()}
		assert.Contains(t, names, "action-s2-main")
		assert.Contains(t, names, "action-s2-optional")
	})

	t.Run("FilterActionsByExactName", func(t *testing.T) {
		actions := ctx.FindActionsByExactName("action-s1-init")
		assert.Len(t, actions, 1)

		actions = ctx.FindActionsByExactName("Action-s1-init") // Wrong case
		assert.Empty(t, actions)
	})

	t.Run("FilterActionsByDescription", func(t *testing.T) {
		actions := ctx.FindActionsByDescription("DB") // Matches DB Setup and DB Cleanup
		assert.Len(t, actions, 2)

		actions = ctx.FindActionsByDescription("Init")
		assert.Len(t, actions, 1)
		assert.Equal(t, "action-s1-init", actions[0].Name())
	})

	t.Run("FilterActionsByType", func(t *testing.T) {
		testActions := ctx.FindActionsByType(&TestAction{}) // All actions are TestAction
		assert.Len(t, testActions, 6)

		// Use the locally defined OtherAction type
		otherActions := ctx.FindActionsByType(&OtherAction{}) // No actions of this type
		assert.Empty(t, otherActions)
	})
}

func TestActionContextModification(t *testing.T) {
	// Note: Each t.Run uses a fresh setup to avoid interference

	t.Run("ListStates", func(t *testing.T) {
		_, ctx := setupActionContextTest(t)

		allStages := ctx.ListAllStages()
		assert.Len(t, allStages, 3) // setup, process, cleanup

		allActions := ctx.ListAllActions()
		assert.Len(t, allActions, 6) // 2 per stage

		setupActions := ctx.ListAllStageActions("stage-setup")
		assert.Len(t, setupActions, 2)

		nonExistentActions := ctx.ListAllStageActions("non-existent")
		assert.Nil(t, nonExistentActions)

		stageStates := ctx.GetStageStates()
		assert.Len(t, stageStates, 3)
		for _, s := range stageStates {
			assert.True(t, s.Enabled, "All stages should be enabled initially")
		}

		actionStates := ctx.GetActionStates("stage-process")
		assert.Len(t, actionStates, 2)
		for _, a := range actionStates {
			assert.True(t, a.Enabled, "All actions should be enabled initially")
		}

		nilStates := ctx.GetActionStates("non-existent")
		assert.Nil(t, nilStates)
	})

	t.Run("AddActionToStage", func(t *testing.T) {
		_, ctx := setupActionContextTest(t)

		newAction := NewTestAction("new-action", "A dynamically added action", nil)
		err := ctx.AddActionToStage("stage-process", newAction)
		assert.NoError(t, err)

		processActions := ctx.ListAllStageActions("stage-process")
		assert.Len(t, processActions, 3)
		assert.Equal(t, "new-action", processActions[2].Name()) // Should be appended

		// Add to non-existent stage
		err = ctx.AddActionToStage("non-existent", newAction)
		assert.Error(t, err)
	})

	t.Run("RemoveAction", func(t *testing.T) {
		wf, ctx := setupActionContextTest(t)

		removed := ctx.RemoveAction("action-s2-main")
		assert.True(t, removed)
		assert.Len(t, wf.Stages[1].Actions, 1) // Stage 2 should have 1 action left
		assert.Equal(t, "action-s2-optional", wf.Stages[1].Actions[0].Name())

		removed = ctx.RemoveAction("action-s2-main") // Remove again
		assert.False(t, removed)

		removed = ctx.RemoveAction("non-existent")
		assert.False(t, removed)
	})

	t.Run("RemoveActionsByTag", func(t *testing.T) {
		wf, ctx := setupActionContextTest(t)

		removedCount := ctx.RemoveActionsByTag("db") // Removes s1-db and s3-db
		assert.Equal(t, 2, removedCount)
		assert.Len(t, wf.Stages[0].Actions, 1) // Stage 1 has 1 left
		assert.Len(t, wf.Stages[2].Actions, 1) // Stage 3 has 1 left
		assert.Equal(t, "action-s1-init", wf.Stages[0].Actions[0].Name())
		assert.Equal(t, "action-s3-files", wf.Stages[2].Actions[0].Name())

		removedCount = ctx.RemoveActionsByTag("non-existent")
		assert.Equal(t, 0, removedCount)
	})

	t.Run("RemoveActionsByType", func(t *testing.T) {
		wf, ctx := setupActionContextTest(t)

		// Add a different type of action
		otherAction := &OtherAction{BaseAction: NewBaseAction("other-type", "Other Type Action")}
		ctx.AddActionToStage("stage-setup", otherAction)
		assert.Len(t, wf.Stages[0].Actions, 3)

		removedCount := ctx.RemoveActionsByType(&OtherAction{}) // Remove the other action
		assert.Equal(t, 1, removedCount)
		assert.Len(t, wf.Stages[0].Actions, 2)

		removedCount = ctx.RemoveActionsByType(&TestAction{}) // Remove all remaining
		assert.Equal(t, 6, removedCount)
		assert.Empty(t, ctx.ListAllActions())
	})

	t.Run("RemoveStage", func(t *testing.T) {
		wf, ctx := setupActionContextTest(t)

		assert.Len(t, wf.Stages, 3)
		removed := ctx.RemoveStage("stage-process")
		assert.True(t, removed)
		assert.Len(t, wf.Stages, 2)
		assert.Equal(t, "stage-setup", wf.Stages[0].ID)
		assert.Equal(t, "stage-cleanup", wf.Stages[1].ID)

		removed = ctx.RemoveStage("stage-process") // Remove again
		assert.False(t, removed)

		removed = ctx.RemoveStage("non-existent")
		assert.False(t, removed)
	})

	t.Run("EnableDisableAction", func(t *testing.T) {
		_, ctx := setupActionContextTest(t)
		actionName := "action-s2-main"

		assert.True(t, ctx.IsActionEnabled(actionName), "Action should be enabled initially")

		ctx.DisableAction(actionName)
		assert.False(t, ctx.IsActionEnabled(actionName), "Action should be disabled")
		assert.True(t, ctx.disabledActions[actionName])

		ctx.EnableAction(actionName)
		assert.True(t, ctx.IsActionEnabled(actionName), "Action should be enabled again")
		assert.False(t, ctx.disabledActions[actionName])

		// Test non-existent action
		assert.True(t, ctx.IsActionEnabled("non-existent"))
		ctx.DisableAction("non-existent")
		assert.False(t, ctx.IsActionEnabled("non-existent"))
		ctx.EnableAction("non-existent")
		assert.True(t, ctx.IsActionEnabled("non-existent"))
	})

	t.Run("EnableDisableStage", func(t *testing.T) {
		_, ctx := setupActionContextTest(t)
		stageID := "stage-process"

		assert.True(t, ctx.IsStageEnabled(stageID), "Stage should be enabled initially")

		ctx.DisableStage(stageID)
		assert.False(t, ctx.IsStageEnabled(stageID), "Stage should be disabled")
		assert.True(t, ctx.disabledStages[stageID])

		ctx.EnableStage(stageID)
		assert.True(t, ctx.IsStageEnabled(stageID), "Stage should be enabled again")
		assert.False(t, ctx.disabledStages[stageID])

		// Test non-existent stage
		assert.True(t, ctx.IsStageEnabled("non-existent"))
		ctx.DisableStage("non-existent")
		assert.False(t, ctx.IsStageEnabled("non-existent"))
		ctx.EnableStage("non-existent")
		assert.True(t, ctx.IsStageEnabled("non-existent"))
	})

	t.Run("EnableDisableActionsByTag", func(t *testing.T) {
		_, ctx := setupActionContextTest(t)
		dbAction1 := "action-s1-db"
		dbAction2 := "action-s3-db"

		disabledCount := ctx.DisableActionsByTag("db")
		assert.Equal(t, 2, disabledCount)
		assert.False(t, ctx.IsActionEnabled(dbAction1))
		assert.False(t, ctx.IsActionEnabled(dbAction2))
		assert.True(t, ctx.IsActionEnabled("action-s1-init")) // Untagged action

		enabledCount := ctx.EnableActionsByTag("db")
		assert.Equal(t, 2, enabledCount)
		assert.True(t, ctx.IsActionEnabled(dbAction1))
		assert.True(t, ctx.IsActionEnabled(dbAction2))

		// Test non-existent tag
		disabledCount = ctx.DisableActionsByTag("non-existent")
		assert.Equal(t, 0, disabledCount)
		enabledCount = ctx.EnableActionsByTag("non-existent")
		assert.Equal(t, 0, enabledCount)
	})

	t.Run("EnableDisableStagesByTag", func(t *testing.T) {
		_, ctx := setupActionContextTest(t)
		coreStage1 := "stage-setup"
		coreStage2 := "stage-process"

		disabledCount := ctx.DisableStagesByTag("core")
		assert.Equal(t, 2, disabledCount)
		assert.False(t, ctx.IsStageEnabled(coreStage1))
		assert.False(t, ctx.IsStageEnabled(coreStage2))
		assert.True(t, ctx.IsStageEnabled("stage-cleanup")) // Untagged stage

		enabledCount := ctx.EnableStagesByTag("core")
		assert.Equal(t, 2, enabledCount)
		assert.True(t, ctx.IsStageEnabled(coreStage1))
		assert.True(t, ctx.IsStageEnabled(coreStage2))

		// Test non-existent tag
		disabledCount = ctx.DisableStagesByTag("non-existent")
		assert.Equal(t, 0, disabledCount)
		enabledCount = ctx.EnableStagesByTag("non-existent")
		assert.Equal(t, 0, enabledCount)
	})

	t.Run("EnableDisableActionsByType", func(t *testing.T) {
		_, ctx := setupActionContextTest(t)
		ctx.AddActionToStage("stage-setup", &OtherAction{BaseAction: NewBaseAction("other-type", "")})

		disabledCount := ctx.DisableActionsByType(&OtherAction{})
		assert.Equal(t, 1, disabledCount)
		assert.False(t, ctx.IsActionEnabled("other-type"))
		assert.True(t, ctx.IsActionEnabled("action-s1-init")) // TestAction type

		enabledCount := ctx.EnableActionsByType(&OtherAction{})
		assert.Equal(t, 1, enabledCount)
		assert.True(t, ctx.IsActionEnabled("other-type"))

		// Disable all TestActions
		disabledCount = ctx.DisableActionsByType(&TestAction{})
		assert.Equal(t, 6, disabledCount)
		assert.False(t, ctx.IsActionEnabled("action-s1-init"))
	})

	// Dynamic Add/Remove tests omitted here as they are covered elsewhere or harder to isolate
	// specifically in the context modification test function.
}

// TestActionContext tests various functions of the ActionContext
func TestActionContext(t *testing.T) {
	// Create a workflow and stage
	workflow := NewWorkflow("test-wf", "Test Workflow", "A workflow for testing")
	stage1 := NewStage("stage1", "Stage 1", "First test stage")
	stage2 := NewStage("stage2", "Stage 2", "Second test stage")

	// Add stages to workflow
	workflow.AddStage(stage1)
	workflow.AddStage(stage2)

	// Create an action
	action := NewTestAction("test-action", "Test Action", func(ctx *ActionContext) error {
		return nil
	})

	// Create context
	ctx := &ActionContext{
		GoContext:       context.Background(),
		Workflow:        workflow,
		Stage:           stage1,
		Action:          action,
		Logger:          NewDefaultLogger(),
		dynamicActions:  []Action{},
		dynamicStages:   []*Stage{},
		disabledActions: make(map[string]bool),
		disabledStages:  make(map[string]bool),
	}

	// Test store access
	store := ctx.Store()
	assert.NotNil(t, store)
	assert.Equal(t, workflow.Store, store)

	// Test stage finding
	foundStage := ctx.FindStage("stage2")
	assert.NotNil(t, foundStage)
	assert.Equal(t, "stage2", foundStage.ID)

	// Test dynamic action generation
	newAction := NewTestAction("dynamic-action", "Dynamic Action", func(ctx *ActionContext) error {
		return nil
	})
	ctx.AddDynamicAction(newAction)
	assert.Len(t, ctx.dynamicActions, 1)

	// Test dynamic stage generation
	newStage := NewStage("dynamic-stage", "Dynamic Stage", "A dynamically generated stage")
	ctx.AddDynamicStage(newStage)
	assert.Len(t, ctx.dynamicStages, 1)

	// Test action enable/disable
	ctx.DisableAction("test-action")
	assert.False(t, ctx.IsActionEnabled("test-action"))
	ctx.EnableAction("test-action")
	assert.True(t, ctx.IsActionEnabled("test-action"))

	// Test stage enable/disable
	ctx.DisableStage("stage1")
	assert.False(t, ctx.IsStageEnabled("stage1"))
	ctx.EnableStage("stage1")
	assert.True(t, ctx.IsStageEnabled("stage1"))
}
