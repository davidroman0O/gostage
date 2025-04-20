package gostate

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDynamicEnableDisable(t *testing.T) {
	// Create a workflow with multiple stages and actions
	workflow := NewWorkflow("dynamic-mgmt", "Dynamic Management", "Testing dynamic enabling/disabling")

	// Create stages
	stage1 := NewStage("stage-1", "First Stage", "First stage of the workflow")
	stage2 := NewStage("stage-2", "Second Stage", "Second stage of the workflow")
	stage3 := NewStage("stage-3", "Third Stage", "Third stage of the workflow")

	// Counters to track execution
	stageCounters := make(map[string]int)
	actionCounters := make(map[string]int)

	// Add actions to the first stage
	stage1.AddAction(NewTestAction("action-1-1", "Action 1-1", func(ctx *ActionContext) error {
		actionCounters["action-1-1"]++
		return nil
	}))

	stage1.AddAction(NewTestAction("action-1-2", "Action 1-2", func(ctx *ActionContext) error {
		actionCounters["action-1-2"]++

		// Disable the third stage
		ctx.DisableStage("stage-3")

		// Disable action-2-2 in the second stage
		ctx.DisableAction("action-2-2")

		return nil
	}))

	// Add actions to the second stage
	stage2.AddAction(NewTestAction("action-2-1", "Action 2-1", func(ctx *ActionContext) error {
		actionCounters["action-2-1"]++
		stageCounters["stage-2"]++
		return nil
	}))

	stage2.AddAction(NewTestAction("action-2-2", "Action 2-2", func(ctx *ActionContext) error {
		actionCounters["action-2-2"]++
		return nil
	}))

	// Add actions to the third stage
	stage3.AddAction(NewTestAction("action-3-1", "Action 3-1", func(ctx *ActionContext) error {
		actionCounters["action-3-1"]++
		stageCounters["stage-3"]++
		return nil
	}))

	// Add stages to workflow
	workflow.AddStage(stage1)
	workflow.AddStage(stage2)
	workflow.AddStage(stage3)

	// Run the workflow
	logger := &TestLogger{t: t}
	err := workflow.Execute(context.Background(), logger)
	assert.NoError(t, err)

	// Verify execution counts - the third stage and action-2-2 should be skipped
	assert.Equal(t, 1, actionCounters["action-1-1"], "Action 1-1 should execute once")
	assert.Equal(t, 1, actionCounters["action-1-2"], "Action 1-2 should execute once")
	assert.Equal(t, 1, actionCounters["action-2-1"], "Action 2-1 should execute once")
	assert.Equal(t, 0, actionCounters["action-2-2"], "Action 2-2 should be skipped")
	assert.Equal(t, 0, actionCounters["action-3-1"], "Action 3-1 should be skipped")

	assert.Equal(t, 1, stageCounters["stage-2"], "Stage 2 should execute")
	assert.Equal(t, 0, stageCounters["stage-3"], "Stage 3 should be skipped")
}

func TestDynamicStageAndActionManagement(t *testing.T) {
	// Create a workflow with a single stage that will dynamically manage other stages
	workflow := NewWorkflow("dynamic-mgmt-workflow", "Dynamic Management", "Testing dynamic workflow management")

	// Create the initial manager stage
	managerStage := NewStage("manager", "Manager Stage", "Stage that manages the workflow")

	// Store created stage IDs for verification
	createdStageIDs := make([]string, 0)

	// Create a map to track when certain operations have been performed
	testState := make(map[string]bool)

	// Add a manager action that creates and manipulates stages and actions
	managerAction := NewTestActionWithTags("manager-action", "Manager Action", []string{"manager", "core"}, func(ctx *ActionContext) error {
		t.Run("Manager Action - Stage/Action Setup", func(t *testing.T) {
			// Create a new stage with tags
			newStage1 := NewStageWithTags("dynamic-stage-1", "Dynamic Stage 1", "Dynamically created stage 1", []string{"dynamic", "primary"})
			newStage1.AddAction(NewTestActionWithTags("dynamic-action-1", "Dynamic Action 1", []string{"dynamic", "primary"}, func(innerCtx *ActionContext) error {
				// Enable the second dynamic stage that will be disabled by default
				innerCtx.EnableStage("dynamic-stage-2")

				// Record that this action executed
				testState["dynamic-action-1-executed"] = true

				return nil
			}))

			// Add the first dynamic stage
			ctx.AddDynamicStage(newStage1)
			createdStageIDs = append(createdStageIDs, "dynamic-stage-1")

			// Create a second stage (will be disabled by default) with tags
			newStage2 := NewStageWithTags("dynamic-stage-2", "Dynamic Stage 2", "Dynamically created stage 2", []string{"dynamic", "secondary"})
			newStage2.AddAction(NewTestActionWithTags("dynamic-action-2", "Dynamic Action 2", []string{"dynamic", "critical"}, func(innerCtx *ActionContext) error {
				t.Run("Manager Action - Dynamic Action 2 Execution", func(t *testing.T) {
					// Find and enable the third action in this stage
					action := innerCtx.FindActionInStage("dynamic-stage-2", "dynamic-action-3")
					assert.NotNil(t, action, "Should find dynamic-action-3")

					// Create and add a new action to this stage directly
					newAction := NewTestActionWithTags("dynamic-action-4", "Dynamic Action 4", []string{"dynamic", "optional"}, func(ctx *ActionContext) error {
						// Record that this action executed
						testState["dynamic-action-4-executed"] = true
						return nil
					})

					innerCtx.AddActionToStage("dynamic-stage-2", newAction)

					// Enable the third action
					innerCtx.EnableAction("dynamic-action-3")

					// Record that this action executed and stage-2 is now active
					testState["dynamic-action-2-executed"] = true
					testState["stage-2-active"] = true

					// List all actions in this stage and verify count
					actions := innerCtx.ListAllStageActions("dynamic-stage-2")
					assert.Equal(t, 4, len(actions), "Should have 4 actions in dynamic-stage-2 at this point (including dynamic-action-3 and dynamic-action-4)")
				})
				return nil
			}))

			// Add a third action that is disabled by default with tags
			newStage2.AddAction(NewTestActionWithTags("dynamic-action-3", "Dynamic Action 3", []string{"dynamic", "cleanup"}, func(innerCtx *ActionContext) error {
				t.Run("Manager Action - Dynamic Action 3 Execution and Filtering Tests", func(t *testing.T) {
					// This should run since it's enabled by dynamic-action-2
					testState["dynamic-action-3-executed"] = true

					// This is a good place to test tag filtering now that all actions are added
					// Test tag-based filtering capabilities on actions
					criticalActions := innerCtx.FindActionsByTag("critical")
					assert.Equal(t, 1, len(criticalActions), "Should find 1 action with 'critical' tag")

					optionalActions := innerCtx.FindActionsByTag("optional")
					assert.Equal(t, 2, len(optionalActions), "Should find 2 actions with 'optional' tag")

					// Test tag-based operations
					dynamicActions := innerCtx.FindActionsByTag("dynamic")
					assert.True(t, len(dynamicActions) >= 4, "Should find at least 4 actions with 'dynamic' tag")

					// Test finding actions by multiple tags
					cleanupActions := innerCtx.FindActionsByTag("cleanup")
					assert.True(t, len(cleanupActions) >= 2, "Should find at least 2 actions with 'cleanup' tag")

					// Test stage tag filtering
					dynamicStages := innerCtx.FindStagesByTag("dynamic")
					assert.Equal(t, 2, len(dynamicStages), "Should find 2 stages with the 'dynamic' tag")

					primaryStages := innerCtx.FindStagesByTag("primary")
					assert.Equal(t, 1, len(primaryStages), "Should find 1 stage with the 'primary' tag")

					// Test finding stages by multiple tags
					dynamicPrimaryStages := innerCtx.FindStagesByAllTags([]string{"dynamic", "primary"})
					assert.Equal(t, 1, len(dynamicPrimaryStages), "Should find 1 stage with both 'dynamic' and 'primary' tags")

					// Test tag operations
					innerCtx.DisableActionsByTag("optional")
					assert.False(t, innerCtx.IsActionEnabled("dynamic-action-4"), "dynamic-action-4 should be disabled")

					// Re-enable the optional action
					enabledCount := innerCtx.EnableActionsByTag("optional")
					assert.Equal(t, 2, enabledCount, "Should enable 2 optional actions")
					assert.True(t, innerCtx.IsActionEnabled("dynamic-action-4"), "dynamic-action-4 should be enabled again")

					// Test finding actions by any tag
					criticalOrOptionalActions := innerCtx.FindActionsByAnyTag([]string{"critical", "optional"})
					assert.Equal(t, 3, len(criticalOrOptionalActions), "Should find 3 actions with either 'critical' or 'optional' tags")
				})
				return nil
			}))

			// Add a fourth action to make sure counts match later with tags
			newStage2.AddAction(NewTestActionWithTags("dynamic-action-5", "Dynamic Action 5", []string{"dynamic", "cleanup", "optional"}, func(innerCtx *ActionContext) error {
				// Record that this action executed
				testState["dynamic-action-5-executed"] = true
				return nil
			}))

			// Disable the third action - will be re-enabled by dynamic-action-2
			ctx.DisableAction("dynamic-action-3")

			// Add the second dynamic stage
			ctx.AddDynamicStage(newStage2)
			createdStageIDs = append(createdStageIDs, "dynamic-stage-2")

			// Disable the second stage by default - will be enabled by dynamic-action-1
			ctx.DisableStage("dynamic-stage-2")

			// List all stages and verify count (only the manager stage should exist in the workflow at this point)
			// The dynamic stages will be added after this action completes
			allStages := ctx.ListAllStages()
			assert.Equal(t, 1, len(allStages), "Should have 1 stage (only manager) before execution completes")

			// Record that this action executed
			testState["manager-action-executed"] = true
		})
		return nil
	})

	// Add a second action to add and then remove a stage
	managerStage.AddAction(NewTestActionWithTags("stage-removal-test", "Stage Removal Test", []string{"manager", "cleanup"}, func(ctx *ActionContext) error {
		t.Run("Stage Removal Action", func(t *testing.T) {
			// Create a test stage to demonstrate removal with tags
			removeStage := NewStageWithTags("remove-me", "Remove Me", "Stage that will be removed", []string{"temporary", "cleanup"})
			ctx.AddDynamicStage(removeStage)

			// Immediately remove it from the dynamic stages
			found := ctx.RemoveStage("remove-me")
			assert.True(t, found, "Should find and remove the 'remove-me' stage")

			// Verify stage was removed
			stage := ctx.FindStage("remove-me")
			assert.Nil(t, stage, "Removed stage should no longer exist")

			// Record that this action executed
			testState["stage-removal-test-executed"] = true
		})
		return nil
	}))

	managerStage.AddAction(managerAction)
	workflow.AddStage(managerStage)

	// Execute context
	logger := &TestLogger{t: t}
	err := workflow.Execute(context.Background(), logger)
	assert.NoError(t, err)

	// === Verification Sub-tests ===

	t.Run("Verify Final Stage Structure", func(t *testing.T) {
		// Verify the workflow now has all expected stages (manager + 2 dynamic stages)
		// The "remove-me" stage should have been removed
		assert.Equal(t, 3, len(workflow.Stages), "Should have 3 stages after execution")

		// Get the stage IDs for verification
		stageIDs := make([]string, 0)
		for _, stage := range workflow.Stages {
			stageIDs = append(stageIDs, stage.ID)
		}

		// Verify all expected stages exist
		assert.Contains(t, stageIDs, "manager", "Manager stage should exist")
		assert.Contains(t, stageIDs, "dynamic-stage-1", "Dynamic stage 1 should exist")
		assert.Contains(t, stageIDs, "dynamic-stage-2", "Dynamic stage 2 should exist")
		assert.NotContains(t, stageIDs, "remove-me", "Removed stage should not exist")

		// Verify tags on stages
		for _, stage := range workflow.Stages {
			if stage.ID == "dynamic-stage-1" {
				assert.True(t, stage.HasTag("primary"), "Dynamic stage 1 should have 'primary' tag")
				assert.True(t, stage.HasTag("dynamic"), "Dynamic stage 1 should have 'dynamic' tag")
			} else if stage.ID == "dynamic-stage-2" {
				assert.True(t, stage.HasTag("secondary"), "Dynamic stage 2 should have 'secondary' tag")
				assert.True(t, stage.HasTag("dynamic"), "Dynamic stage 2 should have 'dynamic' tag")
			}
		}
	})

	t.Run("Verify Action Execution State", func(t *testing.T) {
		// Verify that all the expected actions executed
		assert.True(t, testState["manager-action-executed"], "manager-action should have executed")
		assert.True(t, testState["stage-removal-test-executed"], "stage-removal-test should have executed")
		assert.True(t, testState["dynamic-action-1-executed"], "dynamic-action-1 should have executed")
		assert.True(t, testState["dynamic-action-2-executed"], "dynamic-action-2 should have executed")
		assert.True(t, testState["dynamic-action-3-executed"], "dynamic-action-3 should have executed")
		assert.True(t, testState["dynamic-action-4-executed"], "dynamic-action-4 should have executed")
		assert.True(t, testState["dynamic-action-5-executed"], "dynamic-action-5 should have executed")
		assert.True(t, testState["stage-2-active"], "stage-2 should have been activated")
	})
}

func TestFilteringAndQuerying(t *testing.T) {
	// Create a test workflow with stages and actions
	workflow := NewWorkflow("filter-test", "Filter Test", "Testing filtering capabilities")

	// Add first stage - "setup"
	setupStage := NewStage("setup", "Setup Stage", "Initial setup")
	setupStage.AddAction(NewTestAction("setup-env", "Setup Environment", func(ctx *ActionContext) error {
		return nil
	}))
	setupStage.AddAction(NewTestAction("install-deps", "Install Dependencies", func(ctx *ActionContext) error {
		return nil
	}))
	workflow.AddStage(setupStage)

	// Add second stage - "process"
	processStage := NewStage("process", "Processing Stage", "Main processing")
	processStage.AddAction(NewTestAction("process-data", "Process Data", func(ctx *ActionContext) error {
		// Test filtering capabilities

		// Filter stages by ID prefix
		setupStages := ctx.FilterStages(func(s *Stage) bool {
			return s.ID == "setup"
		})
		assert.Equal(t, 1, len(setupStages), "Should find one stage with ID 'setup'")

		// Filter actions by name containing "install"
		installActions := ctx.FilterActions(func(a Action) bool {
			return a.Name() == "install-deps"
		})
		assert.Equal(t, 1, len(installActions), "Should find one action with name 'install-deps'")

		// Get all action states for setup stage
		actionStates := ctx.GetActionStates("setup")
		assert.Equal(t, 2, len(actionStates), "Setup stage should have 2 actions")

		// Check all stages are enabled by default
		stageStates := ctx.GetStageStates()
		for _, state := range stageStates {
			assert.True(t, state.Enabled, fmt.Sprintf("Stage %s should be enabled by default", state.Stage.ID))
		}

		return nil
	}))
	workflow.AddStage(processStage)

	// Add third stage - "cleanup"
	cleanupStage := NewStage("cleanup", "Cleanup Stage", "Final cleanup")
	cleanupStage.AddAction(NewTestAction("cleanup", "Cleanup", func(ctx *ActionContext) error {
		return nil
	}))
	workflow.AddStage(cleanupStage)

	// Run the workflow
	logger := &TestLogger{t: t}
	err := workflow.Execute(context.Background(), logger)
	assert.NoError(t, err)
}
