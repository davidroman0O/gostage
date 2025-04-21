package main

import (
	"context"
	"fmt"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/examples/common"
	"github.com/davidroman0O/gostage/store"
)

// StageControlAction demonstrates how to enable/disable actions and stages
type StageControlAction struct {
	gostage.BaseAction
}

// NewStageControlAction creates a new stage control action
func NewStageControlAction(name, description string) *StageControlAction {
	return &StageControlAction{
		BaseAction: gostage.NewBaseAction(name, description),
	}
}

// Execute implements the stage control behavior
func (a *StageControlAction) Execute(ctx *gostage.ActionContext) error {
	// Disable a specific action by name
	ctx.DisableAction("action-to-disable")
	ctx.Logger.Info("Disabled action: action-to-disable")

	// Re-enable an action
	ctx.EnableAction("action-to-reenable")
	ctx.Logger.Info("Enabled action: action-to-reenable")

	// Check if an action is enabled
	isEnabled := ctx.IsActionEnabled("some-action")
	ctx.Logger.Info("Action 'some-action' enabled status: %v", isEnabled)

	// Disable an entire stage by ID
	ctx.DisableStage("stage-to-skip")
	ctx.Logger.Info("Disabled stage: stage-to-skip")

	// Disable stages by tag
	disabledCount := ctx.DisableStagesByTag("optional")
	ctx.Logger.Info("Disabled %d stages with tag 'optional'", disabledCount)

	// Get all stage states (enabled/disabled)
	stageStates := ctx.GetStageStates()
	for _, state := range stageStates {
		ctx.Logger.Info("Stage '%s' enabled: %v", state.Stage.ID, state.Enabled)
	}

	return nil
}

// ConditionalAction determines whether to enable/disable other actions
// based on conditions stored in the workflow
type ConditionalAction struct {
	gostage.BaseAction
}

// NewConditionalAction creates a new conditional action
func NewConditionalAction(name, description string) *ConditionalAction {
	return &ConditionalAction{
		BaseAction: gostage.NewBaseAction(name, description),
	}
}

// Execute implements the conditional behavior
func (a *ConditionalAction) Execute(ctx *gostage.ActionContext) error {
	// Example: Check a condition in the store and disable actions based on it
	envValue, err := store.Get[string](ctx.Store, "environment")
	if err != nil {
		return fmt.Errorf("failed to get environment: %w", err)
	}

	ctx.Logger.Info("Current environment: %s", envValue)

	if envValue == "development" {
		// Disable production-only actions
		ctx.DisableActionsByTag("production-only")
		ctx.Logger.Info("Disabled all production-only actions")
	} else if envValue == "production" {
		// Disable development-only actions
		ctx.DisableActionsByTag("dev-only")
		ctx.Logger.Info("Disabled all development-only actions")
	}

	return nil
}

// CreateDisableEnableWorkflow builds a workflow demonstrating control flow
func CreateDisableEnableWorkflow() *gostage.Workflow {
	// Create a new workflow
	wf := gostage.NewWorkflow(
		"control-flow-demo",
		"Control Flow Demonstration",
		"Demonstrates enabling and disabling actions and stages",
	)

	// Stage 1: Initial stage with the control action
	controlStage := gostage.NewStage(
		"control-stage",
		"Control Stage",
		"Contains actions that control workflow flow",
	)

	// Set initial values
	wf.Store.Put("environment", "development")

	// Add control actions
	controlStage.AddAction(NewConditionalAction(
		"environment-checker",
		"Checks environment and disables actions",
	))

	// Stage 2: A stage that might be disabled
	optionalStage := gostage.NewStageWithTags(
		"stage-to-skip",
		"Optional Stage",
		"This stage might be skipped based on conditions",
		[]string{"optional"},
	)

	// Add some actions to the optional stage
	optionalStage.AddAction(NewSimpleAction(
		"action-to-disable",
		"Action that might be disabled",
	))

	optionalStage.AddAction(NewSimpleAction(
		"action-to-reenable",
		"Action that might be reenabled",
	))

	// Stage 3: Final stage with tag-based actions
	finalStage := gostage.NewStage(
		"final-stage",
		"Final Stage",
		"Contains actions with different tags",
	)

	// Add tag-based actions
	finalStage.AddAction(NewSimpleActionWithTags(
		"dev-action",
		"Development Action",
		[]string{"dev-only"},
	))

	finalStage.AddAction(NewSimpleActionWithTags(
		"prod-action",
		"Production Action",
		[]string{"production-only"},
	))

	// Add stages to workflow
	wf.AddStage(controlStage)
	wf.AddStage(optionalStage)
	wf.AddStage(finalStage)

	return wf
}

// SimpleAction is a basic action implementation
type SimpleAction struct {
	gostage.BaseAction
	customTags []string
}

// NewSimpleAction creates a new simple action
func NewSimpleAction(name, description string) *SimpleAction {
	return &SimpleAction{
		BaseAction: gostage.NewBaseAction(name, description),
	}
}

// NewSimpleActionWithTags creates a new simple action with tags
func NewSimpleActionWithTags(name, description string, tags []string) *SimpleAction {
	return &SimpleAction{
		BaseAction: gostage.NewBaseActionWithTags(name, description, tags),
		customTags: tags,
	}
}

// Execute implements a simple behavior
func (a *SimpleAction) Execute(ctx *gostage.ActionContext) error {
	ctx.Logger.Info("Executing simple action: %s", a.Name())
	return nil
}

// Tags returns the action's tags including custom ones
func (a *SimpleAction) Tags() []string {
	if len(a.customTags) > 0 {
		return a.customTags
	}
	return a.BaseAction.Tags()
}

// Main function to run the example
func main() {
	fmt.Println("--- Disable/Enable Actions & Stages Example ---")

	// Create the workflow
	wf := CreateDisableEnableWorkflow()

	// Print workflow information
	fmt.Printf("Workflow: %s - %s\n", wf.ID, wf.Name)
	fmt.Printf("Description: %s\n", wf.Description)
	fmt.Printf("Stages: %d\n\n", len(wf.Stages))

	// Execute the workflow
	fmt.Println("Executing workflow...")

	// Create a context and a console logger
	ctx := context.Background()
	logger := common.NewConsoleLogger(common.LogLevelInfo)

	if err := wf.Execute(ctx, logger); err != nil {
		fmt.Printf("Error executing workflow: %v\n", err)
		return
	}

	fmt.Println("\nWorkflow completed successfully!")
}
