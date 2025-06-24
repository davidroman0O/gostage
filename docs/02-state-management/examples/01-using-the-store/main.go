package main

import (
	"context"
	"fmt"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

type UserProfile struct {
	ID    int
	Name  string
	Email string
}

// --- Action Definitions ---

// CreateUserAction writes an initial user profile to the store.
type CreateUserAction struct {
	gostage.BaseAction
}

func (a *CreateUserAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("--- Stage 1: Creating User ---")
	profile := UserProfile{ID: 1, Name: "Jane Doe", Email: "jane.doe@example.com"}
	ctx.Store().Put("user.profile", profile)
	ctx.Store().Put("user.status", "pending")
	fmt.Printf("  ✅ Created user '%s' and set status to 'pending'.\n", profile.Name)
	return nil
}

// ActivateUserAction reads the user profile, updates it, and changes the status.
type ActivateUserAction struct {
	gostage.BaseAction
}

func (a *ActivateUserAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("\n--- Stage 2: Activating User ---")
	profile, err := store.Get[UserProfile](ctx.Store(), "user.profile")
	if err != nil {
		return err
	}

	status, _ := store.Get[string](ctx.Store(), "user.status")
	fmt.Printf("  - Found user '%s' with status '%s'.\n", profile.Name, status)

	// Update the status
	ctx.Store().Put("user.status", "active")
	fmt.Printf("  ✅ User status updated to 'active'.\n")
	return nil
}

// ProcessUserDataAction reads the final data and performs some work.
type ProcessUserDataAction struct {
	gostage.BaseAction
}

func (a *ProcessUserDataAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("\n--- Stage 3: Processing User Data ---")
	profile, err := store.Get[UserProfile](ctx.Store(), "user.profile")
	if err != nil {
		return err
	}
	status, _ := store.Get[string](ctx.Store(), "user.status")

	if status == "active" {
		fmt.Printf("  ✅ Processing data for active user: %s (%s)\n", profile.Name, profile.Email)
	} else {
		fmt.Printf("  - Skipping processing for inactive user: %s\n", profile.Name)
	}

	// Clean up the status key after processing
	ctx.Store().Delete("user.status")
	fmt.Printf("  - Cleaned up 'user.status' key from the store.\n")
	return nil
}

func main() {
	// Create a workflow to manage the user lifecycle.
	workflow := gostage.NewWorkflow(
		"user-lifecycle",
		"User Lifecycle Workflow",
		"Demonstrates using the store to manage state across stages.",
	)

	// Stage 1: Create the user
	creationStage := gostage.NewStage("create-user", "Create User", "")
	creationStage.AddAction(&CreateUserAction{
		BaseAction: gostage.NewBaseAction("create-user-action", "Creates a new user profile"),
	})

	// Stage 2: Activate the user
	activationStage := gostage.NewStage("activate-user", "Activate User", "")
	activationStage.AddAction(&ActivateUserAction{
		BaseAction: gostage.NewBaseAction("activate-user-action", "Activates a user's account"),
	})

	// Stage 3: Process the user's data
	processingStage := gostage.NewStage("process-data", "Process Data", "")
	processingStage.AddAction(&ProcessUserDataAction{
		BaseAction: gostage.NewBaseAction("process-user-data-action", "Processes data for the user"),
	})

	// Add stages to the workflow
	workflow.AddStage(creationStage)
	workflow.AddStage(activationStage)
	workflow.AddStage(processingStage)

	// Execute the workflow
	runner := gostage.NewRunner()
	fmt.Println("Executing User Lifecycle workflow...")
	if err := runner.Execute(context.Background(), workflow, nil); err != nil {
		fmt.Printf("Workflow execution failed: %v\n", err)
	}
	fmt.Println("\nWorkflow completed.")

	// Inspect the final state of the store
	fmt.Println("\n--- Final Store State ---")
	finalProfile, err := store.Get[UserProfile](workflow.Store, "user.profile")
	if err == nil {
		fmt.Printf("  - user.profile: %+v\n", finalProfile)
	} else {
		fmt.Println("  - user.profile: Not Found")
	}

	_, err = store.Get[string](workflow.Store, "user.status")
	if err == nil {
		fmt.Println("  - user.status: Found (Should have been deleted)")
	} else {
		fmt.Printf("  - user.status: Not Found (Correctly deleted)\n")
	}
}
