package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// Define our own tools key constant
const ToolsProviderKey = "turingpi.tools"

// ToolsProvider interface to simulate whatever is stored in the ToolsProvider key
type ToolsProvider interface {
	GetName() string
}

// MockToolsProvider is a concrete implementation for testing
type MockToolsProvider struct {
	Name string
}

func (m *MockToolsProvider) GetName() string {
	return m.Name
}

func demonstrateToolsProviderIssue() {
	fmt.Println("=== Demonstrating ToolsProvider Issue ===")

	// Create a new workflow
	workflow := gostage.NewWorkflow("test-workflow", "Testing workflow", "1.0.0")

	// Create a mock tools provider
	mockProvider := &MockToolsProvider{Name: "MockProvider"}

	// Store it using the ToolsProvider key
	fmt.Println("Storing MockToolsProvider in the workflow store...")
	workflow.Store.Put(ToolsProviderKey, mockProvider)

	// Try type assertions with the stored object
	fmt.Println("\nRetrieving the object and trying type assertions...")

	// Get all keys to verify it was stored
	fmt.Println("All store keys:", workflow.Store.ListKeys())

	// Try to retrieve the value with store methods
	storeValue, err := store.Get[any](workflow.Store, ToolsProviderKey)
	if err != nil {
		fmt.Printf("Error retrieving value: %v\n", err)
	} else {
		fmt.Printf("Retrieved value type: %T\n", storeValue)

		// Try to cast to concrete type
		if concreteProvider, ok := storeValue.(*MockToolsProvider); ok {
			fmt.Printf("Successfully cast to concrete type: %s\n", concreteProvider.GetName())
		} else {
			fmt.Printf("Failed to cast to concrete type, actual type: %T\n", storeValue)
		}

		// Try to cast to interface
		if interfaceProvider, ok := storeValue.(ToolsProvider); ok {
			fmt.Printf("Successfully cast to interface: %s\n", interfaceProvider.GetName())
		} else {
			fmt.Printf("Failed to cast to interface, actual type: %T\n", storeValue)
			fmt.Println("This is the issue - can't cast stored concrete type to interface!")
		}
	}

	// Try getting with concrete type directly
	concreteValue, err := store.Get[*MockToolsProvider](workflow.Store, ToolsProviderKey)
	if err != nil {
		fmt.Printf("Error retrieving concrete value: %v\n", err)
	} else {
		fmt.Printf("Retrieved concrete value: %s\n", concreteValue.GetName())
	}

	// Try getting with interface type directly (this should fail)
	interfaceValue, err := store.Get[ToolsProvider](workflow.Store, ToolsProviderKey)
	if err != nil {
		fmt.Printf("Error retrieving interface value: %v\n", err)
		if strings.Contains(err.Error(), "type") && strings.Contains(err.Error(), "mismatch") {
			fmt.Println("This is a type mismatch error! Can't retrieve concrete type as interface.")
		}
	} else {
		fmt.Printf("Retrieved interface value: %s\n", interfaceValue.GetName())
		fmt.Println("Unexpectedly worked!")
	}
}

// RetrieveProviderAction is a custom action for testing
type RetrieveProviderAction struct {
	gostage.BaseAction
}

func NewRetrieveProviderAction() *RetrieveProviderAction {
	return &RetrieveProviderAction{
		BaseAction: gostage.NewBaseAction("retrieve-provider", "Test retrieving provider"),
	}
}

// Execute implements the Action interface
func (a *RetrieveProviderAction) Execute(ctx *gostage.ActionContext) error {
	fmt.Println("\n=== In RetrieveProviderAction ===")

	// Try with concrete type first
	concreteProvider, err := store.Get[*MockToolsProvider](ctx.Store(), ToolsProviderKey)
	if err != nil {
		fmt.Printf("ERROR: Failed to get concrete provider: %v\n", err)
		return err
	}

	fmt.Printf("Successfully got concrete provider: %s\n", concreteProvider.GetName())

	// Try with interface type
	interfaceProvider, err := store.Get[ToolsProvider](ctx.Store(), ToolsProviderKey)
	if err == nil {
		fmt.Printf("Successfully got interface provider: %s\n", interfaceProvider.GetName())
	} else {
		fmt.Printf("ERROR with interface provider: %v\n", err)
		fmt.Println("This confirms the issue is with interface retrieval.")
	}

	return nil
}

func demonstrateActionRetrieval() {
	fmt.Println("\n=== Demonstrating ToolsProvider in Action Context ===")

	// Create a new workflow
	workflow := gostage.NewWorkflow("action-test", "Action test workflow", "1.0.0")

	// Create a mock provider
	mockProvider := &MockToolsProvider{Name: "ActionContextTest"}

	// Store it
	fmt.Println("Storing MockToolsProvider in the workflow store...")
	workflow.Store.Put(ToolsProviderKey, mockProvider)

	// Create a stage
	stage := gostage.NewStage("test-stage", "Test Stage", "Test Stage")
	workflow.AddStage(stage)

	// Add the action to the stage
	action := NewRetrieveProviderAction()
	stage.AddAction(action)

	// Execute the workflow - using the correct runner method
	fmt.Println("Executing the workflow...")

	// Create a logger for workflow execution
	logger := &SimpleLogger{}

	// Create a new runner to execute the workflow (correct gostage API)
	runner := gostage.NewRunner()

	// Execute the workflow using the runner with the correct method
	ctx := context.Background()
	err := runner.Execute(ctx, workflow, logger)
	if err != nil {
		fmt.Printf("Workflow execution failed: %v\n", err)
	} else {
		fmt.Println("Workflow executed successfully.")
	}
}

// SimpleLogger implements a basic logger for the workflow
type SimpleLogger struct{}

// Debug implements part of the gostage.Logger interface
func (l *SimpleLogger) Debug(format string, args ...interface{}) {
	fmt.Printf("[DEBUG] "+format+"\n", args...)
}

// Info implements part of the gostage.Logger interface
func (l *SimpleLogger) Info(format string, args ...interface{}) {
	fmt.Printf("[INFO] "+format+"\n", args...)
}

// Error implements part of the gostage.Logger interface
func (l *SimpleLogger) Error(format string, args ...interface{}) {
	fmt.Printf("[ERROR] "+format+"\n", args...)
}

// Warn implements part of the gostage.Logger interface
func (l *SimpleLogger) Warn(format string, args ...interface{}) {
	fmt.Printf("[WARN] "+format+"\n", args...)
}

func main() {
	fmt.Println("Starting store test playground...")

	// Exit if there's any error
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Program panicked: %v\n", r)
			os.Exit(1)
		}
	}()

	// Run the demonstrations
	demonstrateToolsProviderIssue()
	demonstrateActionRetrieval()

	fmt.Println("\nStore test playground completed.")
}
