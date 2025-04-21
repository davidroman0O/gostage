package main

import (
	"context"
	"fmt"
	"os"

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

// AnotherProvider is a different concrete implementation of ToolsProvider
type AnotherProvider struct {
	Name      string
	ExtraData string
}

func (a *AnotherProvider) GetName() string {
	return a.Name
}

func demonstrateToolsProviderIssue() {
	fmt.Println("=== Demonstrating ToolsProvider Interface Fix ===")

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

	// Try to retrieve the value with store methods (should work with our fix)
	interfaceValue, err := store.Get[ToolsProvider](workflow.Store, ToolsProviderKey)
	if err != nil {
		fmt.Printf("Error retrieving interface value: %v\n", err)
	} else {
		fmt.Printf("Successfully retrieved interface value: %s\n", interfaceValue.GetName())
		fmt.Println("Our fix works! Can retrieve concrete type as interface.")
	}

	// Try getting with concrete type directly
	concreteValue, err := store.Get[*MockToolsProvider](workflow.Store, ToolsProviderKey)
	if err != nil {
		fmt.Printf("Error retrieving concrete value: %v\n", err)
	} else {
		fmt.Printf("Retrieved concrete value: %s\n", concreteValue.GetName())
	}

	fmt.Println("\n=== Testing Multiple Implementations ===")

	// Store a different implementation of the same interface
	anotherProvider := &AnotherProvider{Name: "AnotherProvider", ExtraData: "Extra"}
	workflow.Store.Put("another-provider", anotherProvider)

	// Try retrieving it as the interface
	anotherValue, err := store.Get[ToolsProvider](workflow.Store, "another-provider")
	if err != nil {
		fmt.Printf("Error retrieving another interface value: %v\n", err)
	} else {
		fmt.Printf("Successfully retrieved another interface value: %s\n", anotherValue.GetName())
		fmt.Println("Can retrieve different implementations as the same interface!")
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

	// Try with interface type directly (should work with our fix)
	interfaceProvider, err := store.Get[ToolsProvider](ctx.Store(), ToolsProviderKey)
	if err == nil {
		fmt.Printf("Successfully got interface provider: %s\n", interfaceProvider.GetName())
		fmt.Println("Fix confirmed! Interface retrieval works in actions too.")
	} else {
		fmt.Printf("ERROR with interface provider: %v\n", err)
	}

	// Try with concrete type too for comparison
	concreteProvider, err := store.Get[*MockToolsProvider](ctx.Store(), ToolsProviderKey)
	if err != nil {
		fmt.Printf("ERROR: Failed to get concrete provider: %v\n", err)
		return err
	}

	fmt.Printf("Successfully got concrete provider: %s\n", concreteProvider.GetName())

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

	// Execute the workflow
	fmt.Println("Executing the workflow...")

	// Create a logger for workflow execution
	logger := &SimpleLogger{}

	// Create a new runner to execute the workflow
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
	fmt.Println("Starting enhanced store test playground...")

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

	fmt.Println("\nEnhanced store test playground completed.")
}
