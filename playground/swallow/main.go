package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
)

// Define our own tools key constant
const ToolsProviderKey = "turingpi.tools"

// ---- Tool interfaces ----

// BMCTool interface for BMC operations
type BMCTool interface {
	GetPowerStatus(nodeID int) (string, error)
}

// ImageTool interface for image operations
type ImageTool interface {
	PrepareImage(path string) error
}

// ContainerTool interface for container operations
type ContainerTool interface {
	RunContainer(name string) error
}

// ---- Tool implementations ----

// MockBMCTool implements the BMCTool interface
type MockBMCTool struct {
	Name string
}

func (m *MockBMCTool) GetPowerStatus(nodeID int) (string, error) {
	return fmt.Sprintf("power on (from %s)", m.Name), nil
}

// MockImageTool implements the ImageTool interface
type MockImageTool struct{}

func (m *MockImageTool) PrepareImage(path string) error {
	return nil
}

// MockContainerTool implements the ContainerTool interface
type MockContainerTool struct{}

func (m *MockContainerTool) RunContainer(name string) error {
	return nil
}

// ---- ToolProvider interface and implementation ----

// ToolProvider interface - similar to the real one in the codebase
type ToolProvider interface {
	GetName() string
	GetBMCTool() BMCTool
	GetImageTool() ImageTool
	GetContainerTool() ContainerTool
}

// MockToolProvider implements ToolProvider with unexported fields
// This mimics the structure of TuringPiToolProvider which also has unexported fields
type MockToolProvider struct {
	name          string
	bmcTool       BMCTool       // Unexported interface field
	imageTool     ImageTool     // Unexported interface field
	containerTool ContainerTool // Unexported interface field
	mu            sync.RWMutex  // Unexported mutex
}

func NewMockToolProvider(name string) *MockToolProvider {
	return &MockToolProvider{
		name:          name,
		bmcTool:       &MockBMCTool{Name: name + "-bmc"},
		imageTool:     &MockImageTool{},
		containerTool: &MockContainerTool{},
	}
}

func (m *MockToolProvider) GetName() string {
	return m.name
}

func (m *MockToolProvider) GetBMCTool() BMCTool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bmcTool
}

func (m *MockToolProvider) GetImageTool() ImageTool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.imageTool
}

func (m *MockToolProvider) GetContainerTool() ContainerTool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.containerTool
}

// ---- Demo functions ----

// demonstrateShallowCopyIssue shows how internal interface fields get lost
// This reproduces the issue in the basic playground
func demonstrateShallowCopyIssue() {
	fmt.Println("=== Demonstrating Shallow Copy Issue ===")

	// Create a new workflow
	workflow := gostage.NewWorkflow("test-workflow", "Testing workflow", "1.0.0")

	// Create a mock tools provider
	mockProvider := NewMockToolProvider("ShallowCopyTest")

	// Check BMC tool before storing
	fmt.Println("Before storing:")
	fmt.Printf("Provider name: %s\n", mockProvider.GetName())

	// BMC tool should exist before storing
	bmcTool := mockProvider.GetBMCTool()
	if bmcTool != nil {
		powerStatus, _ := bmcTool.GetPowerStatus(1)
		fmt.Printf("BMC tool is present, power status: %s\n", powerStatus)
	} else {
		fmt.Println("BMC tool is nil before storing (unexpected!)")
	}

	// Store the provider in the workflow store
	fmt.Println("\nStoring provider in the workflow store...")
	workflow.Store.Put(ToolsProviderKey, mockProvider)

	// Retrieve it back as the concrete type
	storedProvider, err := store.Get[*MockToolProvider](workflow.Store, ToolsProviderKey)
	if err != nil {
		fmt.Printf("Error retrieving provider: %v\n", err)
		return
	}

	// Check if the BMC tool survived
	fmt.Println("\nAfter retrieval:")
	fmt.Printf("Retrieved provider name: %s\n", storedProvider.GetName())

	// This is the key part - the BMC tool is likely to be nil after retrieval
	retrievedBMCTool := storedProvider.GetBMCTool()
	if retrievedBMCTool != nil {
		powerStatus, _ := retrievedBMCTool.GetPowerStatus(1)
		fmt.Printf("BMC tool is still present, power status: %s\n", powerStatus)
	} else {
		fmt.Println("BMC tool is nil after retrieval! The store performs a shallow copy.")
		fmt.Println("This reproduces the issue in the basic playground.")
	}
}

// demonstrateInterfaceRetrievalIssue shows how we can't retrieve a concrete type as its interface
func demonstrateInterfaceRetrievalIssue() {
	fmt.Println("\n=== Demonstrating Interface Retrieval Issue ===")

	// Create a new workflow
	workflow := gostage.NewWorkflow("test-workflow", "Testing workflow", "1.0.0")

	// Create a mock tools provider
	mockProvider := NewMockToolProvider("InterfaceTest")

	// Store it using the ToolsProvider key
	fmt.Println("Storing MockToolProvider in the workflow store...")
	workflow.Store.Put(ToolsProviderKey, mockProvider)

	// Try getting with concrete type directly
	concreteProvider, err := store.Get[*MockToolProvider](workflow.Store, ToolsProviderKey)
	if err != nil {
		fmt.Printf("Error retrieving as concrete type: %v\n", err)
	} else {
		fmt.Printf("Successfully retrieved as concrete type: %s\n", concreteProvider.GetName())
	}

	// Try getting with interface type directly (this should fail)
	interfaceProvider, interfaceErr := store.Get[ToolProvider](workflow.Store, ToolsProviderKey)
	if interfaceErr != nil {
		fmt.Printf("Error retrieving as interface: %v\n", interfaceErr)
		fmt.Println("This is the interface type mismatch issue.")
	} else {
		fmt.Printf("Successfully retrieved as interface: %s\n", interfaceProvider.GetName())
		fmt.Println("Unexpected success!")
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
	concreteProvider, err := store.Get[*MockToolProvider](ctx.Store(), ToolsProviderKey)
	if err != nil {
		fmt.Printf("ERROR: Failed to get concrete provider: %v\n", err)
		return err
	}

	fmt.Printf("Successfully got concrete provider: %s\n", concreteProvider.GetName())

	// Check if the BMC tool survived
	bmcTool := concreteProvider.GetBMCTool()
	if bmcTool != nil {
		status, _ := bmcTool.GetPowerStatus(1)
		fmt.Printf("BMC tool available, power status: %s\n", status)
	} else {
		fmt.Println("BMC tool is nil in action context! The issue persists in workflows.")
	}

	// Try with interface type
	interfaceProvider, err := store.Get[ToolProvider](ctx.Store(), ToolsProviderKey)
	if err == nil {
		fmt.Printf("Successfully got interface provider: %s\n", interfaceProvider.GetName())
	} else {
		fmt.Printf("ERROR with interface provider: %v\n", err)
		fmt.Println("This confirms the issue is with interface retrieval.")
	}

	return nil
}

func demonstrateActionRetrieval() {
	fmt.Println("\n=== Demonstrating ToolProvider in Action Context ===")

	// Create a new workflow
	workflow := gostage.NewWorkflow("action-test", "Action test workflow", "1.0.0")

	// Create a mock provider
	mockProvider := NewMockToolProvider("ActionContextTest")

	// Store it
	fmt.Println("Storing MockToolProvider in the workflow store...")
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
	demonstrateShallowCopyIssue()
	demonstrateInterfaceRetrievalIssue()
	demonstrateActionRetrieval()

	fmt.Println("\nStore test playground completed.")
}
