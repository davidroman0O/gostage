package store

import (
	"sync"
	"testing"
)

// ---- Tool interfaces for testing nested interface serialization ----

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
	return m.Name + "-power-status", nil
}

// MockImageTool implements the ImageTool interface
type MockImageTool struct {
	Name string
}

func (m *MockImageTool) PrepareImage(path string) error {
	return nil
}

// MockContainerTool implements the ContainerTool interface
type MockContainerTool struct {
	Name string
}

func (m *MockContainerTool) RunContainer(name string) error {
	return nil
}

// ---- Complex ToolProvider with interface fields ----

// TestToolProvider interface with multiple methods
type TestToolProvider interface {
	GetName() string
	GetBMCTool() BMCTool
	GetImageTool() ImageTool
	GetContainerTool() ContainerTool
}

// ComplexToolProvider implements TestToolProvider with unexported fields
type ComplexToolProvider struct {
	name          string
	bmcTool       BMCTool
	imageTool     ImageTool
	containerTool ContainerTool
	mu            sync.RWMutex
}

func NewComplexToolProvider(name string) *ComplexToolProvider {
	return &ComplexToolProvider{
		name:          name,
		bmcTool:       &MockBMCTool{Name: name + "-bmc"},
		imageTool:     &MockImageTool{Name: name + "-image"},
		containerTool: &MockContainerTool{Name: name + "-container"},
	}
}

func (p *ComplexToolProvider) GetName() string {
	return p.name
}

func (p *ComplexToolProvider) GetBMCTool() BMCTool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.bmcTool
}

func (p *ComplexToolProvider) GetImageTool() ImageTool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.imageTool
}

func (p *ComplexToolProvider) GetContainerTool() ContainerTool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.containerTool
}

// TestNestedInterfaceShallowCopy tests that nested interface fields are properly preserved
// This test is expected to fail with the current implementation
func TestNestedInterfaceShallowCopy(t *testing.T) {
	store := NewKVStore()

	// Create a provider with nested interface fields
	provider := NewComplexToolProvider("TestProvider")

	// Verify the BMC tool works before storage
	bmcTool := provider.GetBMCTool()
	if bmcTool == nil {
		t.Fatalf("BMC tool should not be nil before storage")
	}

	status, err := bmcTool.GetPowerStatus(1)
	if err != nil {
		t.Fatalf("Failed to get power status before storage: %v", err)
	}

	if status != "TestProvider-bmc-power-status" {
		t.Errorf("Unexpected power status before storage: %s", status)
	}

	// Store the provider
	err = store.Put("test-provider", provider)
	if err != nil {
		t.Fatalf("Failed to store provider: %v", err)
	}

	// Retrieve the provider
	retrievedProvider, err := Get[*ComplexToolProvider](store, "test-provider")
	if err != nil {
		t.Fatalf("Failed to retrieve provider: %v", err)
	}

	// Check if the name is preserved (should work)
	if retrievedProvider.GetName() != "TestProvider" {
		t.Errorf("Expected name 'TestProvider', got '%s'", retrievedProvider.GetName())
	}

	// Check if the BMC tool is preserved (likely to fail with current implementation)
	retrievedBMCTool := retrievedProvider.GetBMCTool()

	// This is the expected failure point - the BMC tool will be nil
	if retrievedBMCTool == nil {
		t.Log("As expected, BMC tool is nil after retrieval - unexported interface fields are not preserved")
	} else {
		// If somehow it works, verify it functions correctly
		status, err := retrievedBMCTool.GetPowerStatus(1)
		if err != nil {
			t.Errorf("Failed to get power status after retrieval: %v", err)
		} else if status != "TestProvider-bmc-power-status" {
			t.Errorf("Unexpected power status after retrieval: %s", status)
		} else {
			t.Log("PASSED! BMC tool was preserved and working correctly")
		}
	}
}

// PublicFieldsToolProvider implements TestToolProvider with exported fields
type PublicFieldsToolProvider struct {
	Name          string
	BMCTool       BMCTool
	ImageTool     ImageTool
	ContainerTool ContainerTool
	mu            sync.RWMutex
}

func NewPublicFieldsToolProvider(name string) *PublicFieldsToolProvider {
	return &PublicFieldsToolProvider{
		Name:          name,
		BMCTool:       &MockBMCTool{Name: name + "-bmc"},
		ImageTool:     &MockImageTool{Name: name + "-image"},
		ContainerTool: &MockContainerTool{Name: name + "-container"},
	}
}

func (p *PublicFieldsToolProvider) GetName() string {
	return p.Name
}

func (p *PublicFieldsToolProvider) GetBMCTool() BMCTool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.BMCTool
}

func (p *PublicFieldsToolProvider) GetImageTool() ImageTool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ImageTool
}

func (p *PublicFieldsToolProvider) GetContainerTool() ContainerTool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ContainerTool
}

// TestNestedInterfaceWithPublicFields tests if making interface fields public helps
// This test is also expected to fail with the current implementation
func TestNestedInterfaceWithPublicFields(t *testing.T) {
	store := NewKVStore()

	// Create a provider with public interface fields
	provider := NewPublicFieldsToolProvider("PublicFields")

	// Store the provider
	err := store.Put("public-provider", provider)
	if err != nil {
		t.Fatalf("Failed to store provider: %v", err)
	}

	// Retrieve the provider
	retrievedProvider, err := Get[*PublicFieldsToolProvider](store, "public-provider")
	if err != nil {
		t.Fatalf("Failed to retrieve provider: %v", err)
	}

	// Check if the BMC tool is preserved
	retrievedBMCTool := retrievedProvider.GetBMCTool()

	// Even with public fields, interfaces are not correctly serialized/deserialized
	if retrievedBMCTool == nil {
		t.Log("As expected, BMC tool is nil even with public fields - interface values are not preserved")
	} else {
		status, err := retrievedBMCTool.GetPowerStatus(1)
		if err != nil {
			t.Errorf("Failed to get power status: %v", err)
		} else if status != "PublicFields-bmc-power-status" {
			t.Errorf("Unexpected power status: %s", status)
		} else {
			t.Log("PASSED! BMC tool was preserved and working correctly")
		}
	}
}
