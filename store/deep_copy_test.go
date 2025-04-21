package store

import (
	"testing"
)

// DeepCopyable is an interface for types that need custom deep copy behavior
type DeepCopyable interface {
	DeepCopy() interface{}
}

// DeepCopyToolProvider implements both TestToolProvider and DeepCopyable interfaces
type DeepCopyToolProvider struct {
	Name          string
	bmcTool       BMCTool
	imageTool     ImageTool
	containerTool ContainerTool
}

func NewDeepCopyToolProvider(name string) *DeepCopyToolProvider {
	return &DeepCopyToolProvider{
		Name:          name,
		bmcTool:       &MockBMCTool{Name: name + "-bmc"},
		imageTool:     &MockImageTool{Name: name + "-image"},
		containerTool: &MockContainerTool{Name: name + "-container"},
	}
}

func (p *DeepCopyToolProvider) GetName() string {
	return p.Name
}

func (p *DeepCopyToolProvider) GetBMCTool() BMCTool {
	return p.bmcTool
}

func (p *DeepCopyToolProvider) GetImageTool() ImageTool {
	return p.imageTool
}

func (p *DeepCopyToolProvider) GetContainerTool() ContainerTool {
	return p.containerTool
}

// DeepCopy implements a proper deep copy that preserves interfaces
func (p *DeepCopyToolProvider) DeepCopy() interface{} {
	if p == nil {
		return nil
	}

	// Create a new instance
	copy := &DeepCopyToolProvider{
		Name: p.Name,
	}

	// Copy BMC tool - special handling for interface values
	if bmc, ok := p.bmcTool.(*MockBMCTool); ok {
		copy.bmcTool = &MockBMCTool{Name: bmc.Name}
	}

	// Copy Image tool - special handling for interface values
	if img, ok := p.imageTool.(*MockImageTool); ok {
		copy.imageTool = &MockImageTool{Name: img.Name}
	}

	// Copy Container tool - special handling for interface values
	if cont, ok := p.containerTool.(*MockContainerTool); ok {
		copy.containerTool = &MockContainerTool{Name: cont.Name}
	}

	return copy
}

// TestDeepCopyMechanism demonstrates how to properly handle complex interface types
// by using a DeepCopyable interface instead of relying on JSON serialization
func TestDeepCopyMechanism(t *testing.T) {
	// Demonstrates how interface-heavy objects should be handled:
	// 1. Objects provide their own deep copy mechanism
	// 2. The store recognizes and uses this mechanism instead of JSON serialization

	// Currently this is just a conceptual demonstration, not an actual implementation
	// in the KVStore code

	// Create a provider that supports deep copying
	provider := NewDeepCopyToolProvider("DeepCopyTest")

	// Verify the BMC tool works before storage
	bmcTool := provider.GetBMCTool()
	if bmcTool == nil {
		t.Fatalf("BMC tool should not be nil before storage")
	}

	// Simple mock of what the store should do for DeepCopyable objects
	copiedVal := provider.DeepCopy()

	// Cast back to the concrete type
	copiedProvider, ok := copiedVal.(*DeepCopyToolProvider)
	if !ok {
		t.Fatalf("Failed to cast deep copied value back to the correct type")
	}

	// Check if the name is preserved
	if copiedProvider.GetName() != "DeepCopyTest" {
		t.Errorf("Expected name 'DeepCopyTest', got '%s'", copiedProvider.GetName())
	}

	// Check if the BMC tool is preserved
	copiedBMCTool := copiedProvider.GetBMCTool()
	if copiedBMCTool == nil {
		t.Fatalf("BMC tool is nil after deep copy")
	}

	// Verify the BMC tool works after deep copy
	status, err := copiedBMCTool.GetPowerStatus(1)
	if err != nil {
		t.Errorf("Failed to get power status after deep copy: %v", err)
	}

	if status != "DeepCopyTest-bmc-power-status" {
		t.Errorf("Unexpected power status after deep copy: %s", status)
	}

	t.Log("PASSED! Deep copy mechanism successfully preserved interface fields")
}
