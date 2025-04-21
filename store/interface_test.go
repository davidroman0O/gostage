package store

import (
	"testing"
	"time"
)

// ToolsProvider interface to test interface retrieval
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

// AnotherProvider is another concrete implementation of ToolsProvider
type AnotherProvider struct {
	Name      string
	ExtraData string
}

func (a *AnotherProvider) GetName() string {
	return a.Name
}

// TestInterfaceRetrieval tests storing and retrieving values as interfaces
func TestInterfaceRetrieval(t *testing.T) {
	store := NewKVStore()

	// Create test providers
	mockProvider := &MockToolsProvider{Name: "MockProvider"}
	anotherProvider := &AnotherProvider{Name: "AnotherProvider", ExtraData: "Extra"}

	// Store the providers
	err := store.Put("mock-provider", mockProvider)
	if err != nil {
		t.Fatalf("Failed to store mock provider: %v", err)
	}

	err = store.Put("another-provider", anotherProvider)
	if err != nil {
		t.Fatalf("Failed to store another provider: %v", err)
	}

	// Test retrieving as concrete type
	retrievedMock, err := Get[*MockToolsProvider](store, "mock-provider")
	if err != nil {
		t.Fatalf("Failed to retrieve mock provider as concrete type: %v", err)
	}
	if retrievedMock.GetName() != "MockProvider" {
		t.Errorf("Expected name 'MockProvider', got '%s'", retrievedMock.GetName())
	}

	// Test retrieving as interface
	retrievedInterface, err := Get[ToolsProvider](store, "mock-provider")
	if err != nil {
		t.Fatalf("Failed to retrieve mock provider as interface: %v", err)
	}
	if retrievedInterface.GetName() != "MockProvider" {
		t.Errorf("Expected interface name 'MockProvider', got '%s'", retrievedInterface.GetName())
	}

	// Test retrieving different implementation as same interface
	anotherInterface, err := Get[ToolsProvider](store, "another-provider")
	if err != nil {
		t.Fatalf("Failed to retrieve another provider as interface: %v", err)
	}
	if anotherInterface.GetName() != "AnotherProvider" {
		t.Errorf("Expected interface name 'AnotherProvider', got '%s'", anotherInterface.GetName())
	}
}

// TestInterfaceRetrievalErrorCases tests error cases for interface retrieval
func TestInterfaceRetrievalErrorCases(t *testing.T) {
	store := NewKVStore()

	// Store a string (cannot implement an interface)
	err := store.Put("string-value", "Hello World")
	if err != nil {
		t.Fatalf("Failed to store string: %v", err)
	}

	// Attempt to retrieve as an interface (should fail)
	_, err = Get[ToolsProvider](store, "string-value")
	if err == nil {
		t.Errorf("Expected error when retrieving string as interface, got nil")
	}

	// Test with non-existent key
	_, err = Get[ToolsProvider](store, "non-existent")
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got: %v", err)
	}

	// Test with expired key
	mockProvider := &MockToolsProvider{Name: "ExpiringProvider"}
	err = store.PutWithTTL("expiring-provider", mockProvider, time.Millisecond*10)
	if err != nil {
		t.Fatalf("Failed to store expiring provider: %v", err)
	}

	// Wait for expiration
	time.Sleep(time.Millisecond * 20)

	_, err = Get[ToolsProvider](store, "expiring-provider")
	if err != ErrExpired {
		t.Errorf("Expected ErrExpired, got: %v", err)
	}
}

// TestMultipleInterfaceImplementations tests retrieving multiple implementations
// of the same interface from the store
func TestMultipleInterfaceImplementations(t *testing.T) {
	store := NewKVStore()

	// Store multiple implementations
	providers := []struct {
		key      string
		provider ToolsProvider
	}{
		{"provider1", &MockToolsProvider{Name: "Provider1"}},
		{"provider2", &MockToolsProvider{Name: "Provider2"}},
		{"provider3", &AnotherProvider{Name: "Provider3", ExtraData: "Extra3"}},
		{"provider4", &AnotherProvider{Name: "Provider4", ExtraData: "Extra4"}},
	}

	for _, p := range providers {
		err := store.Put(p.key, p.provider)
		if err != nil {
			t.Fatalf("Failed to store provider %s: %v", p.key, err)
		}
	}

	// Retrieve and verify each one
	for _, p := range providers {
		retrieved, err := Get[ToolsProvider](store, p.key)
		if err != nil {
			t.Fatalf("Failed to retrieve provider %s: %v", p.key, err)
		}

		if retrieved.GetName() != p.provider.GetName() {
			t.Errorf("Expected name %s, got %s for key %s",
				p.provider.GetName(), retrieved.GetName(), p.key)
		}

		// Check that we can cast back to the concrete type when needed
		switch p.provider.(type) {
		case *MockToolsProvider:
			_, ok := retrieved.(*MockToolsProvider)
			if !ok {
				t.Errorf("Could not cast %s back to *MockToolsProvider", p.key)
			}
		case *AnotherProvider:
			concreteProvider, ok := retrieved.(*AnotherProvider)
			if !ok {
				t.Errorf("Could not cast %s back to *AnotherProvider", p.key)
			} else if concreteProvider.ExtraData != p.provider.(*AnotherProvider).ExtraData {
				t.Errorf("ExtraData mismatch for %s, expected %s, got %s",
					p.key, p.provider.(*AnotherProvider).ExtraData, concreteProvider.ExtraData)
			}
		}
	}
}

// NestedStruct is used in ComplexProvider
type NestedStruct struct {
	Value string
}

// ComplexProvider is a more complex implementation of ToolsProvider
type ComplexProvider struct {
	Name      string
	Nested    NestedStruct
	NestedPtr *NestedStruct
}

// GetName implements ToolsProvider
func (c *ComplexProvider) GetName() string {
	return c.Name
}

// TestInterfaceWithNestedStructs tests interface retrieval for more complex struct types
func TestInterfaceWithNestedStructs(t *testing.T) {
	store := NewKVStore()

	// Create and store a complex provider
	complexProvider := &ComplexProvider{
		Name:      "ComplexProvider",
		Nested:    NestedStruct{Value: "Nested"},
		NestedPtr: &NestedStruct{Value: "NestedPtr"},
	}

	err := store.Put("complex-provider", complexProvider)
	if err != nil {
		t.Fatalf("Failed to store complex provider: %v", err)
	}

	// Retrieve as interface
	retrievedInterface, err := Get[ToolsProvider](store, "complex-provider")
	if err != nil {
		t.Fatalf("Failed to retrieve complex provider as interface: %v", err)
	}

	if retrievedInterface.GetName() != "ComplexProvider" {
		t.Errorf("Expected name 'ComplexProvider', got '%s'", retrievedInterface.GetName())
	}

	// Cast back to concrete type and verify nested fields
	retrievedComplex, ok := retrievedInterface.(*ComplexProvider)
	if !ok {
		t.Fatalf("Could not cast back to *ComplexProvider")
	}

	if retrievedComplex.Nested.Value != "Nested" {
		t.Errorf("Expected Nested.Value 'Nested', got '%s'", retrievedComplex.Nested.Value)
	}

	if retrievedComplex.NestedPtr == nil {
		t.Errorf("NestedPtr is nil, expected non-nil")
	} else if retrievedComplex.NestedPtr.Value != "NestedPtr" {
		t.Errorf("Expected NestedPtr.Value 'NestedPtr', got '%s'", retrievedComplex.NestedPtr.Value)
	}
}

// TestTypeKindOptimization tests that the typeKind field is correctly used for optimization
func TestTypeKindOptimization(t *testing.T) {
	store := NewKVStore()

	// Store various types that can't implement interfaces
	primitives := []struct {
		key   string
		value interface{}
	}{
		{"int", 42},
		{"string", "Hello"},
		{"float", 3.14},
		{"bool", true},
	}

	for _, p := range primitives {
		err := store.Put(p.key, p.value)
		if err != nil {
			t.Fatalf("Failed to store %s: %v", p.key, err)
		}

		// Attempt to retrieve as an interface (should fail quickly due to kind optimization)
		_, err = Get[ToolsProvider](store, p.key)
		if err == nil {
			t.Errorf("Expected error when retrieving %s as interface, got nil", p.key)
		}
	}

	// Verify we can successfully retrieve as the correct type
	intValue, err := Get[int](store, "int")
	if err != nil || intValue != 42 {
		t.Errorf("Failed to retrieve int value: %v", err)
	}

	stringValue, err := Get[string](store, "string")
	if err != nil || stringValue != "Hello" {
		t.Errorf("Failed to retrieve string value: %v", err)
	}
}
