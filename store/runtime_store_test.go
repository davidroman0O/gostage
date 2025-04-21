package store

import (
	"sync"
	"testing"
)

// SimpleRuntimeStore demonstrates how a proper runtime store should work
// by holding direct object references without serialization
type SimpleRuntimeStore struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

func NewSimpleRuntimeStore() *SimpleRuntimeStore {
	return &SimpleRuntimeStore{
		data: make(map[string]interface{}),
	}
}

func (s *SimpleRuntimeStore) Put(key string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Store the actual object reference directly - no serialization
	s.data[key] = value
}

func (s *SimpleRuntimeStore) Get(key string) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

// TestSimpleRuntimeStore demonstrates how a runtime store should properly
// preserve interface values by not using serialization
func TestSimpleRuntimeStore(t *testing.T) {
	// Create a simple runtime store
	store := NewSimpleRuntimeStore()

	// Create a provider with interface fields
	provider := NewComplexToolProvider("RuntimeTest")

	// Store it directly
	store.Put("provider", provider)

	// Retrieve it directly
	rawValue, ok := store.Get("provider")
	if !ok {
		t.Fatalf("Failed to retrieve provider from store")
	}

	// Cast it back to the concrete type
	retrievedProvider, ok := rawValue.(*ComplexToolProvider)
	if !ok {
		t.Fatalf("Failed to cast retrieved value to ComplexToolProvider")
	}

	// Check that the name is preserved
	if retrievedProvider.GetName() != "RuntimeTest" {
		t.Errorf("Expected name 'RuntimeTest', got '%s'", retrievedProvider.GetName())
	}

	// Most importantly, check that the interface fields are preserved
	bmcTool := retrievedProvider.GetBMCTool()
	if bmcTool == nil {
		t.Fatalf("BMC tool is nil after retrieval")
	}

	// Verify the BMC tool works correctly
	status, err := bmcTool.GetPowerStatus(1)
	if err != nil {
		t.Errorf("Failed to get power status: %v", err)
	}

	if status != "RuntimeTest-bmc-power-status" {
		t.Errorf("Unexpected power status: %s", status)
	}

	// This test should pass because we're not serializing/deserializing
	// We're just storing and retrieving the actual object reference
}
