package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMetadata(t *testing.T) {
	store := NewKVStore()

	// Test adding entry with metadata
	meta := NewMetadata()
	meta.AddTag("important")
	meta.AddTag("test")
	meta.SetProperty("priority", 1)
	meta.Description = "Test value with metadata"

	err := store.PutWithMetadata("key1", "value1", meta)
	assert.NoError(t, err)

	// Test getting metadata
	retrievedMeta, err := store.GetMetadata("key1")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(retrievedMeta.Tags))
	assert.True(t, retrievedMeta.HasTag("important"))
	assert.True(t, retrievedMeta.HasTag("test"))

	// Test properties
	priority, ok := retrievedMeta.GetProperty("priority")
	assert.True(t, ok)
	assert.Equal(t, 1, priority)

	// Test description
	assert.Equal(t, "Test value with metadata", retrievedMeta.Description)

	// Test adding another entry with different tags
	meta2 := NewMetadata()
	meta2.AddTag("optional")
	meta2.AddTag("test")

	err = store.PutWithMetadata("key2", "value2", meta2)
	assert.NoError(t, err)

	// Test has tag
	hasTag, err := store.HasTag("key1", "important")
	assert.NoError(t, err)
	assert.True(t, hasTag)

	hasTag, err = store.HasTag("key2", "important")
	assert.NoError(t, err)
	assert.False(t, hasTag)

	// Test finding keys by tag
	keysWithImportant := store.FindKeysByTag("important")
	assert.Equal(t, 1, len(keysWithImportant))
	assert.Equal(t, "key1", keysWithImportant[0])

	keysWithTest := store.FindKeysByTag("test")
	assert.Equal(t, 2, len(keysWithTest))
	assert.Contains(t, keysWithTest, "key1")
	assert.Contains(t, keysWithTest, "key2")

	// Test finding keys by multiple tags
	keysWithAllTags := store.FindKeysByAllTags([]string{"important", "test"})
	assert.Equal(t, 1, len(keysWithAllTags))
	assert.Equal(t, "key1", keysWithAllTags[0])

	keysWithAnyTag := store.FindKeysByAnyTag([]string{"important", "optional"})
	assert.Equal(t, 2, len(keysWithAnyTag))

	// Test finding keys by property
	keysWithPriority := store.FindKeysByProperty("priority", 1)
	assert.Equal(t, 1, len(keysWithPriority))
	assert.Equal(t, "key1", keysWithPriority[0])

	// Test adding tag to existing key
	err = store.AddTag("key2", "important")
	assert.NoError(t, err)

	keysWithImportant = store.FindKeysByTag("important")
	assert.Equal(t, 2, len(keysWithImportant))

	// Test removing tag
	err = store.RemoveTag("key2", "important")
	assert.NoError(t, err)

	keysWithImportant = store.FindKeysByTag("important")
	assert.Equal(t, 1, len(keysWithImportant))

	// Test setting property
	err = store.SetProperty("key2", "priority", 2)
	assert.NoError(t, err)

	keysWithPriority = store.FindKeysByProperty("priority", 2)
	assert.Equal(t, 1, len(keysWithPriority))
	assert.Equal(t, "key2", keysWithPriority[0])

	// Test store merge with metadata
	otherStore := NewKVStore()
	otherMeta := NewMetadata()
	otherMeta.AddTag("shared")
	otherMeta.SetProperty("source", "other-store")

	err = otherStore.PutWithMetadata("key3", "value3", otherMeta)
	assert.NoError(t, err)

	// Add an entry with the same key but different metadata
	otherMeta2 := NewMetadata()
	otherMeta2.AddTag("important")
	otherMeta2.AddTag("shared")
	otherMeta2.SetProperty("priority", 3)

	err = otherStore.PutWithMetadata("key1", "new-value", otherMeta2)
	assert.NoError(t, err)

	// Merge stores
	collisions, err := store.Merge(otherStore, Overwrite)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(collisions))
	assert.Equal(t, "key1", collisions[0])

	// Check that metadata was merged for the collision
	mergedMeta, err := store.GetMetadata("key1")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(mergedMeta.Tags))
	assert.True(t, mergedMeta.HasTag("important"))
	assert.True(t, mergedMeta.HasTag("test"))
	assert.True(t, mergedMeta.HasTag("shared"))

	priority, ok = mergedMeta.GetProperty("priority")
	assert.True(t, ok)
	assert.Equal(t, 3, priority)

	// Check that new key was added with its metadata
	key3Meta, err := store.GetMetadata("key3")
	assert.NoError(t, err)
	assert.True(t, key3Meta.HasTag("shared"))

	source, ok := key3Meta.GetProperty("source")
	assert.True(t, ok)
	assert.Equal(t, "other-store", source)
}

func TestMetadataWithTTL(t *testing.T) {
	store := NewKVStore()

	// Create metadata
	meta := NewMetadata()
	meta.AddTag("temporary")
	meta.SetProperty("expires", true)

	// Add an entry with short TTL
	err := store.PutWithTTLAndMetadata("temp-key", "temp-value", 100*time.Millisecond, meta)
	assert.NoError(t, err)

	// Verify metadata is available immediately
	tempMeta, err := store.GetMetadata("temp-key")
	assert.NoError(t, err)
	assert.True(t, tempMeta.HasTag("temporary"))

	// Should be found in tag search
	keysWithTag := store.FindKeysByTag("temporary")
	assert.Equal(t, 1, len(keysWithTag))

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	// Metadata should no longer be available
	_, err = store.GetMetadata("temp-key")
	assert.Equal(t, ErrExpired, err)

	// Should no longer be found in tag search
	keysWithTag = store.FindKeysByTag("temporary")
	assert.Equal(t, 0, len(keysWithTag))
}

// TestStoreEdgeCases tests edge cases in the KV store
func TestStoreEdgeCases(t *testing.T) {
	t.Run("expired_key_behavior", func(t *testing.T) {
		s := NewKVStore()

		// Add a key with a very short expiration
		err := s.PutWithTTL("temp-key", "temp-value", 1*time.Millisecond)
		assert.NoError(t, err)

		// Wait for expiration
		time.Sleep(5 * time.Millisecond)

		// Should get an error when retrieving expired key
		_, err = Get[string](s, "temp-key")
		assert.Error(t, err)
		assert.Equal(t, ErrExpired, err)

		// Count should not include expired keys
		assert.Equal(t, 0, s.Count())

		// Trying to update an expired key should fail
		err = s.Put("temp-key", "updated-value")
		assert.NoError(t, err) // Actually should work as a new entry

		// Should be able to get the new value
		val, err := Get[string](s, "temp-key")
		assert.NoError(t, err)
		assert.Equal(t, "updated-value", val)
	})

	t.Run("type_mismatch", func(t *testing.T) {
		s := NewKVStore()

		// Store an integer
		err := s.Put("key", 123)
		assert.NoError(t, err)

		// Try to get as string (should fail)
		_, err = Get[string](s, "key")
		assert.Error(t, err)
		// Don't assert on the exact error message as the implementation might wrap it
		assert.Contains(t, err.Error(), "type mismatch")

		// Try to get as int (should succeed)
		val, err := Get[int](s, "key")
		assert.NoError(t, err)
		assert.Equal(t, 123, val)
	})

	t.Run("store_key_collisions", func(t *testing.T) {
		s1 := NewKVStore()
		s2 := NewKVStore()

		// Set up different values for the same keys
		s1.Put("key1", "value1-from-s1")
		s1.Put("key2", "value2-from-s1")
		s1.Put("key3", 123)

		s2.Put("key1", "value1-from-s2")
		s2.Put("key2", 456)
		s2.Put("key4", "value4-from-s2")

		// Test key collisions
		collisions := s1.FindKeyCollisions(s2)
		assert.Equal(t, 2, len(collisions))
		assert.Contains(t, collisions, "key1")
		assert.Contains(t, collisions, "key2")

		// Test different merge strategies

		// Skip strategy
		collisions, err := s1.Merge(s2, Skip)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(collisions))

		// Original values should be preserved
		val1, err := Get[string](s1, "key1")
		assert.NoError(t, err)
		assert.Equal(t, "value1-from-s1", val1)

		// New keys should be added
		val4, err := Get[string](s1, "key4")
		assert.NoError(t, err)
		assert.Equal(t, "value4-from-s2", val4)

		// Reset store
		s1 = NewKVStore()
		s1.Put("key1", "value1-from-s1")
		s1.Put("key2", "value2-from-s1")
		s1.Put("key3", 123)

		// Overwrite strategy
		collisions, err = s1.Merge(s2, Overwrite)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(collisions))

		// Values should be overwritten
		val1, err = Get[string](s1, "key1")
		assert.NoError(t, err)
		assert.Equal(t, "value1-from-s2", val1)

		// Type changes should be reflected
		val2, err := Get[int](s1, "key2")
		assert.NoError(t, err)
		assert.Equal(t, 456, val2)

		// Reset store
		s1 = NewKVStore()
		s1.Put("key1", "value1-from-s1")
		s1.Put("key2", "value2-from-s1")

		// Error strategy
		_, err = s1.Merge(s2, Error)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "collision")
	})
}

// TestUpdateFields tests updating fields in the KV store
func TestUpdateFields(t *testing.T) {
	type TestStruct struct {
		Name   string
		Value  int
		Nested struct {
			Label string
		}
	}

	t.Run("update_single_field", func(t *testing.T) {
		s := NewKVStore()
		initial := TestStruct{Name: "Initial", Value: 10, Nested: struct{ Label string }{Label: "NestedInitial"}}
		err := s.Put("obj1", initial)
		assert.NoError(t, err)

		// Update the Name field
		err = s.UpdateField("obj1", "Name", "UpdatedName")
		assert.NoError(t, err)

		// Verify update
		updated, err := Get[TestStruct](s, "obj1")
		assert.NoError(t, err)
		assert.Equal(t, "UpdatedName", updated.Name)
		assert.Equal(t, 10, updated.Value) // Other fields unchanged
		assert.Equal(t, "NestedInitial", updated.Nested.Label)
	})

	t.Run("update_nested_field", func(t *testing.T) {
		s := NewKVStore()
		initial := TestStruct{Name: "NestedTest", Value: 20, Nested: struct{ Label string }{Label: "OldLabel"}}
		err := s.Put("obj2", initial)
		assert.NoError(t, err)

		// Update the nested Label field
		err = s.UpdateField("obj2", "Nested.Label", "NewLabel")
		assert.NoError(t, err)

		// Verify update
		updated, err := Get[TestStruct](s, "obj2")
		assert.NoError(t, err)
		assert.Equal(t, "NestedTest", updated.Name)
		assert.Equal(t, "NewLabel", updated.Nested.Label)
	})

	t.Run("update_multiple_fields", func(t *testing.T) {
		s := NewKVStore()
		initial := TestStruct{Name: "MultiTest", Value: 30, Nested: struct{ Label string }{Label: "MultiLabelOld"}}
		err := s.Put("obj3", initial)
		assert.NoError(t, err)

		// Update multiple fields
		updates := map[string]interface{}{
			"Name":         "MultiNameUpdated",
			"Value":        35,
			"Nested.Label": "MultiLabelNew",
		}
		err = s.UpdateFields("obj3", updates)
		assert.NoError(t, err)

		// Verify updates
		updated, err := Get[TestStruct](s, "obj3")
		assert.NoError(t, err)
		assert.Equal(t, "MultiNameUpdated", updated.Name)
		assert.Equal(t, 35, updated.Value)
		assert.Equal(t, "MultiLabelNew", updated.Nested.Label)
	})

	t.Run("update_non_existent_key", func(t *testing.T) {
		s := NewKVStore()
		err := s.UpdateField("nonexistent", "Name", "Value")
		assert.Error(t, err)
		assert.Equal(t, ErrNotFound, err)

		err = s.UpdateFields("nonexistent", map[string]interface{}{"Name": "Value"})
		assert.Error(t, err)
		assert.Equal(t, ErrNotFound, err)
	})

	t.Run("update_non_existent_field", func(t *testing.T) {
		s := NewKVStore()
		initial := TestStruct{Name: "FieldTest", Value: 40}
		err := s.Put("obj4", initial)
		assert.NoError(t, err)

		// Try to update a field that doesn't exist
		err = s.UpdateField("obj4", "NonExistentField", "Value")
		assert.Error(t, err) // xreflect should return an error

		// Try UpdateFields with a non-existent field
		updates := map[string]interface{}{
			"Name":             "NewName",
			"NonExistentField": "Value",
		}
		err = s.UpdateFields("obj4", updates)
		assert.Error(t, err) // Should fail on the non-existent field

		// Verify original data is unchanged after failed UpdateFields
		current, err := Get[TestStruct](s, "obj4")
		assert.NoError(t, err)
		assert.Equal(t, "FieldTest", current.Name) // Should still be original name
	})

	t.Run("update_field_type_mismatch", func(t *testing.T) {
		s := NewKVStore()
		initial := TestStruct{Name: "TypeTest", Value: 50}
		err := s.Put("obj5", initial)
		assert.NoError(t, err)

		// Try to update Value (int) with a string
		err = s.UpdateField("obj5", "Value", "not-an-int")
		assert.Error(t, err) // xreflect should return a type error

		// Try UpdateFields with a type mismatch
		updates := map[string]interface{}{
			"Name":  "NewName",
			"Value": "not-an-int",
		}
		err = s.UpdateFields("obj5", updates)
		assert.Error(t, err)
	})

	t.Run("update_field_expired_key", func(t *testing.T) {
		s := NewKVStore()
		initial := TestStruct{Name: "ExpiryTest", Value: 60}
		err := s.PutWithTTL("obj6", initial, 1*time.Millisecond)
		assert.NoError(t, err)

		time.Sleep(5 * time.Millisecond) // Wait for expiration

		err = s.UpdateField("obj6", "Name", "NewName")
		assert.Error(t, err)
		assert.True(t, err == ErrExpired || err == ErrNotFound, "Error should be either ErrExpired or ErrNotFound due to implementation details")

		// Reinsert for the second test
		err = s.PutWithTTL("obj6", initial, 1*time.Millisecond)
		assert.NoError(t, err)

		time.Sleep(5 * time.Millisecond) // Wait for expiration

		err = s.UpdateFields("obj6", map[string]interface{}{"Name": "NewName"})
		assert.Error(t, err)
		assert.True(t, err == ErrExpired || err == ErrNotFound, "Error should be either ErrExpired or ErrNotFound due to implementation details")
	})
}

func TestSchemaAndTypeFiltering(t *testing.T) {
	type Simple struct {
		ID   string
		Data int
	}
	type Complex struct {
		ID     string
		Label  string
		Simple Simple // Nested struct
	}
	type Another struct {
		Value float64
	}

	s := NewKVStore()

	// Add entries
	err := s.Put("simple1", Simple{ID: "s1", Data: 10})
	assert.NoError(t, err)
	err = s.Put("complex1", Complex{ID: "c1", Label: "L1", Simple: Simple{ID: "s1-nested", Data: 11}})
	assert.NoError(t, err)
	err = s.Put("another1", Another{Value: 1.23})
	assert.NoError(t, err)
	err = s.Put("simple2", Simple{ID: "s2", Data: 20})
	assert.NoError(t, err)

	t.Run("find_keys_by_type", func(t *testing.T) {
		// Find Simple types
		simpleKeys := KeysByType[Simple](s)
		assert.Len(t, simpleKeys, 2)
		assert.Contains(t, simpleKeys, "simple1")
		assert.Contains(t, simpleKeys, "simple2")

		// Find Complex types
		complexKeys := KeysByType[Complex](s)
		assert.Len(t, complexKeys, 1)
		assert.Equal(t, "complex1", complexKeys[0])

		// Find Another types
		anotherKeys := KeysByType[Another](s)
		assert.Len(t, anotherKeys, 1)
		assert.Equal(t, "another1", anotherKeys[0])

		// Find non-existent type
		stringKeys := KeysByType[string](s)
		assert.Len(t, stringKeys, 0)
	})

	t.Run("find_keys_by_schema_simple", func(t *testing.T) {
		// Define a simple schema pattern matching Simple struct
		simplePattern := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"ID":   map[string]interface{}{"type": "string"},
				"Data": map[string]interface{}{"type": "integer"},
			},
		}

		keys := s.FindKeysBySchema(simplePattern)
		assert.Len(t, keys, 2, "Should find simple1 and simple2 based on full schema")
		assert.Contains(t, keys, "simple1")
		assert.Contains(t, keys, "simple2")
	})

	t.Run("find_keys_by_schema_partial", func(t *testing.T) {
		// Define a partial schema pattern (just requires an ID string)
		partialPattern := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"ID": map[string]interface{}{"type": "string"},
			},
		}

		keys := s.FindKeysBySchema(partialPattern)
		// Should match both Simple and Complex types as both have an ID string field
		assert.Len(t, keys, 3, "Should find simple1, simple2, and complex1 based on partial schema")
		assert.Contains(t, keys, "simple1")
		assert.Contains(t, keys, "simple2")
		assert.Contains(t, keys, "complex1")
	})

	t.Run("find_keys_by_schema_nested", func(t *testing.T) {
		// Define a schema pattern matching the Complex struct including nested
		complexPattern := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"ID":    map[string]interface{}{"type": "string"},
				"Label": map[string]interface{}{"type": "string"},
				"Simple": map[string]interface{}{ // Nested object schema
					"type": "object",
					"properties": map[string]interface{}{
						"ID":   map[string]interface{}{"type": "string"},
						"Data": map[string]interface{}{"type": "integer"},
					},
				},
			},
		}

		keys := s.FindKeysBySchema(complexPattern)
		assert.Len(t, keys, 1, "Should find complex1 based on full nested schema")
		if len(keys) > 0 {
			assert.Equal(t, "complex1", keys[0])
		}
	})

	t.Run("find_keys_by_schema_no_match", func(t *testing.T) {
		// Define a schema pattern that won't match anything
		noMatchPattern := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"NonExistent": map[string]interface{}{"type": "string"},
			},
		}

		keys := s.FindKeysBySchema(noMatchPattern)
		assert.Len(t, keys, 0, "Should find no keys for a non-matching schema")
	})

	t.Run("find_keys_by_schema_with_ttl", func(t *testing.T) {
		// Add an entry with TTL
		err := s.PutWithTTL("simple_ttl", Simple{ID: "s_ttl", Data: 30}, 1*time.Millisecond)
		assert.NoError(t, err)

		simplePattern := map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{"ID": map[string]interface{}{"type": "string"}},
		}

		// Should find it initially
		keys := s.FindKeysBySchema(simplePattern)
		assert.Contains(t, keys, "simple_ttl")

		// Wait for expiration
		time.Sleep(5 * time.Millisecond)

		// Should not find it after expiration
		keys = s.FindKeysBySchema(simplePattern)
		assert.NotContains(t, keys, "simple_ttl")
	})
}

func TestStorePropertyFiltering(t *testing.T) {
	s := NewKVStore()

	// Setup data with properties
	meta1 := NewMetadata()
	meta1.SetProperty("status", "active")
	meta1.SetProperty("count", 10)
	s.PutWithMetadata("item1", "value1", meta1)

	meta2 := NewMetadata()
	meta2.SetProperty("status", "inactive")
	meta2.SetProperty("count", 20)
	s.PutWithMetadata("item2", "value2", meta2)

	meta3 := NewMetadata()
	meta3.SetProperty("status", "active")
	meta3.SetProperty("level", 5.5)
	s.PutWithMetadata("item3", "value3", meta3)

	// Item with no relevant properties
	s.Put("item4", "value4")

	t.Run("GetProperty_success", func(t *testing.T) {
		status, err := s.GetProperty("item1", "status")
		assert.NoError(t, err)
		assert.Equal(t, "active", status)

		count, err := s.GetProperty("item2", "count")
		assert.NoError(t, err)
		assert.Equal(t, 20, count)

		level, err := s.GetProperty("item3", "level")
		assert.NoError(t, err)
		assert.Equal(t, 5.5, level)
	})

	t.Run("GetProperty_key_not_found", func(t *testing.T) {
		_, err := s.GetProperty("nonexistent", "status")
		assert.Error(t, err)
		assert.Equal(t, ErrNotFound, err)
	})

	t.Run("GetProperty_property_not_found", func(t *testing.T) {
		_, err := s.GetProperty("item1", "nonexistent_prop")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "property 'nonexistent_prop' not found")
	})

	t.Run("GetProperty_no_metadata", func(t *testing.T) {
		// GetProperty automatically creates metadata if it doesn't exist,
		// so trying to get a property will result in 'property not found'.
		_, err := s.GetProperty("item4", "status")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "property 'status' not found")
	})

	t.Run("FindKeysByProperty_string", func(t *testing.T) {
		activeKeys := s.FindKeysByProperty("status", "active")
		assert.Len(t, activeKeys, 2)
		assert.Contains(t, activeKeys, "item1")
		assert.Contains(t, activeKeys, "item3")

		inactiveKeys := s.FindKeysByProperty("status", "inactive")
		assert.Len(t, inactiveKeys, 1)
		assert.Equal(t, "item2", inactiveKeys[0])

		noMatchKeys := s.FindKeysByProperty("status", "pending")
		assert.Empty(t, noMatchKeys)
	})

	t.Run("FindKeysByProperty_int", func(t *testing.T) {
		count10Keys := s.FindKeysByProperty("count", 10)
		assert.Len(t, count10Keys, 1)
		assert.Equal(t, "item1", count10Keys[0])

		count20Keys := s.FindKeysByProperty("count", 20)
		assert.Len(t, count20Keys, 1)
		assert.Equal(t, "item2", count20Keys[0])

		noMatchKeys := s.FindKeysByProperty("count", 30)
		assert.Empty(t, noMatchKeys)
	})

	t.Run("FindKeysByProperty_float", func(t *testing.T) {
		levelKeys := s.FindKeysByProperty("level", 5.5)
		assert.Len(t, levelKeys, 1)
		assert.Equal(t, "item3", levelKeys[0])

		noMatchKeys := s.FindKeysByProperty("level", 6.0)
		assert.Empty(t, noMatchKeys)
	})

	t.Run("FindKeysByProperty_non_existent_prop", func(t *testing.T) {
		keys := s.FindKeysByProperty("nonexistent_prop", "value")
		assert.Empty(t, keys)
	})

	t.Run("FindKeysByProperty_no_metadata", func(t *testing.T) {
		keys := s.FindKeysByProperty("status", "active")
		assert.NotContains(t, keys, "item4") // item4 has no metadata
	})

	t.Run("FindKeysByProperty_with_ttl", func(t *testing.T) {
		metaTTL := NewMetadata()
		metaTTL.SetProperty("status", "expiring")
		err := s.PutWithTTLAndMetadata("item_ttl", "v_ttl", 1*time.Millisecond, metaTTL)
		assert.NoError(t, err)

		// Should find it initially
		keys := s.FindKeysByProperty("status", "expiring")
		assert.Len(t, keys, 1)
		assert.Equal(t, "item_ttl", keys[0])

		// Wait for expiration
		time.Sleep(5 * time.Millisecond)

		// Should not find it after expiration
		keys = s.FindKeysByProperty("status", "expiring")
		assert.Empty(t, keys)
	})
}

func TestGetTypeSchema(t *testing.T) {
	type SchemaTestStruct struct {
		FieldA string `json:"field_a"`
		FieldB int    `json:"field_b,omitempty"`
	}

	s := NewKVStore()

	// Add an entry
	instance := SchemaTestStruct{FieldA: "test", FieldB: 123}
	err := s.Put("schemaKey", instance)
	assert.NoError(t, err)

	t.Run("get_schema_success", func(t *testing.T) {
		schema, err := s.GetTypeSchema("schemaKey")
		assert.NoError(t, err)
		assert.NotNil(t, schema)

		// Basic validation of the schema structure (as map[string]interface{})
		schemaMap, ok := schema.(map[string]interface{})
		assert.True(t, ok, "Schema should be a map")
		assert.Equal(t, "object", schemaMap["type"], "Schema type should be object")

		properties, ok := schemaMap["properties"].(map[string]interface{})
		assert.True(t, ok, "Schema properties should be a map")
		assert.Contains(t, properties, "field_a", "Schema should have field_a")
		assert.Contains(t, properties, "field_b", "Schema should have field_b")

		fieldASchema, ok := properties["field_a"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "string", fieldASchema["type"], "Field A type should be string")

		fieldBSchema, ok := properties["field_b"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "integer", fieldBSchema["type"], "Field B type should be integer")
	})

	t.Run("get_schema_key_not_found", func(t *testing.T) {
		_, err := s.GetTypeSchema("nonexistent")
		assert.Error(t, err)
		assert.Equal(t, ErrNotFound, err)
	})

	t.Run("get_schema_expired_key", func(t *testing.T) {
		err := s.PutWithTTL("expiredSchemaKey", instance, 1*time.Millisecond)
		assert.NoError(t, err)
		time.Sleep(5 * time.Millisecond)

		_, err = s.GetTypeSchema("expiredSchemaKey")
		assert.Error(t, err)
		assert.Equal(t, ErrExpired, err)
	})

	t.Run("get_schema_empty_key", func(t *testing.T) {
		_, err := s.GetTypeSchema("")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "key cannot be empty")
	})
}

func TestSchemaMatch(t *testing.T) {
	// Define target schema (representing a complex object)
	targetSchema := map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{ // Note: Using map[string]interface{} directly
			"id":       map[string]interface{}{"type": "string"},
			"name":     map[string]interface{}{"type": "string"},
			"count":    map[string]interface{}{"type": "integer"},
			"isActive": map[string]interface{}{"type": "boolean"},
			"nested": map[string]interface{}{ // Nested object
				"type": "object",
				"properties": map[string]interface{}{ // Note: Using map[string]interface{} directly
					"label": map[string]interface{}{"type": "string"},
				},
			},
		},
	}

	t.Run("exact_match", func(t *testing.T) {
		// Pattern is identical to target
		pattern := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{ // Note: Using map[string]interface{} directly
				"id":       map[string]interface{}{"type": "string"},
				"name":     map[string]interface{}{"type": "string"},
				"count":    map[string]interface{}{"type": "integer"},
				"isActive": map[string]interface{}{"type": "boolean"},
				"nested": map[string]interface{}{ // Nested object
					"type": "object",
					"properties": map[string]interface{}{ // Note: Using map[string]interface{} directly
						"label": map[string]interface{}{"type": "string"},
					},
				},
			},
		}
		assert.True(t, SchemaMatch(targetSchema, pattern), "Identical schemas should match")
	})

	t.Run("partial_match_top_level", func(t *testing.T) {
		// Pattern has a subset of top-level properties
		pattern := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{ // Note: Using map[string]interface{} directly
				"id":   map[string]interface{}{"type": "string"},
				"name": map[string]interface{}{"type": "string"},
			},
		}
		assert.True(t, SchemaMatch(targetSchema, pattern), "Partial top-level schema should match")
	})

	t.Run("partial_match_nested", func(t *testing.T) {
		// Pattern matches a required top-level field and the nested structure
		pattern := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{ // Note: Using map[string]interface{} directly
				"id": map[string]interface{}{"type": "string"},
				"nested": map[string]interface{}{ // Nested object
					"type": "object",
					"properties": map[string]interface{}{ // Note: Using map[string]interface{} directly
						"label": map[string]interface{}{"type": "string"},
					},
				},
			},
		}
		assert.True(t, SchemaMatch(targetSchema, pattern), "Partial schema including nested should match")
	})

	t.Run("no_match_missing_property", func(t *testing.T) {
		// Pattern requires a property not in the target
		pattern := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{ // Note: Using map[string]interface{} directly
				"id":          map[string]interface{}{"type": "string"},
				"nonexistent": map[string]interface{}{"type": "string"},
			},
		}
		assert.False(t, SchemaMatch(targetSchema, pattern), "Schema with non-existent required property should not match")
	})

	t.Run("no_match_wrong_type", func(t *testing.T) {
		// Pattern requires a property with a different type
		pattern := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{ // Note: Using map[string]interface{} directly
				"id": map[string]interface{}{"type": "integer"}, // Wrong type
			},
		}
		// SchemaMatch now checks for property type matches, so this should fail
		assert.False(t, SchemaMatch(targetSchema, pattern), "SchemaMatch should fail when property types don't match")
	})

	t.Run("no_match_missing_nested_property", func(t *testing.T) {
		// Pattern requires a nested property that doesn't exist
		pattern := map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{ // Note: Using map[string]interface{} directly
				"nested": map[string]interface{}{ // Nested object
					"type": "object",
					"properties": map[string]interface{}{ // Note: Using map[string]interface{} directly
						"nonexistent": map[string]interface{}{"type": "string"},
					},
				},
			},
		}
		assert.False(t, SchemaMatch(targetSchema, pattern), "Schema requiring non-existent nested property should not match")
	})

	t.Run("empty_pattern", func(t *testing.T) {
		pattern := map[string]interface{}{}
		assert.True(t, SchemaMatch(targetSchema, pattern), "Empty pattern should match any object schema")

		pattern = map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
		assert.True(t, SchemaMatch(targetSchema, pattern), "Pattern with empty properties should match")
	})

	t.Run("non_object_types", func(t *testing.T) {
		targetStringSchema := map[string]interface{}{"type": "string"}
		patternStringSchema := map[string]interface{}{"type": "string"}
		patternIntSchema := map[string]interface{}{"type": "integer"}

		assert.True(t, SchemaMatch(targetStringSchema, patternStringSchema), "Identical non-object types should match")
		assert.False(t, SchemaMatch(targetStringSchema, patternIntSchema), "Different non-object types should not match")
		assert.False(t, SchemaMatch(targetSchema, patternStringSchema), "Object and non-object types should not match")
	})

	t.Run("invalid_input", func(t *testing.T) {
		// Test with non-map inputs where assertToMap would fail
		assert.False(t, SchemaMatch("not a map", targetSchema), "Invalid target should not match")
		assert.False(t, SchemaMatch(targetSchema, "not a map"), "Invalid pattern should not match")
		assert.False(t, SchemaMatch(nil, targetSchema), "Nil target should not match")
		assert.False(t, SchemaMatch(targetSchema, nil), "Nil pattern should not match")
	})
}

func TestAssertToMap(t *testing.T) {
	t.Run("already_map", func(t *testing.T) {
		input := map[string]interface{}{"key": "value"}
		m, ok := assertToMap(input)
		assert.True(t, ok)
		assert.Equal(t, input, m)
	})

	t.Run("struct_to_map", func(t *testing.T) {
		type MyStruct struct {
			Field string `json:"field"`
		}
		input := MyStruct{Field: "data"}
		m, ok := assertToMap(input)
		assert.True(t, ok)
		assert.NotNil(t, m)
		assert.Equal(t, "data", m["field"])
	})

	t.Run("pointer_to_struct_to_map", func(t *testing.T) {
		type MyStruct struct {
			Field string `json:"field"`
		}
		input := &MyStruct{Field: "data_ptr"}
		m, ok := assertToMap(input)
		assert.True(t, ok)
		assert.NotNil(t, m)
		assert.Equal(t, "data_ptr", m["field"])
	})

	t.Run("invalid_input_string", func(t *testing.T) {
		_, ok := assertToMap("just a string")
		assert.False(t, ok)
	})

	t.Run("invalid_input_nil", func(t *testing.T) {
		_, ok := assertToMap(nil)
		assert.False(t, ok)
	})
}

// TestCloning tests the store cloning functionality
func TestCloning(t *testing.T) {
	// Define test types
	type User struct {
		Name  string
		Email string
		Age   int
	}

	type Nested struct {
		Title string
		Count int
		Data  map[string]string
	}

	// Setup the original store with various data types and options
	originalStore := NewKVStore()

	// 1. Basic value
	err := originalStore.Put("simple", "hello world")
	assert.NoError(t, err)

	// 2. Struct
	user := User{Name: "John Doe", Email: "john@example.com", Age: 30}
	err = originalStore.Put("user", user)
	assert.NoError(t, err)

	// 3. Entry with TTL
	err = originalStore.PutWithTTL("expiring", 42, 1*time.Hour)
	assert.NoError(t, err)

	// 4. Entry with metadata
	meta := NewMetadata()
	meta.AddTag("important")
	meta.SetProperty("priority", 1)
	meta.Description = "Test metadata"

	nested := Nested{
		Title: "Test Nested",
		Count: 5,
		Data: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}
	err = originalStore.PutWithMetadata("nested", nested, meta)
	assert.NoError(t, err)

	// Test basic Clone() functionality
	t.Run("clone_basic", func(t *testing.T) {
		// Clone the store
		clonedStore := originalStore.Clone()

		// Verify store count
		assert.Equal(t, originalStore.Count(), clonedStore.Count())

		// Check every key exists
		assert.Contains(t, clonedStore.ListKeys(), "simple")
		assert.Contains(t, clonedStore.ListKeys(), "user")
		assert.Contains(t, clonedStore.ListKeys(), "expiring")
		assert.Contains(t, clonedStore.ListKeys(), "nested")

		// Check basic value
		simpleVal, err := Get[string](clonedStore, "simple")
		assert.NoError(t, err)
		assert.Equal(t, "hello world", simpleVal)

		// Check struct data
		userVal, err := Get[User](clonedStore, "user")
		assert.NoError(t, err)
		assert.Equal(t, "John Doe", userVal.Name)
		assert.Equal(t, "john@example.com", userVal.Email)
		assert.Equal(t, 30, userVal.Age)

		// Check nested struct
		nestedVal, err := Get[Nested](clonedStore, "nested")
		assert.NoError(t, err)
		assert.Equal(t, "Test Nested", nestedVal.Title)
		assert.Equal(t, 5, nestedVal.Count)
		assert.Equal(t, "value1", nestedVal.Data["key1"])
		assert.Equal(t, "value2", nestedVal.Data["key2"])
	})

	// Test metadata cloning
	t.Run("clone_metadata", func(t *testing.T) {
		clonedStore := originalStore.Clone()

		// Check metadata was cloned
		meta, err := clonedStore.GetMetadata("nested")
		assert.NoError(t, err)
		assert.NotNil(t, meta)

		// Check tags
		assert.Contains(t, meta.Tags, "important")

		// Check properties
		priority, exists := meta.GetProperty("priority")
		assert.True(t, exists)
		assert.Equal(t, 1, priority)

		// Check description
		assert.Equal(t, "Test metadata", meta.Description)
	})

	// Test reference isolation
	t.Run("clone_isolation", func(t *testing.T) {
		clonedStore := originalStore.Clone()

		// 1. Modify original - should not affect clone
		originalStore.Put("simple", "modified value")
		simpleVal, err := Get[string](clonedStore, "simple")
		assert.NoError(t, err)
		assert.Equal(t, "hello world", simpleVal, "Clone should not be affected by changes to original")

		// 2. Modify clone - should not affect original
		clonedStore.Put("user", User{Name: "Jane Doe", Email: "jane@example.com", Age: 25})
		userVal, err := Get[User](originalStore, "user")
		assert.NoError(t, err)
		assert.Equal(t, "John Doe", userVal.Name, "Original should not be affected by changes to clone")

		// 3. Modify metadata in original - should not affect clone
		origMeta, _ := originalStore.GetMetadata("nested")
		origMeta.AddTag("new-tag")

		cloneMeta, _ := clonedStore.GetMetadata("nested")
		assert.NotContains(t, cloneMeta.Tags, "new-tag", "Clone metadata should not be affected by changes to original metadata")

		// 4. Modify metadata in clone - should not affect original
		cloneMeta.AddTag("clone-only")

		origMeta, _ = originalStore.GetMetadata("nested")
		assert.NotContains(t, origMeta.Tags, "clone-only", "Original metadata should not be affected by changes to clone metadata")
	})

	// Test TTL preservation
	t.Run("clone_ttl", func(t *testing.T) {
		// Add a short TTL entry to test
		originalStore.PutWithTTL("quick-expire", "will expire", 50*time.Millisecond)

		// Clone immediately
		clonedStore := originalStore.Clone()

		// Verify entry exists in both stores
		_, err := Get[string](originalStore, "quick-expire")
		assert.NoError(t, err)

		_, err = Get[string](clonedStore, "quick-expire")
		assert.NoError(t, err)

		// Wait for expiration
		time.Sleep(100 * time.Millisecond)

		// Verify entry expired in both stores
		_, err = Get[string](originalStore, "quick-expire")
		assert.Error(t, err)
		assert.Equal(t, ErrExpired, err)

		_, err = Get[string](clonedStore, "quick-expire")
		assert.Error(t, err)
		assert.Equal(t, ErrExpired, err)
	})

	// Test CloneFrom
	t.Run("clone_from", func(t *testing.T) {
		// Test with a valid store
		newStore := CloneFrom(originalStore)
		assert.Equal(t, originalStore.Count(), newStore.Count())

		// Test with nil
		nilStore := CloneFrom(nil)
		assert.NotNil(t, nilStore)
		assert.Equal(t, 0, nilStore.Count())
	})
}

// TestCopyFrom tests copying entries from one store to another
func TestCopyFrom(t *testing.T) {
	// Define test types
	type Item struct {
		ID    string
		Value int
	}

	// Setup the source store
	sourceStore := NewKVStore()

	// Populate source store
	err := sourceStore.Put("key1", "value1")
	assert.NoError(t, err)

	err = sourceStore.Put("key2", 123)
	assert.NoError(t, err)

	item := Item{ID: "item1", Value: 42}
	err = sourceStore.Put("item", item)
	assert.NoError(t, err)

	meta := NewMetadata()
	meta.AddTag("source")
	meta.SetProperty("source-prop", true)
	err = sourceStore.PutWithMetadata("meta-key", "metadata-value", meta)
	assert.NoError(t, err)

	err = sourceStore.PutWithTTL("ttl-key", "expires", 1*time.Hour)
	assert.NoError(t, err)

	t.Run("copy_to_empty_store", func(t *testing.T) {
		// Create empty destination store
		destStore := NewKVStore()

		// Copy from source to destination
		copied, err := destStore.CopyFrom(sourceStore)
		assert.NoError(t, err)
		assert.Equal(t, 5, copied) // Should copy all 5 entries

		// Verify all keys were copied
		assert.Equal(t, 5, destStore.Count())

		// Verify values were copied correctly
		val1, err := Get[string](destStore, "key1")
		assert.NoError(t, err)
		assert.Equal(t, "value1", val1)

		val2, err := Get[int](destStore, "key2")
		assert.NoError(t, err)
		assert.Equal(t, 123, val2)

		itemVal, err := Get[Item](destStore, "item")
		assert.NoError(t, err)
		assert.Equal(t, "item1", itemVal.ID)
		assert.Equal(t, 42, itemVal.Value)

		// Verify metadata was copied
		meta, err := destStore.GetMetadata("meta-key")
		assert.NoError(t, err)
		assert.True(t, meta.HasTag("source"))
		prop, exists := meta.GetProperty("source-prop")
		assert.True(t, exists)
		assert.Equal(t, true, prop)
	})

	t.Run("copy_with_existing_entries", func(t *testing.T) {
		// Create destination with some existing entries
		destStore := NewKVStore()

		// Add some entries to destination, including one with same key as source
		err := destStore.Put("key1", "dest-value") // Same key as in source
		assert.NoError(t, err)

		err = destStore.Put("dest-only", "only in dest")
		assert.NoError(t, err)

		// Copy from source to destination
		copied, err := destStore.CopyFrom(sourceStore)
		assert.NoError(t, err)
		assert.Equal(t, 4, copied) // Should copy 4 entries (skipping key1)

		// Verify destination has both unique keys from source and dest
		assert.Equal(t, 6, destStore.Count())

		// Verify the conflicting key was not overwritten
		val1, err := Get[string](destStore, "key1")
		assert.NoError(t, err)
		assert.Equal(t, "dest-value", val1, "Existing value should not be overwritten")

		// Verify original dest entry still exists
		destOnly, err := Get[string](destStore, "dest-only")
		assert.NoError(t, err)
		assert.Equal(t, "only in dest", destOnly)

		// Verify new entries from source were added
		val2, err := Get[int](destStore, "key2")
		assert.NoError(t, err)
		assert.Equal(t, 123, val2)
	})

	t.Run("copy_with_expired_entries", func(t *testing.T) {
		// Create source with an expired entry
		tempSource := NewKVStore()

		// Add a regular entry
		err := tempSource.Put("permanent", "stays")
		assert.NoError(t, err)

		// Add an entry that will expire
		err = tempSource.PutWithTTL("will-expire", "gone", 10*time.Millisecond)
		assert.NoError(t, err)

		// Wait for expiration
		time.Sleep(20 * time.Millisecond)

		// Create destination
		destStore := NewKVStore()

		// Copy from source to destination
		copied, err := destStore.CopyFrom(tempSource)
		assert.NoError(t, err)
		assert.Equal(t, 1, copied, "Should only copy the non-expired entry")

		// Verify only non-expired entry exists
		assert.Equal(t, 1, destStore.Count())
		assert.Contains(t, destStore.ListKeys(), "permanent")
		assert.NotContains(t, destStore.ListKeys(), "will-expire")
	})

	t.Run("copy_with_nil_source", func(t *testing.T) {
		destStore := NewKVStore()

		// Try to copy from nil source
		copied, err := destStore.CopyFrom(nil)
		assert.Error(t, err)
		assert.Equal(t, 0, copied)
		assert.Contains(t, err.Error(), "source store cannot be nil")
	})

	t.Run("reference_isolation", func(t *testing.T) {
		// Create source and destination stores
		source := NewKVStore()
		dest := NewKVStore()

		// Add complex data to source
		nested := map[string]interface{}{
			"nested1": "value1",
			"nested2": 42,
		}
		err := source.Put("complex", nested)
		assert.NoError(t, err)

		// Copy to destination
		copied, err := dest.CopyFrom(source)
		assert.NoError(t, err)
		assert.Equal(t, 1, copied)

		// Modify source data
		nested["nested1"] = "modified"
		err = source.Put("complex", nested)
		assert.NoError(t, err)

		// Get data from destination - should not be affected
		result, err := Get[map[string]interface{}](dest, "complex")
		assert.NoError(t, err)
		assert.Equal(t, "value1", result["nested1"], "Destination data should not be affected by source changes")
	})
}

// TestCopyFromWithOverwrite tests copying entries with overwrite from one store to another
func TestCopyFromWithOverwrite(t *testing.T) {
	// Setup the source store
	sourceStore := NewKVStore()

	// Populate source store
	err := sourceStore.Put("key1", "source-value1")
	assert.NoError(t, err)

	err = sourceStore.Put("key2", 123)
	assert.NoError(t, err)

	meta := NewMetadata()
	meta.AddTag("source-tag")
	err = sourceStore.PutWithMetadata("meta-key", "source-meta-value", meta)
	assert.NoError(t, err)

	// Setup the destination store
	destStore := NewKVStore()

	// Add some entries to destination with overlapping keys
	err = destStore.Put("key1", "dest-value1") // Same key as in source
	assert.NoError(t, err)

	// Different destination metadata
	destMeta := NewMetadata()
	destMeta.AddTag("dest-tag")
	err = destStore.PutWithMetadata("meta-key", "dest-meta-value", destMeta)
	assert.NoError(t, err)

	err = destStore.Put("dest-only", "only in dest")
	assert.NoError(t, err)

	t.Run("copy_with_overwrite", func(t *testing.T) {
		// Make a copy of the destination for this test
		testDest := destStore.Clone()

		// Copy from source to destination with overwrite
		copied, overwritten, err := testDest.CopyFromWithOverwrite(sourceStore)
		assert.NoError(t, err)
		assert.Equal(t, 1, copied, "Should copy 1 new entry")
		assert.Equal(t, 2, overwritten, "Should overwrite 2 entries")

		// Verify destination has entries from both
		assert.Equal(t, 4, testDest.Count())

		// Verify the conflicting keys were overwritten with source values
		val1, err := Get[string](testDest, "key1")
		assert.NoError(t, err)
		assert.Equal(t, "source-value1", val1, "Existing value should be overwritten")

		metaVal, err := Get[string](testDest, "meta-key")
		assert.NoError(t, err)
		assert.Equal(t, "source-meta-value", metaVal)

		// Verify metadata was overwritten
		meta, err := testDest.GetMetadata("meta-key")
		assert.NoError(t, err)
		assert.True(t, meta.HasTag("source-tag"))
		assert.False(t, meta.HasTag("dest-tag"))

		// Verify original dest entry still exists
		destOnly, err := Get[string](testDest, "dest-only")
		assert.NoError(t, err)
		assert.Equal(t, "only in dest", destOnly)

		// Verify new entries from source were added
		val2, err := Get[int](testDest, "key2")
		assert.NoError(t, err)
		assert.Equal(t, 123, val2)
	})

	t.Run("metadata_isolation_after_overwrite", func(t *testing.T) {
		// Make a copy of the destination for this test
		testDest := destStore.Clone()

		// Copy from source to destination with overwrite
		copied, overwritten, err := testDest.CopyFromWithOverwrite(sourceStore)
		assert.NoError(t, err)
		assert.Greater(t, copied+overwritten, 0, "Should have copied or overwritten at least one entry")

		// Get metadata from destination
		destMeta, err := testDest.GetMetadata("meta-key")
		assert.NoError(t, err)

		// Get metadata from source
		sourceMeta, err := sourceStore.GetMetadata("meta-key")
		assert.NoError(t, err)

		// Modify source metadata
		sourceMeta.AddTag("new-source-tag")

		// Verify destination metadata not affected
		assert.False(t, destMeta.HasTag("new-source-tag"))

		// Modify destination metadata
		destMeta.AddTag("new-dest-tag")

		// Verify source metadata not affected
		assert.False(t, sourceMeta.HasTag("new-dest-tag"))
	})

	t.Run("copy_with_nil_source", func(t *testing.T) {
		testDest := NewKVStore()

		// Try to copy from nil source
		copied, overwritten, err := testDest.CopyFromWithOverwrite(nil)
		assert.Error(t, err)
		assert.Equal(t, 0, copied)
		assert.Equal(t, 0, overwritten)
		assert.Contains(t, err.Error(), "source store cannot be nil")
	})

	t.Run("expired_entries", func(t *testing.T) {
		// Create source with an expired entry
		tempSource := NewKVStore()

		// Add a regular entry
		err := tempSource.Put("permanent", "stays")
		assert.NoError(t, err)

		// Add an entry that will expire
		err = tempSource.PutWithTTL("will-expire", "gone", 10*time.Millisecond)
		assert.NoError(t, err)

		// Wait for expiration
		time.Sleep(20 * time.Millisecond)

		// Create destination
		testDest := NewKVStore()

		// Copy from source to destination
		copied, overwritten, err := testDest.CopyFromWithOverwrite(tempSource)
		assert.NoError(t, err)
		assert.Equal(t, 1, copied, "Should only copy the non-expired entry")
		assert.Equal(t, 0, overwritten)

		// Verify only non-expired entry exists
		assert.Equal(t, 1, testDest.Count())
		assert.Contains(t, testDest.ListKeys(), "permanent")
		assert.NotContains(t, testDest.ListKeys(), "will-expire")
	})
}
