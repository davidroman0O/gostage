package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/invopop/jsonschema"
)

// KVStore is a threadsafe, type‑aware in‑memory store.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]entry
}

// NewKVStore constructs an empty store.
func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]entry)}
}

// Put stores any Go value under key, capturing its concrete type.
func (s *KVStore) Put(key string, value any) error {
	return s.PutWithTTL(key, value, 0)
}

// PutWithMetadata stores a value with metadata
func (s *KVStore) PutWithMetadata(key string, value any, metadata *Metadata) error {
	return s.PutWithTTLAndMetadata(key, value, 0, metadata)
}

// PutWithTTL stores any Go value under key with a specified time-to-live duration.
// If ttl is 0 or negative, the entry will not expire.
func (s *KVStore) PutWithTTL(key string, value any, ttl time.Duration) error {
	return s.PutWithTTLAndMetadata(key, value, ttl, nil)
}

// PutWithTTLAndMetadata stores any Go value with both TTL and metadata
func (s *KVStore) PutWithTTLAndMetadata(key string, value any, ttl time.Duration, metadata *Metadata) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	// Special handling for nil values
	if value == nil {
		var expiresAt *time.Time
		if ttl > 0 {
			exp := time.Now().Add(ttl)
			expiresAt = &exp
		}

		// Use the provided metadata or preserve existing
		var meta *Metadata
		if metadata != nil {
			meta = metadata
		} else {
			s.mu.RLock()
			if existingEntry, exists := s.data[key]; exists && existingEntry.metadata != nil {
				meta = existingEntry.metadata
				// Update the UpdatedAt timestamp
				meta.UpdatedAt = time.Now()
			}
			s.mu.RUnlock()
		}

		s.mu.Lock()
		s.data[key] = entry{
			typ:       nil,
			typeKind:  reflect.Invalid,
			value:     nil,
			expiresAt: expiresAt,
			metadata:  meta,
		}
		s.mu.Unlock()
		return nil
	}

	t := reflect.TypeOf(value)
	k := t.Kind() // Get the kind once

	var expiresAt *time.Time
	if ttl > 0 {
		exp := time.Now().Add(ttl)
		expiresAt = &exp
	}

	// Use the provided metadata or create a new one
	var meta *Metadata
	if metadata != nil {
		meta = metadata
	}

	s.mu.Lock()
	// If entry already exists and has metadata, preserve it unless new metadata is provided
	if existingEntry, exists := s.data[key]; exists && existingEntry.metadata != nil && metadata == nil {
		meta = existingEntry.metadata
		// Update the UpdatedAt timestamp
		meta.UpdatedAt = time.Now()
	}
	// Store the actual value directly - no serialization
	s.data[key] = entry{typ: t, typeKind: k, value: value, expiresAt: expiresAt, metadata: meta}
	s.mu.Unlock()
	return nil
}

// Get retrieves a value of type T for the given key.
func Get[T any](s *KVStore, key string) (T, error) {
	var zero T
	if key == "" {
		return zero, errors.New("key cannot be empty")
	}

	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return zero, ErrNotFound
	}

	// Check if the entry has expired
	if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
		s.Delete(key)
		return zero, ErrExpired
	}

	// Get the requested type
	want := reflect.TypeOf((*T)(nil)).Elem()
	wantKind := want.Kind()

	// If requesting an interface, check if the stored type implements it
	if wantKind == reflect.Interface {
		// Use the cached typeKind to check if the stored object can be an interface implementation
		if !canImplementInterface(e.typeKind) {
			return zero, fmt.Errorf("%w: wanted interface %v, but stored value type %v (kind: %v) can't implement interfaces",
				ErrTypeMismatch, want, e.typ, e.typeKind)
		}

		// Do the actual interface implementation check
		if !e.typ.Implements(want) {
			return zero, fmt.Errorf("%w: wanted interface %v, got %v which doesn't implement it",
				ErrTypeMismatch, want, e.typ)
		}

		// Direct type assertion - no serialization/deserialization needed
		result, ok := e.value.(T)
		if !ok {
			return zero, fmt.Errorf("type assertion failed: %T cannot be converted to requested interface", e.value)
		}

		return result, nil
	}

	// For non-interface types, require an exact match
	if e.typ != want {
		return zero, fmt.Errorf("%w: wanted %v (kind: %v), got %v (kind: %v)",
			ErrTypeMismatch, want, wantKind, e.typ, e.typeKind)
	}

	// For exact type matches, do a direct type assertion - no serialization/deserialization
	result, ok := e.value.(T)
	if !ok {
		return zero, fmt.Errorf("type assertion failed: %T cannot be converted to %v", e.value, want)
	}

	return result, nil
}

// Helper function to determine if a reflect.Kind can implement interfaces
func canImplementInterface(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
		reflect.Bool, reflect.String, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return false
	case reflect.Interface, reflect.Ptr, reflect.Struct, reflect.Map, reflect.Array, reflect.Slice:
		return true
	default:
		return false
	}
}

// GetOrDefault retrieves a value of type T for the given key.
func GetOrDefault[T any](s *KVStore, key string, defaultValue T) (T, error) {
	value, err := Get[T](s, key)
	if err == ErrNotFound || err == ErrExpired {
		return defaultValue, nil
	}
	return value, err
}

// Delete removes a key from the store.
func (s *KVStore) Delete(key string) bool {
	if key == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.data[key]
	if exists {
		delete(s.data, key)
		return true
	}
	return false
}

// Clear removes all keys from the store.
func (s *KVStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]entry)
}

// ListKeys returns all stored keys.
func (s *KVStore) ListKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]string, 0, len(s.data))
	for k, e := range s.data {
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}
		out = append(out, k)
	}
	return out
}

// Count returns the number of valid entries in the store.
func (s *KVStore) Count() int {
	return len(s.ListKeys())
}

// ListTypes returns the set of all concrete types stored.
func (s *KVStore) ListTypes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := map[reflect.Type]struct{}{}
	out := []string{}

	for _, e := range s.data {
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}

		if _, ok := seen[e.typ]; ok {
			continue
		}
		seen[e.typ] = struct{}{}
		out = append(out, e.typ.String())
	}
	return out
}

// KeysByType returns all keys whose stored value has type T.
func KeysByType[T any](s *KVStore) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	want := reflect.TypeOf((*T)(nil)).Elem()
	keys := []string{}

	for k, e := range s.data {
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}

		if e.typ == want {
			keys = append(keys, k)
		}
	}
	return keys
}

// GetTypeSchema returns a JSON Schema representation of the stored value's type.
func (s *KVStore) GetTypeSchema(key string) (interface{}, error) {
	if key == "" {
		return nil, errors.New("key cannot be empty")
	}

	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return nil, ErrNotFound
	}

	if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
		s.Delete(key)
		return nil, ErrExpired
	}

	return TypeToSchema(e.typ), nil
}

// TypeToSchema converts a reflect.Type to a JSON schema.
func TypeToSchema(t reflect.Type) interface{} {
	instance := reflect.New(t).Interface()
	reflector := jsonschema.Reflector{
		ExpandedStruct:            true,
		DoNotReference:            true,  // Avoid using $ref, which can make schema validation more complex
		AllowAdditionalProperties: false, // Strictly match schema properties
	}

	// Get the schema and convert it to a map
	schema := reflector.Reflect(instance)

	// Marshal and unmarshal to convert to a map[string]interface{}
	data, err := json.Marshal(schema)
	if err != nil {
		// Return a basic schema if marshaling fails
		return map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{},
		}
	}

	var schemaMap map[string]interface{}
	if err := json.Unmarshal(data, &schemaMap); err != nil {
		// Return a basic schema if unmarshaling fails
		return map[string]interface{}{
			"type":       "object",
			"properties": map[string]interface{}{},
		}
	}

	// Ensure type is set correctly if missing
	if _, exists := schemaMap["type"]; !exists {
		schemaMap["type"] = "object"
	}

	// Ensure properties exists
	if _, exists := schemaMap["properties"]; !exists {
		schemaMap["properties"] = map[string]interface{}{}
	}

	return schemaMap
}

// setFieldValue sets a field value in a struct using a dot-notation path
// Returns an error if the path is invalid or type is mismatched
func setFieldValue(obj interface{}, path string, value interface{}) error {
	// Split the path into segments
	segments := strings.Split(path, ".")
	if len(segments) == 0 {
		return fmt.Errorf("empty path")
	}

	// Start with the object
	v := reflect.ValueOf(obj)

	// Navigate to the field
	for i := 0; i < len(segments); i++ {
		segment := segments[i]

		// If it's a pointer, get the element it points to
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		// Make sure we have a struct at this point
		if v.Kind() != reflect.Struct {
			return fmt.Errorf("path segment '%s' does not point to a struct", segment)
		}

		// Get the field
		field := v.FieldByName(segment)
		if !field.IsValid() {
			return fmt.Errorf("no field named '%s'", segment)
		}

		// If this is the last segment, set the field value
		if i == len(segments)-1 {
			// Make sure the field is settable
			if !field.CanSet() {
				return fmt.Errorf("field '%s' cannot be set (unexported?)", segment)
			}

			// Convert the value to the field's type if needed
			valueOfValue := reflect.ValueOf(value)
			if valueOfValue.Type().AssignableTo(field.Type()) {
				field.Set(valueOfValue)
				return nil
			}

			// Try to convert the value
			if valueOfValue.Type().ConvertibleTo(field.Type()) {
				field.Set(valueOfValue.Convert(field.Type()))
				return nil
			}

			return fmt.Errorf("value type %v cannot be assigned to field type %v", valueOfValue.Type(), field.Type())
		}

		// Not the last segment, so we need to traverse deeper
		v = field
	}

	return nil // Should never reach here
}

// UpdateField updates a single field in a stored object using dot notation.
func (s *KVStore) UpdateField(key string, fieldPath string, fieldValue interface{}) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	if fieldPath == "" {
		return errors.New("fieldPath cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		return ErrNotFound
	}

	if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
		delete(s.data, key)
		return ErrExpired
	}

	// Make a deep copy of the original value
	valueCopy := deepCopy(e.value)

	// Create a pointer if the value is not already a pointer
	var targetPtr interface{}
	if reflect.TypeOf(valueCopy).Kind() != reflect.Ptr {
		// Create a new pointer to this value type
		targetPtr = reflect.New(reflect.TypeOf(valueCopy)).Interface()
		// Set the element to our copy
		reflect.ValueOf(targetPtr).Elem().Set(reflect.ValueOf(valueCopy))
	} else {
		targetPtr = valueCopy
	}

	// Try to set the field value
	err := setFieldValue(targetPtr, fieldPath, fieldValue)
	if err != nil {
		return fmt.Errorf("failed to update field: %w", err)
	}

	// Store the updated value (if it wasn't a pointer, get the element)
	var updatedValue interface{}
	if reflect.TypeOf(e.value).Kind() != reflect.Ptr {
		updatedValue = reflect.ValueOf(targetPtr).Elem().Interface()
	} else {
		updatedValue = targetPtr
	}

	// Update the entry in the store
	s.data[key] = entry{
		typ:       e.typ,
		typeKind:  e.typeKind,
		value:     updatedValue,
		expiresAt: e.expiresAt,
		metadata:  e.metadata,
	}

	return nil
}

// UpdateFields updates multiple fields in a stored object.
func (s *KVStore) UpdateFields(key string, fields map[string]interface{}) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	if len(fields) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		return ErrNotFound
	}

	if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
		delete(s.data, key)
		return ErrExpired
	}

	// Make a deep copy of the original value
	valueCopy := deepCopy(e.value)

	// Create a pointer if the value is not already a pointer
	var targetPtr interface{}
	if reflect.TypeOf(valueCopy).Kind() != reflect.Ptr {
		// Create a new pointer to this value type
		targetPtr = reflect.New(reflect.TypeOf(valueCopy)).Interface()
		// Set the element to our copy
		reflect.ValueOf(targetPtr).Elem().Set(reflect.ValueOf(valueCopy))
	} else {
		targetPtr = valueCopy
	}

	// Apply each field update
	for fieldPath, fieldValue := range fields {
		err := setFieldValue(targetPtr, fieldPath, fieldValue)
		if err != nil {
			return fmt.Errorf("failed to update field '%s': %w", fieldPath, err)
		}
	}

	// Store the updated value (if it wasn't a pointer, get the element)
	var updatedValue interface{}
	if reflect.TypeOf(e.value).Kind() != reflect.Ptr {
		updatedValue = reflect.ValueOf(targetPtr).Elem().Interface()
	} else {
		updatedValue = targetPtr
	}

	// Update the entry in the store
	s.data[key] = entry{
		typ:       e.typ,
		typeKind:  e.typeKind,
		value:     updatedValue,
		expiresAt: e.expiresAt,
		metadata:  e.metadata,
	}

	return nil
}

// Merge combines this store with another, handling collisions according to the strategy.
// Returns a list of collided keys and handles metadata merging.
func (s *KVStore) Merge(other *KVStore, strategy MergeStrategy) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	collisions := []string{}

	for key, otherEntry := range other.data {
		// Check if the entry has expired
		if otherEntry.expiresAt != nil && time.Now().After(*otherEntry.expiresAt) {
			continue
		}

		_, exists := s.data[key]
		if exists {
			collisions = append(collisions, key)

			switch strategy {
			case Error:
				return collisions, fmt.Errorf("key collision on merge: %s", key)
			case Skip:
				// Keep the original entry
				continue
			case Overwrite:
				// Fall through to overwrite
			}
		}

		// Handle metadata merging
		if exists && strategy == Overwrite {
			if existingEntry, ok := s.data[key]; ok && existingEntry.metadata != nil && otherEntry.metadata != nil {
				// Merge tags (union of both sets)
				for _, tag := range otherEntry.metadata.Tags {
					found := false
					for _, existingTag := range existingEntry.metadata.Tags {
						if existingTag == tag {
							found = true
							break
						}
					}
					if !found {
						existingEntry.metadata.Tags = append(existingEntry.metadata.Tags, tag)
					}
				}

				// Merge properties (overwrite existingEntry properties with otherEntry properties)
				for k, v := range otherEntry.metadata.Properties {
					existingEntry.metadata.Properties[k] = v
				}

				// Update timestamps
				existingEntry.metadata.UpdatedAt = time.Now()

				// Create a new entry with merged metadata
				otherEntry.metadata = existingEntry.metadata
			}
		}

		// Add or overwrite the entry
		s.data[key] = otherEntry
	}

	return collisions, nil
}

// FindKeyCollisions identifies keys that exist in both stores.
func (s *KVStore) FindKeyCollisions(other *KVStore) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	var collisions []string
	for k, e := range s.data {
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}

		if otherEntry, exists := other.data[k]; exists {
			if otherEntry.expiresAt != nil && time.Now().After(*otherEntry.expiresAt) {
				continue
			}
			collisions = append(collisions, k)
		}
	}

	return collisions
}

// FindKeysBySchema returns all keys whose type schema matches the given pattern.
// Pattern can be a partial schema - entries must contain at least all fields in pattern.
func (s *KVStore) FindKeysBySchema(pattern interface{}) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []string
	for k, e := range s.data {
		// Skip expired entries
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}

		schema := TypeToSchema(e.typ)
		if SchemaMatch(schema, pattern) {
			keys = append(keys, k)
		}
	}

	return keys
}

// SchemaMatch checks if a target schema matches a pattern schema.
// Pattern can be a partial schema - target must contain at least all fields in pattern.
func SchemaMatch(target, pattern interface{}) bool {
	// Convert to maps for easier comparison
	targetMap, targetOk := assertToMap(target)
	patternMap, patternOk := assertToMap(pattern)

	if !targetOk || !patternOk {
		return false
	}

	// Look for properties in the pattern
	if patternProps, ok := patternMap["properties"].(map[string]interface{}); ok {
		targetProps, ok := targetMap["properties"].(map[string]interface{})
		if !ok {
			return false
		}

		// All properties in pattern must exist in target
		for propName, propPattern := range patternProps {
			propTarget, exists := targetProps[propName]
			if !exists {
				return false
			}

			// If the property is an object, recursively check
			if propPatternMap, ok := assertToMap(propPattern); ok {
				propTargetMap, ok := assertToMap(propTarget)
				if !ok {
					return false
				}

				// Check if property type matches
				if patternType, hasType := propPatternMap["type"]; hasType {
					targetType, hasTargetType := propTargetMap["type"]
					if !hasTargetType || patternType != targetType {
						return false
					}
				}

				// If it has properties, recurse
				if _, hasProps := propPatternMap["properties"]; hasProps {
					if !SchemaMatch(propTarget, propPattern) {
						return false
					}
				}
			}
		}
		return true
	}

	// If no properties, do a simple check on type
	if patternType, ok := patternMap["type"]; ok {
		targetType, ok := targetMap["type"]
		if !ok {
			return false
		}
		return patternType == targetType
	}

	// Default to true for empty pattern
	return true
}

// assertToMap tries to convert an interface to a map[string]interface{}
func assertToMap(v interface{}) (map[string]interface{}, bool) {
	if v == nil {
		return nil, false
	}

	if m, ok := v.(map[string]interface{}); ok {
		return m, true
	}

	// Try marshaling and unmarshaling if it's not already a map
	data, err := json.Marshal(v)
	if err != nil {
		return nil, false
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, false
	}

	return m, true
}

// GetMetadata returns the metadata for a key
func (s *KVStore) GetMetadata(key string) (*Metadata, error) {
	if key == "" {
		return nil, errors.New("key cannot be empty")
	}

	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return nil, ErrNotFound
	}

	// Check if the entry has expired
	if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
		s.Delete(key)
		return nil, ErrExpired
	}

	// If no metadata exists, create a new one
	if e.metadata == nil {
		meta := NewMetadata()
		s.mu.Lock()
		s.data[key] = entry{typ: e.typ, typeKind: e.typeKind, value: e.value, expiresAt: e.expiresAt, metadata: meta}
		s.mu.Unlock()
		return meta, nil
	}

	return e.metadata, nil
}

// SetMetadata sets or replaces the metadata for a key
func (s *KVStore) SetMetadata(key string, metadata *Metadata) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	if metadata == nil {
		return errors.New("metadata cannot be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok {
		return ErrNotFound
	}

	// Check if the entry has expired
	if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
		delete(s.data, key)
		return ErrExpired
	}

	e.metadata = metadata
	s.data[key] = e
	return nil
}

// AddTag adds a tag to the metadata for a key
func (s *KVStore) AddTag(key string, tag string) error {
	meta, err := s.GetMetadata(key)
	if err != nil {
		return err
	}

	meta.AddTag(tag)
	return nil
}

// RemoveTag removes a tag from the metadata for a key
func (s *KVStore) RemoveTag(key string, tag string) error {
	meta, err := s.GetMetadata(key)
	if err != nil {
		return err
	}

	meta.RemoveTag(tag)
	return nil
}

// HasTag checks if a key's metadata has a specific tag
func (s *KVStore) HasTag(key string, tag string) (bool, error) {
	meta, err := s.GetMetadata(key)
	if err != nil {
		return false, err
	}

	return meta.HasTag(tag), nil
}

// FindKeysByTag returns all keys that have a specific tag in their metadata
func (s *KVStore) FindKeysByTag(tag string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []string
	for k, e := range s.data {
		// Skip expired entries
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}

		// Skip entries with no metadata or without the tag
		if e.metadata != nil && e.metadata.HasTag(tag) {
			keys = append(keys, k)
		}
	}
	return keys
}

// FindKeysByAllTags returns all keys that have all the specified tags in their metadata
func (s *KVStore) FindKeysByAllTags(tags []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []string
	for k, e := range s.data {
		// Skip expired entries
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}

		// Skip entries with no metadata or without all the tags
		if e.metadata != nil && e.metadata.HasAllTags(tags) {
			keys = append(keys, k)
		}
	}
	return keys
}

// FindKeysByAnyTag returns all keys that have any of the specified tags in their metadata
func (s *KVStore) FindKeysByAnyTag(tags []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []string
	for k, e := range s.data {
		// Skip expired entries
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}

		// Skip entries with no metadata or without any of the tags
		if e.metadata != nil && e.metadata.HasAnyTag(tags) {
			keys = append(keys, k)
		}
	}
	return keys
}

// SetProperty sets a property in a key's metadata
func (s *KVStore) SetProperty(key string, propertyKey string, propertyValue interface{}) error {
	meta, err := s.GetMetadata(key)
	if err != nil {
		return err
	}

	meta.SetProperty(propertyKey, propertyValue)
	return nil
}

// GetProperty gets a property from a key's metadata
func (s *KVStore) GetProperty(key string, propertyKey string) (interface{}, error) {
	meta, err := s.GetMetadata(key)
	if err != nil {
		return nil, err
	}

	val, exists := meta.GetProperty(propertyKey)
	if !exists {
		return nil, fmt.Errorf("property '%s' not found", propertyKey)
	}
	return val, nil
}

// FindKeysByProperty returns all keys that have a specific property with a specific value
func (s *KVStore) FindKeysByProperty(propertyKey string, propertyValue interface{}) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var keys []string
	for k, e := range s.data {
		// Skip expired entries
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}

		// Skip entries with no metadata
		if e.metadata == nil {
			continue
		}

		// Check if the property exists and matches the value
		if val, exists := e.metadata.Properties[propertyKey]; exists {
			// Compare values (careful with types)
			if reflect.DeepEqual(val, propertyValue) {
				keys = append(keys, k)
			}
		}
	}
	return keys
}

// Clone creates a new KVStore with a deep copy of all entries from this store.
// The returned store will have the same data but no shared references with the original.
func (s *KVStore) Clone() *KVStore {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create a new store
	newStore := NewKVStore()

	// Copy all entries, handling expired keys
	for key, e := range s.data {
		// Skip expired entries
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}

		// Get the original value
		originalValue := e.value

		// Handle TTL if present
		var ttl time.Duration
		if e.expiresAt != nil {
			ttl = time.Until(*e.expiresAt)
			if ttl <= 0 {
				continue // Skip if expired during processing
			}
		}

		// Handle metadata if present
		var metadata *Metadata
		if e.metadata != nil {
			metadata = &Metadata{
				Tags:        append([]string{}, e.metadata.Tags...),
				Properties:  make(map[string]interface{}),
				Description: e.metadata.Description,
				CreatedAt:   e.metadata.CreatedAt,
				UpdatedAt:   e.metadata.UpdatedAt,
			}
			// Deep copy properties
			for k, v := range e.metadata.Properties {
				metadata.Properties[k] = v
			}
		}

		// Create a proper deep copy based on the type
		var deepCopy interface{}

		// Handle different types for proper deep copying
		if reflect.TypeOf(originalValue).Kind() == reflect.Ptr {
			// For pointers, create a new instance and copy the value
			elemType := reflect.TypeOf(originalValue).Elem()
			newInstance := reflect.New(elemType)

			// Deep copy depends on the underlying element type
			if elemType.Kind() == reflect.Struct {
				// For struct pointers, copy each field
				newInstance.Elem().Set(reflect.ValueOf(originalValue).Elem())
			} else if elemType.Kind() == reflect.Map {
				// For maps, create a new map and copy all entries
				originalMap := reflect.ValueOf(originalValue).Elem().Interface()
				newInstance.Elem().Set(reflect.ValueOf(originalMap))
			} else if elemType.Kind() == reflect.Slice || elemType.Kind() == reflect.Array {
				// For slices/arrays, create a new one and copy all elements
				originalSlice := reflect.ValueOf(originalValue).Elem().Interface()
				newInstance.Elem().Set(reflect.ValueOf(originalSlice))
			} else {
				// For other pointer types, just make a simple copy
				newInstance.Elem().Set(reflect.ValueOf(originalValue).Elem())
			}

			deepCopy = newInstance.Interface()
		} else if reflect.TypeOf(originalValue).Kind() == reflect.Struct {
			// For structs, create a new instance and copy
			newStruct := reflect.New(reflect.TypeOf(originalValue)).Elem()
			newStruct.Set(reflect.ValueOf(originalValue))
			deepCopy = newStruct.Interface()
		} else if reflect.TypeOf(originalValue).Kind() == reflect.Map {
			// For maps, create a new map and copy all entries
			deepCopy = reflect.ValueOf(originalValue).Interface()
		} else if reflect.TypeOf(originalValue).Kind() == reflect.Slice || reflect.TypeOf(originalValue).Kind() == reflect.Array {
			// For slices/arrays, create a new one and copy all elements
			deepCopy = reflect.ValueOf(originalValue).Interface()
		} else {
			// For primitive types, a simple copy is sufficient
			deepCopy = originalValue
		}

		// Store the value in the new store (no serialization needed)
		var expiresAt *time.Time
		if ttl > 0 {
			exp := time.Now().Add(ttl)
			expiresAt = &exp
		}

		// Create the entry directly
		newStore.data[key] = entry{
			typ:       e.typ,
			typeKind:  e.typeKind,
			value:     deepCopy,
			expiresAt: expiresAt,
			metadata:  metadata,
		}
	}

	return newStore
}

// CloneFrom creates a new KVStore with all entries copied from the provided store.
// This is a static helper method that internally calls Clone().
func CloneFrom(source *KVStore) *KVStore {
	if source == nil {
		return NewKVStore()
	}
	return source.Clone()
}

// CopyFrom copies all entries from the source store to the current store.
// It returns the number of entries copied and an error if one occurred.
func (s *KVStore) CopyFrom(source *KVStore) (int, error) {
	if source == nil {
		return 0, fmt.Errorf("source store is nil")
	}

	source.mu.RLock()
	defer source.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	copied := 0
	for key, srcEntry := range source.data {
		// Skip expired entries
		if srcEntry.expiresAt != nil && time.Now().After(*srcEntry.expiresAt) {
			continue
		}

		// Skip keys that already exist in the destination
		if _, exists := s.data[key]; exists {
			continue
		}

		// Use our deepCopy function to ensure proper reference isolation
		deepCopiedValue := deepCopy(srcEntry.value)

		// Deep copy metadata if it exists
		var metadataCopy *Metadata
		if srcEntry.metadata != nil {
			metadataCopy = &Metadata{
				Tags:        append([]string{}, srcEntry.metadata.Tags...),
				Properties:  make(map[string]interface{}),
				Description: srcEntry.metadata.Description,
				CreatedAt:   srcEntry.metadata.CreatedAt,
				UpdatedAt:   srcEntry.metadata.UpdatedAt,
			}
			// Deep copy properties
			for k, v := range srcEntry.metadata.Properties {
				metadataCopy.Properties[k] = deepCopy(v)
			}
		}

		// Create a new entry with the deep-copied value
		s.data[key] = entry{
			typ:       srcEntry.typ,
			typeKind:  srcEntry.typeKind,
			value:     deepCopiedValue,
			expiresAt: srcEntry.expiresAt,
			metadata:  metadataCopy,
		}

		copied++
	}

	return copied, nil
}

// CopyFromWithOverwrite copies all entries from the source store into this store,
// overwriting any existing entries in the destination with the same keys.
// This is a deep copy operation, so no references are shared between the stores.
// Returns the number of entries copied, the number of entries overwritten, and an error if one occurred.
func (s *KVStore) CopyFromWithOverwrite(source *KVStore) (copied int, overwritten int, err error) {
	if source == nil {
		return 0, 0, fmt.Errorf("source store is nil")
	}

	source.mu.RLock()
	defer source.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	for key, srcEntry := range source.data {
		// Skip expired entries
		if srcEntry.expiresAt != nil && time.Now().After(*srcEntry.expiresAt) {
			continue
		}

		// Check if key exists in destination
		_, exists := s.data[key]

		// Use our deepCopy function to ensure proper reference isolation
		deepCopiedValue := deepCopy(srcEntry.value)

		// Deep copy metadata if it exists
		var metadataCopy *Metadata
		if srcEntry.metadata != nil {
			metadataCopy = &Metadata{
				Tags:        append([]string{}, srcEntry.metadata.Tags...),
				Properties:  make(map[string]interface{}),
				Description: srcEntry.metadata.Description,
				CreatedAt:   srcEntry.metadata.CreatedAt,
				UpdatedAt:   srcEntry.metadata.UpdatedAt,
			}
			// Deep copy properties
			for k, v := range srcEntry.metadata.Properties {
				metadataCopy.Properties[k] = deepCopy(v)
			}
		}

		// Create a new entry with the deep-copied value
		s.data[key] = entry{
			typ:       srcEntry.typ,
			typeKind:  srcEntry.typeKind,
			value:     deepCopiedValue,
			expiresAt: srcEntry.expiresAt,
			metadata:  metadataCopy,
		}

		if exists {
			overwritten++
		} else {
			copied++
		}
	}

	return copied, overwritten, nil
}

// deepCopy creates a proper deep copy of a value
func deepCopy(value interface{}) interface{} {
	if value == nil {
		return nil
	}

	valueType := reflect.TypeOf(value)
	valueKind := valueType.Kind()

	// Handle different types for proper deep copying
	if valueKind == reflect.Ptr {
		// For pointers, create a new instance and deep copy the target
		elemType := valueType.Elem()
		newInstance := reflect.New(elemType)

		// Deep copy the pointed-to value
		elemValue := reflect.ValueOf(value).Elem().Interface()
		elemCopy := deepCopy(elemValue)

		// Set the new instance to the copied element
		newInstance.Elem().Set(reflect.ValueOf(elemCopy))
		return newInstance.Interface()
	} else if valueKind == reflect.Struct {
		// For structs, create a new instance and copy each field
		newStruct := reflect.New(valueType).Elem()

		// Copy each field
		for i := 0; i < valueType.NumField(); i++ {
			field := reflect.ValueOf(value).Field(i)
			if field.CanInterface() {
				fieldValue := field.Interface()
				fieldCopy := deepCopy(fieldValue)
				newStruct.Field(i).Set(reflect.ValueOf(fieldCopy))
			} else {
				// For unexported fields, we can't access their interface directly
				// Just copy the value directly if possible
				newStruct.Field(i).Set(field)
			}
		}

		return newStruct.Interface()
	} else if valueKind == reflect.Map {
		// For maps, create a new map and deep copy all entries
		valueValue := reflect.ValueOf(value)
		newMap := reflect.MakeMap(valueType)

		// Iterate over the map entries
		iter := valueValue.MapRange()
		for iter.Next() {
			k := iter.Key()
			v := iter.Value().Interface()
			// Deep copy the value
			vCopy := deepCopy(v)
			newMap.SetMapIndex(k, reflect.ValueOf(vCopy))
		}

		return newMap.Interface()
	} else if valueKind == reflect.Slice {
		// For slices, create a new slice and deep copy all elements
		valueValue := reflect.ValueOf(value)
		newSlice := reflect.MakeSlice(valueType, valueValue.Len(), valueValue.Cap())

		// Copy each element
		for i := 0; i < valueValue.Len(); i++ {
			elem := valueValue.Index(i).Interface()
			elemCopy := deepCopy(elem)
			newSlice.Index(i).Set(reflect.ValueOf(elemCopy))
		}

		return newSlice.Interface()
	} else if valueKind == reflect.Array {
		// For arrays, create a new array and deep copy all elements
		valueValue := reflect.ValueOf(value)
		newArray := reflect.New(valueType).Elem()

		// Copy each element
		for i := 0; i < valueValue.Len(); i++ {
			elem := valueValue.Index(i).Interface()
			elemCopy := deepCopy(elem)
			newArray.Index(i).Set(reflect.ValueOf(elemCopy))
		}

		return newArray.Interface()
	} else {
		// For primitive types, a simple copy is sufficient
		return value
	}
}
