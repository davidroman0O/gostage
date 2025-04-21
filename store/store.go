package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/invopop/jsonschema"
	"github.com/morrisxyang/xreflect"
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

	blob, err := json.Marshal(value)
	if err != nil {
		return err
	}

	t := reflect.TypeOf(value)

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
	s.data[key] = entry{typ: t, blob: blob, expiresAt: expiresAt, metadata: meta}
	s.mu.Unlock()
	return nil
}

// Get retrieves and unmarshals key into a value of type T.
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

	want := reflect.TypeOf((*T)(nil)).Elem()
	if e.typ != want {
		return zero, fmt.Errorf("%w: wanted %v, got %v",
			ErrTypeMismatch, want, e.typ)
	}

	var v T
	if err := json.Unmarshal(e.blob, &v); err != nil {
		return zero, err
	}

	return v, nil
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

	instance := reflect.New(e.typ).Interface()
	if err := json.Unmarshal(e.blob, instance); err != nil {
		return err
	}

	// Use recover to catch panics from type conversion errors
	var setErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				setErr = fmt.Errorf("type conversion error: %v", r)
			}
		}()
		setErr = xreflect.SetEmbedField(instance, fieldPath, fieldValue)
	}()

	if setErr != nil {
		return fmt.Errorf("failed to update field: %w", setErr)
	}

	newBlob, err := json.Marshal(instance)
	if err != nil {
		return err
	}

	s.data[key] = entry{
		typ:       e.typ,
		blob:      newBlob,
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

	instance := reflect.New(e.typ).Interface()
	if err := json.Unmarshal(e.blob, instance); err != nil {
		return err
	}

	for fieldPath, fieldValue := range fields {
		// Use recover to catch panics from type conversion errors
		var setErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					setErr = fmt.Errorf("type conversion error: %v", r)
				}
			}()
			setErr = xreflect.SetEmbedField(instance, fieldPath, fieldValue)
		}()

		if setErr != nil {
			return fmt.Errorf("failed to update field %s: %w", fieldPath, setErr)
		}
	}

	newBlob, err := json.Marshal(instance)
	if err != nil {
		return err
	}

	s.data[key] = entry{
		typ:       e.typ,
		blob:      newBlob,
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
		s.data[key] = entry{typ: e.typ, blob: e.blob, expiresAt: e.expiresAt, metadata: meta}
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

		// Create a new instance of the stored value
		instance := reflect.New(e.typ).Interface()
		if err := json.Unmarshal(e.blob, instance); err != nil {
			// Skip problematic entries
			continue
		}

		// Get the actual value (not the pointer)
		value := reflect.ValueOf(instance).Elem().Interface()

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

		// Add to new store with the same properties
		if metadata != nil {
			if ttl > 0 {
				newStore.PutWithTTLAndMetadata(key, value, ttl, metadata)
			} else {
				newStore.PutWithMetadata(key, value, metadata)
			}
		} else {
			if ttl > 0 {
				newStore.PutWithTTL(key, value, ttl)
			} else {
				newStore.Put(key, value)
			}
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

// CopyFrom copies all entries from the source store into this store.
// This is a deep copy operation, so no references are shared between the stores.
// Existing entries in the destination store are preserved, and only non-existing keys are copied.
// Returns the number of entries copied.
func (s *KVStore) CopyFrom(source *KVStore) (int, error) {
	if source == nil {
		return 0, errors.New("source store cannot be nil")
	}

	// First read all data from source store with only a read lock
	source.mu.RLock()
	entriesToCopy := make(map[string]entry)
	for key, e := range source.data {
		// Skip expired entries
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}
		entriesToCopy[key] = e
	}
	source.mu.RUnlock()

	// Then lock the destination store to modify it
	s.mu.Lock()
	defer s.mu.Unlock()

	copied := 0

	// Copy entries that don't already exist in the destination
	for key, e := range entriesToCopy {
		// Skip keys that already exist in the destination
		if _, exists := s.data[key]; exists {
			continue
		}

		// Create a new instance of the stored value for validation
		// This ensures the blob can be unmarshaled to the specified type
		instance := reflect.New(e.typ).Interface()
		if err := json.Unmarshal(e.blob, instance); err != nil {
			// Skip problematic entries
			continue
		}

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

		// Create the entry directly in the destination's data map
		var expiresAt *time.Time
		if ttl > 0 {
			exp := time.Now().Add(ttl)
			expiresAt = &exp
		}

		// Create the entry directly
		s.data[key] = entry{typ: e.typ, blob: e.blob, expiresAt: expiresAt, metadata: metadata}
		copied++
	}

	return copied, nil
}

// CopyFromWithOverwrite copies all entries from the source store into this store,
// overwriting any existing entries in the destination with the same keys.
// This is a deep copy operation, so no references are shared between the stores.
// Returns the number of entries copied and the number of entries overwritten.
func (s *KVStore) CopyFromWithOverwrite(source *KVStore) (copied int, overwritten int, err error) {
	if source == nil {
		return 0, 0, errors.New("source store cannot be nil")
	}

	// First read all data from source store with only a read lock
	source.mu.RLock()
	entriesToCopy := make(map[string]entry)
	for key, e := range source.data {
		// Skip expired entries
		if e.expiresAt != nil && time.Now().After(*e.expiresAt) {
			continue
		}
		entriesToCopy[key] = e
	}
	source.mu.RUnlock()

	// Then lock the destination store to modify it
	s.mu.Lock()
	defer s.mu.Unlock()

	// Copy all entries
	for key, e := range entriesToCopy {
		// Check if key exists in destination
		_, exists := s.data[key]

		// Create a new instance of the stored value for validation
		// This ensures the blob can be unmarshaled to the specified type
		instance := reflect.New(e.typ).Interface()
		if err := json.Unmarshal(e.blob, instance); err != nil {
			// Skip problematic entries
			continue
		}

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

		// Create the entry directly in the destination's data map
		var expiresAt *time.Time
		if ttl > 0 {
			exp := time.Now().Add(ttl)
			expiresAt = &exp
		}

		// Create the entry directly
		s.data[key] = entry{typ: e.typ, blob: e.blob, expiresAt: expiresAt, metadata: metadata}

		if exists {
			overwritten++
		} else {
			copied++
		}
	}

	return copied, overwritten, nil
}
