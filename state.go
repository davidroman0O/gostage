package gostage

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

// stateEntry holds a single key-value pair in the run state buffer.
type stateEntry struct {
	value    any
	typeName string // Go type name for round-trip fidelity
	dirty    bool   // true if written during this step (needs flushing)
}

// typedEntry is the wire format for spawn state transfer.
// Each value carries its Go type name so the receiver can restore
// the original type after JSON decoding (which turns all numbers to float64).
type typedEntry struct {
	V json.RawMessage `json:"v"`
	T string          `json:"t"`
}

// StateEntry is the persistence-facing representation of a state key.
type StateEntry struct {
	Value    []byte // JSON-encoded value
	TypeName string // Go type name (e.g. "int", "string")
}

// runState is the per-run state buffer replacing KVStore.
// Writes are buffered in memory during step execution and flushed
// to persistence at step boundaries.
type runState struct {
	mu      sync.RWMutex
	runID   RunID
	data    map[string]stateEntry
	deleted map[string]struct{} // keys pending deletion in persistence
	persist Persistence         // nil = memory-only (children, tests)
	limit   int                 // max user entries, 0 = unlimited
}

// newRunState creates a new run state buffer.
// If persist is nil, Flush is a no-op (suitable for children and tests).
func newRunState(runID RunID, persist Persistence) *runState {
	return &runState{
		runID:   runID,
		data:    make(map[string]stateEntry),
		deleted: make(map[string]struct{}),
		persist: persist,
	}
}

// Set stores a value and marks it dirty (will be flushed to persistence).
// Values are stored by reference — mutable types (slices, maps, structs)
// are not deep-copied. In concurrent ForEach, each item should write to
// distinct keys to avoid data races on shared mutable values.
func (s *runState) Set(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = stateEntry{
		value:    value,
		typeName: goTypeName(value),
		dirty:    true,
	}
}

// setLimit configures the maximum number of entries in the state.
// 0 means unlimited.
func (s *runState) setLimit(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limit = n
}

// setWithLimit stores a value like Set but enforces the entry limit.
// If the limit is reached and the key is new, returns ErrStateLimitExceeded.
// Updating an existing key always succeeds regardless of limit.
func (s *runState) setWithLimit(key string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.limit > 0 {
		if _, exists := s.data[key]; !exists && len(s.data) >= s.limit {
			return ErrStateLimitExceeded
		}
	}
	s.data[key] = stateEntry{
		value:    value,
		typeName: goTypeName(value),
		dirty:    true,
	}
	return nil
}

// SetBatch stores multiple values atomically, marking all as dirty.
// Acquires the lock once for all writes. Used for atomic child state merge.
func (s *runState) SetBatch(entries map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range entries {
		s.data[k] = stateEntry{
			value:    v,
			typeName: goTypeName(v),
			dirty:    true,
		}
	}
}

// SetClean stores a value without marking it dirty.
// Used for loading data from persistence or parent inputs.
func (s *runState) SetClean(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = stateEntry{
		value:    value,
		typeName: goTypeName(value),
		dirty:    false,
	}
}

// Get retrieves a value by key. Returns (value, true) or (nil, false).
func (s *runState) Get(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return nil, false
	}
	return e.value, true
}

// Delete removes a key from the buffer and records it as a tombstone
// so Flush can remove it from persistence on the next flush cycle.
func (s *runState) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	if s.persist != nil {
		s.deleted[key] = struct{}{}
	}
}

// Keys returns all keys in the buffer.
func (s *runState) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// ExportAll returns a snapshot of all key-value pairs.
// Used for building Result.Store.
func (s *runState) ExportAll() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]any, len(s.data))
	for k, e := range s.data {
		out[k] = e.value
	}
	return out
}

// ExportDirty returns only the key-value pairs that were written (dirty).
// Used by child processes to return only their own writes.
func (s *runState) ExportDirty() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]any)
	for k, e := range s.data {
		if e.dirty {
			out[k] = e.value
		}
	}
	return out
}

// SerializeAll returns all entries as JSON-encoded typedEntry values
// (each carrying the Go type name for round-trip fidelity).
// Used by spawn to send the full state snapshot to child processes.
func (s *runState) SerializeAll() (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string][]byte, len(s.data))
	for k, e := range s.data {
		jsonVal, err := json.Marshal(e.value)
		if err != nil {
			return nil, fmt.Errorf("marshal key %q: %w", k, err)
		}
		te := typedEntry{V: jsonVal, T: e.typeName}
		data, err := json.Marshal(te)
		if err != nil {
			return nil, fmt.Errorf("marshal entry %q: %w", k, err)
		}
		result[k] = data
	}
	return result, nil
}

// SerializeDirty returns only dirty entries as JSON-encoded typedEntry values.
// Used by child processes to return only their writes back to the parent.
func (s *runState) SerializeDirty() (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string][]byte)
	for k, e := range s.data {
		if !e.dirty {
			continue
		}
		jsonVal, err := json.Marshal(e.value)
		if err != nil {
			return nil, fmt.Errorf("marshal key %q: %w", k, err)
		}
		te := typedEntry{V: jsonVal, T: e.typeName}
		data, err := json.Marshal(te)
		if err != nil {
			return nil, fmt.Errorf("marshal entry %q: %w", k, err)
		}
		result[k] = data
	}
	return result, nil
}

// Clone creates an independent copy of the run state.
// All entries in the clone are clean (not dirty).
func (s *runState) Clone() *runState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c := &runState{
		runID:   s.runID,
		data:    make(map[string]stateEntry, len(s.data)),
		deleted: make(map[string]struct{}),
		persist: s.persist,
	}
	for k, e := range s.data {
		c.data[k] = stateEntry{
			value:    e.value,
			typeName: e.typeName,
			dirty:    false,
		}
	}
	return c
}

// Flush writes all dirty entries to persistence and clears the dirty flags.
// No-op if persist is nil (children, tests without persistence).
func (s *runState) Flush(ctx context.Context) error {
	if s.persist == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entries := make(map[string]StateEntry)
	for k, e := range s.data {
		if !e.dirty {
			continue
		}
		jsonVal, err := json.Marshal(e.value)
		if err != nil {
			return fmt.Errorf("marshal key %q: %w", k, err)
		}
		entries[k] = StateEntry{
			Value:    jsonVal,
			TypeName: e.typeName,
		}
	}

	if len(entries) > 0 {
		if err := s.persist.SaveState(ctx, s.runID, entries); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
		// Clear dirty flags
		for k, e := range s.data {
			if e.dirty {
				e.dirty = false
				s.data[k] = e
			}
		}
	}

	// Flush tombstones: remove keys that were deleted since last flush
	for key := range s.deleted {
		if err := s.persist.DeleteStateKey(ctx, s.runID, key); err != nil {
			return fmt.Errorf("delete state key %q: %w", key, err)
		}
	}
	s.deleted = make(map[string]struct{})

	return nil
}

// LoadFromPersistence loads all state entries from the persistence layer
// into the buffer. Existing entries are overwritten. All loaded entries
// are clean (not dirty).
func (s *runState) LoadFromPersistence(ctx context.Context) error {
	if s.persist == nil {
		return nil
	}

	entries, err := s.persist.LoadState(ctx, s.runID)
	if err != nil {
		return fmt.Errorf("load state: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for k, e := range entries {
		var val any
		if err := json.Unmarshal(e.Value, &val); err != nil {
			return fmt.Errorf("unmarshal key %q: %w", k, err)
		}
		// Restore type fidelity
		val = convertType(val, e.TypeName)
		s.data[k] = stateEntry{
			value:    val,
			typeName: e.TypeName,
			dirty:    false,
		}
	}
	return nil
}

// goTypeName returns the Go type name for a value.
func goTypeName(v any) string {
	if v == nil {
		return ""
	}
	switch v.(type) {
	case bool:
		return "bool"
	case int:
		return "int"
	case int8:
		return "int8"
	case int16:
		return "int16"
	case int32:
		return "int32"
	case int64:
		return "int64"
	case uint:
		return "uint"
	case uint8:
		return "uint8"
	case uint16:
		return "uint16"
	case uint32:
		return "uint32"
	case uint64:
		return "uint64"
	case float32:
		return "float32"
	case float64:
		return "float64"
	case string:
		return "string"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// convertType restores a JSON-decoded value to its original Go type.
// JSON decodes all numbers as float64; this converts back to the original type.
func convertType(val any, typeName string) any {
	if typeName == "" {
		return val
	}

	f, isFloat := val.(float64)
	if !isFloat {
		return val
	}

	switch typeName {
	case "int":
		return int(f)
	case "int8":
		return int8(f)
	case "int16":
		return int16(f)
	case "int32":
		return int32(f)
	case "int64":
		return int64(f)
	case "uint":
		return uint(f)
	case "uint8":
		return uint8(f)
	case "uint16":
		return uint16(f)
	case "uint32":
		return uint32(f)
	case "uint64":
		return uint64(f)
	case "float32":
		return float32(f)
	case "float64":
		return f
	default:
		return val
	}
}

// serializableCache stores the result of isJSONSerializable per reflect.Type.
var serializableCache sync.Map

// ResetSerializableCache clears the isJSONSerializable results cache.
// Call alongside ResetTaskRegistry() in test setup to ensure clean state.
func ResetSerializableCache() {
	serializableCache.Range(func(k, v any) bool {
		serializableCache.Delete(k)
		return true
	})
}

// isJSONSerializable reports whether a value of the given type can be
// marshaled to JSON without error. Results are cached per type.
func isJSONSerializable(t reflect.Type) bool {
	if t == nil {
		return true
	}
	if cached, ok := serializableCache.Load(t); ok {
		return cached.(bool)
	}
	result := checkSerializable(t, make(map[reflect.Type]bool))
	serializableCache.Store(t, result)
	return result
}

// checkSerializable walks the type tree and returns false if any component
// is not JSON-serializable. The visited map breaks recursive type cycles.
func checkSerializable(t reflect.Type, visited map[reflect.Type]bool) bool {
	if t == nil {
		return true
	}
	if visited[t] {
		return true // cycle — assume OK
	}
	visited[t] = true

	switch t.Kind() {
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return false
	case reflect.Complex64, reflect.Complex128:
		return false
	case reflect.Ptr:
		return checkSerializable(t.Elem(), visited)
	case reflect.Slice, reflect.Array:
		return checkSerializable(t.Elem(), visited)
	case reflect.Map:
		// JSON requires string keys
		if t.Key().Kind() != reflect.String {
			return false
		}
		return checkSerializable(t.Elem(), visited)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			if f.Tag.Get("json") == "-" {
				continue
			}
			if !checkSerializable(f.Type, visited) {
				return false
			}
		}
		return true
	case reflect.Interface:
		return true // can't check statically
	default:
		return true // primitives, strings
	}
}
