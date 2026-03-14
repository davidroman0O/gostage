package gostage

import (
	"encoding/json"
	"fmt"
)

// SerializeStateForChild exports the workflow state plus ForEach item/index
// as a map of JSON-encoded typedEntry values for gRPC transfer.
// Each entry carries the Go type name so the child can restore original types.
func SerializeStateForChild(state *RunStateAccessor, item any, index int) (map[string][]byte, error) {
	result, err := state.SerializeAll()
	if err != nil {
		return nil, err
	}

	// Add ForEach item/index with type metadata
	for k, v := range map[string]any{"__foreach_item": item, "__foreach_index": index} {
		jsonVal, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal key %q: %w", k, err)
		}
		te := typedEntry{V: jsonVal, T: goTypeName(v)}
		data, err := json.Marshal(te)
		if err != nil {
			return nil, fmt.Errorf("marshal entry %q: %w", k, err)
		}
		result[k] = data
	}

	return result, nil
}

// serializeStateForChild is the unexported variant for internal tests.
// It wraps a *runState into a RunStateAccessor and delegates.
func serializeStateForChild(s *runState, item any, index int) (map[string][]byte, error) {
	return SerializeStateForChild(&RunStateAccessor{state: s}, item, index)
}

// deserializeStoreData is the unexported variant for internal tests.
func deserializeStoreData(data map[string][]byte) (map[string]any, error) {
	return DeserializeStoreData(data)
}

// DeserializeStoreData converts a map of JSON-encoded typedEntry values back
// to Go values with type restoration. Each entry is expected to be a
// typedEntry{V, T} where T is the original Go type name.
func DeserializeStoreData(data map[string][]byte) (map[string]any, error) {
	result := make(map[string]any, len(data))
	for k, raw := range data {
		var te typedEntry
		if err := json.Unmarshal(raw, &te); err == nil && len(te.V) > 0 {
			// Typed format: decode value and restore original type
			var val any
			if err := json.Unmarshal(te.V, &val); err != nil {
				return nil, fmt.Errorf("unmarshal value for key %q: %w", k, err)
			}
			result[k] = convertType(val, te.T)
			continue
		}
		// Fallback: untyped format (backwards compatibility)
		var val any
		if err := json.Unmarshal(raw, &val); err != nil {
			return nil, fmt.Errorf("unmarshal key %q: %w", k, err)
		}
		result[k] = val
	}
	return result, nil
}
