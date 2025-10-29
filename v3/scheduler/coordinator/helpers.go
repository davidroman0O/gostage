package coordinator

import (
	"encoding/json"
	"fmt"
)

func copyStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyStringStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func copyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func PoolMetadataToStrings(values map[string]any) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for k, v := range values {
		if v == nil {
			continue
		}
		switch val := v.(type) {
		case string:
			out[k] = val
		case fmt.Stringer:
			out[k] = val.String()
		case json.RawMessage:
			out[k] = string(val)
		default:
			encoded, err := json.Marshal(v)
			if err != nil {
				panic(fmt.Sprintf("gostage: pool metadata key %q cannot be encoded: %v", k, err))
			}
			out[k] = string(encoded)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
