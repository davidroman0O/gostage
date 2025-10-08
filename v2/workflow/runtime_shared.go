package workflow

import "time"

// RuntimeToggle captures metadata for runtime enable/disable/remove events.
type RuntimeToggle struct {
    CreatedBy string
    Timestamp time.Time
}

func copyRuntimeToggleMap(src map[string]RuntimeToggle) map[string]RuntimeToggle {
    if src == nil {
        return nil
    }
    out := make(map[string]RuntimeToggle, len(src))
    for k, v := range src {
        out[k] = v
    }
    return out
}

func cloneMap(src map[string]interface{}) map[string]interface{} {
    if src == nil {
        return nil
    }
    dup := make(map[string]interface{}, len(src))
    for k, v := range src {
        dup[k] = v
    }
    return dup
}
