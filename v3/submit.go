package gostage

import "github.com/davidroman0O/gostage/v3/state"

// SubmitOption customises workflow submission behaviour.
type SubmitOption interface {
	applySubmit(*submitConfig)
}

type submitOptionFunc func(*submitConfig)

func (fn submitOptionFunc) applySubmit(cfg *submitConfig) { fn(cfg) }

type submitConfig struct {
	priority     state.Priority
	tags         []string
	initialStore map[string]any
	metadata     map[string]any
}

func newSubmitConfig() submitConfig {
	return submitConfig{
		priority: state.PriorityDefault,
		metadata: make(map[string]any),
	}
}

// WithPriority sets the queue priority for the workflow.
func WithPriority(priority int) SubmitOption {
	return submitOptionFunc(func(cfg *submitConfig) {
		cfg.priority = state.Priority(priority)
	})
}

// WithTags overrides or extends the workflow tags used for pool selection.
func WithTags(tags ...string) SubmitOption {
	return submitOptionFunc(func(cfg *submitConfig) {
		cfg.tags = appendUniqueStrings(cfg.tags, tags...)
	})
}

// WithInitialStore seeds the workflow store before execution.
func WithInitialStore(values map[string]any) SubmitOption {
	return submitOptionFunc(func(cfg *submitConfig) {
		cfg.initialStore = copyMap(values)
	})
}

// WithMetadata attaches custom metadata to the queued workflow.
func WithMetadata(values map[string]any) SubmitOption {
	return submitOptionFunc(func(cfg *submitConfig) {
		if cfg.metadata == nil {
			cfg.metadata = make(map[string]any, len(values))
		}
		for k, v := range values {
			cfg.metadata[k] = v
		}
	})
}

func appendUniqueStrings(dst []string, values ...string) []string {
	m := make(map[string]struct{}, len(dst))
	for _, v := range dst {
		m[v] = struct{}{}
	}
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, ok := m[v]; ok {
			continue
		}
		dst = append(dst, v)
		m[v] = struct{}{}
	}
	return dst
}
