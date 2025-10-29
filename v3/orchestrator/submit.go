package orchestrator

import (
	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/state"
)

// SubmitOption customises workflow submission behaviour.
type SubmitOption interface {
	applySubmit(*bootstrap.SubmitRequest)
}

type submitOptionFunc func(*bootstrap.SubmitRequest)

func (fn submitOptionFunc) applySubmit(cfg *bootstrap.SubmitRequest) { fn(cfg) }

// WithPriority sets the queue priority for the workflow.
func WithPriority(priority int) SubmitOption {
	return submitOptionFunc(func(cfg *bootstrap.SubmitRequest) {
		cfg.Priority = state.Priority(priority)
	})
}

// WithTags overrides or extends the workflow tags used for pool selection.
func WithTags(tags ...string) SubmitOption {
	return submitOptionFunc(func(cfg *bootstrap.SubmitRequest) {
		cfg.Tags = appendUniqueStrings(cfg.Tags, tags...)
	})
}

// WithInitialStore seeds the workflow store before execution.
func WithInitialStore(values map[string]any) SubmitOption {
	return submitOptionFunc(func(cfg *bootstrap.SubmitRequest) {
		cfg.InitialStore = copyMap(values)
	})
}

// WithMetadata attaches custom metadata to the queued workflow.
func WithMetadata(values map[string]any) SubmitOption {
	return submitOptionFunc(func(cfg *bootstrap.SubmitRequest) {
		if cfg.Metadata == nil {
			cfg.Metadata = make(map[string]any, len(values))
		}
		for k, v := range values {
			cfg.Metadata[k] = v
		}
	})
}
