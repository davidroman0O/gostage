package bootstrap

import "github.com/davidroman0O/gostage/v3/state"

// SubmitRequest captures all submission-time overrides before a workflow is enqueued.
type SubmitRequest struct {
	Priority     state.Priority
	Tags         []string
	Metadata     map[string]any
	InitialStore map[string]any
}

const MetadataInitialStoreKey = "gostage.initial_store"

// NewSubmitRequest constructs a request with default priority.
func NewSubmitRequest() *SubmitRequest {
	return &SubmitRequest{
		Priority: state.PriorityDefault,
		Metadata: make(map[string]any),
	}
}
