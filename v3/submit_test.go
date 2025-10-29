package gostage

import (
	"context"
	"testing"

	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/pools"
	"github.com/davidroman0O/gostage/v3/state"
	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestSubmitOptionsPopulateRequest(t *testing.T) {
	req := bootstrap.NewSubmitRequest()

	apply := func(opt SubmitOption) {
		applySubmitOptionForTest(opt, req)
	}

	apply(WithPriority(7))
	apply(WithTags("priority", "payments"))
	initial := map[string]any{"seed": true}
	apply(WithInitialStore(initial))
	meta := map[string]any{"region": "us"}
	apply(WithMetadata(meta))

	if req.Priority != state.Priority(7) {
		t.Fatalf("expected priority 7, got %d", req.Priority)
	}
	if len(req.Tags) != 2 || req.Tags[0] != "priority" || req.Tags[1] != "payments" {
		t.Fatalf("unexpected tags: %+v", req.Tags)
	}
	if req.InitialStore["seed"] != true {
		t.Fatalf("initial store not copied")
	}
	if req.Metadata["region"] != "us" {
		t.Fatalf("metadata missing override: %+v", req.Metadata)
	}
	meta["region"] = "eu"
	if req.Metadata["region"] != "us" {
		t.Fatalf("metadata map should be defensive copy")
	}
}

func TestParentSubmitMetadataPrecedence(t *testing.T) {
	queue := &captureQueue{}
	store := state.NewMemoryStore()
	pool := pools.NewLocal("local", state.Selector{All: []string{"order"}}, 1)
	parent := newParentNodeForTest()
	parent.SetQueueForTest(queue)
	parent.SetStoreForTest(store)
	parent.SetPoolsForTest([]*poolBinding{{Pool: pool}})

	def := workflow.Definition{
		ID:   "wf",
		Tags: []string{"order"},
		Metadata: map[string]any{
			"region": "us",
			"tier":   "gold",
		},
	}

	ref := WorkflowDefinition(def)
	initialStore := map[string]any{"seed": true}

	id, err := parent.Submit(context.Background(), ref,
		WithTags("priority"),
		WithMetadata(map[string]any{"region": "eu", "priority": 5}),
		WithInitialStore(initialStore),
	)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if id == "" {
		t.Fatalf("expected workflow id")
	}
	if queue.lastDefinition.ID != "wf" {
		t.Fatalf("definition not enqueued")
	}
	if !contains(queue.lastDefinition.Tags, "order") || !contains(queue.lastDefinition.Tags, "priority") {
		t.Fatalf("tags not propagated: %+v", queue.lastDefinition.Tags)
	}
	initialStore["seed"] = false
	if queue.lastMetadata["region"] != "eu" {
		t.Fatalf("queue metadata did not override definition metadata: %+v", queue.lastMetadata)
	}
	if queue.lastMetadata["tier"] != "gold" {
		t.Fatalf("definition metadata missing: %+v", queue.lastMetadata)
	}
	if queue.lastMetadata["priority"] != 5 {
		t.Fatalf("submit metadata missing: %+v", queue.lastMetadata)
	}
	if storeMap, ok := queue.lastMetadata[bootstrap.MetadataInitialStoreKey].(map[string]any); !ok || storeMap["seed"] != true {
		t.Fatalf("initial store missing from metadata: %+v", queue.lastMetadata[bootstrap.MetadataInitialStoreKey])
	}
	if storeMap, _ := queue.lastMetadata[bootstrap.MetadataInitialStoreKey].(map[string]any); storeMap["seed"] != true {
		t.Fatalf("initial store should not be affected by caller mutation")
	}
}

type captureQueue struct {
	lastDefinition workflow.Definition
	lastMetadata   map[string]any
}

func (q *captureQueue) Enqueue(_ context.Context, def workflow.Definition, _ state.Priority, metadata map[string]any) (state.WorkflowID, error) {
	q.lastDefinition = def
	if metadata != nil {
		copied := make(map[string]any, len(metadata))
		for k, v := range metadata {
			copied[k] = v
		}
		q.lastMetadata = copied
	}
	return state.WorkflowID("queued"), nil
}

func (q *captureQueue) Claim(context.Context, state.Selector, string) (*state.ClaimedWorkflow, error) {
	return nil, state.ErrNoPending
}
func (q *captureQueue) Release(context.Context, state.WorkflowID) error                  { return nil }
func (q *captureQueue) Ack(context.Context, state.WorkflowID, state.ResultSummary) error { return nil }
func (q *captureQueue) Cancel(context.Context, state.WorkflowID) error                   { return nil }
func (q *captureQueue) Stats(context.Context) (state.QueueStats, error) {
	return state.QueueStats{}, nil
}
func (q *captureQueue) PendingCount(context.Context, state.Selector) (int, error) { return 0, nil }
func (q *captureQueue) AuditLog(context.Context, int) ([]state.QueueAuditRecord, error) {
	return nil, nil
}
func (q *captureQueue) Close() error { return nil }

func contains(slice []string, value string) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}
