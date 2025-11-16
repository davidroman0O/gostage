package gostage_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/davidroman0O/gostage/v3/layers/orchestration/bootstrap"
	"github.com/davidroman0O/gostage/v3/e2e/testkit"
	"github.com/davidroman0O/gostage/v3/layers/foundation/gostagetest"
	"github.com/davidroman0O/gostage/v3/shared/metadata"
	"github.com/davidroman0O/gostage/v3/shared/pools"
	rt "github.com/davidroman0O/gostage/v3/shared/runtime"
	"github.com/davidroman0O/gostage/v3/layers/domain"
	"github.com/davidroman0O/gostage/v3/layers/domain/store"
	"github.com/davidroman0O/gostage/v3/shared/workflow"

	gostage "github.com/davidroman0O/gostage/v3"
)

// deserializeDef is a test helper to deserialize []byte back to workflow.Definition
func deserializeDef(data []byte) workflow.Definition {
	var def workflow.Definition
	_ = json.Unmarshal(data, &def)
	return def
}

func TestSubmitWorkflowID(t *testing.T) {
	ctx := context.Background()
	testkit.ResetRegistry(t)

	// Register action and workflow
	gostage.MustRegisterAction("test.echo", func(_ rt.Context) error {
		return nil
	})

	def := workflow.Definition{
		ID: "test-workflow",
		Stages: []workflow.Stage{
			{
				Name: "stage1",
				Actions: []workflow.Action{
					{Ref: "test.echo"},
				},
			},
		},
	}

	workflowID, _ := gostage.MustRegisterWorkflow(def)

	queue := &captureQueue{}
	memStore := store.NewMemoryStore()
	storeAdapter := domain.NewStoreAdapter(memStore)
	pool := pools.NewLocal("local", domain.Selector{}, 1) // Empty selector accepts all workflows
	parent := gostagetest.NewParentNode()
	parent.SetQueueForTest(queue)
	parent.SetStoreForTest(storeAdapter)
	parent.SetPoolsForTest([]*gostagetest.PoolBinding{{Pool: pool}})

	// Test SubmitWorkflowID
	runID, err := parent.SubmitWorkflowID(ctx, workflowID)
	if err != nil {
		t.Fatalf("SubmitWorkflowID: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty run ID")
	}
}

func TestSubmitDefinition(t *testing.T) {
	ctx := context.Background()
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("test.echo", func(_ rt.Context) error {
		return nil
	})

	def := workflow.Definition{
		ID: "inline-workflow",
		Stages: []workflow.Stage{
			{
				Name: "stage1",
				Actions: []workflow.Action{
					{Ref: "test.echo"},
				},
			},
		},
	}

	queue := &captureQueue{}
	memStore := store.NewMemoryStore()
	storeAdapter := domain.NewStoreAdapter(memStore)
	pool := pools.NewLocal("local", domain.Selector{}, 1)
	parent := gostagetest.NewParentNode()
	parent.SetQueueForTest(queue)
	parent.SetStoreForTest(storeAdapter)
	parent.SetPoolsForTest([]*gostagetest.PoolBinding{{Pool: pool}})

	// Test SubmitDefinition
	runID, err := parent.SubmitDefinition(ctx, def)
	if err != nil {
		t.Fatalf("SubmitDefinition: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty run ID")
	}
}

func TestSubmitWorkflowIDWithOptions(t *testing.T) {
	ctx := context.Background()
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("test.echo", func(_ rt.Context) error {
		return nil
	})

	def := workflow.Definition{
		ID: "test-workflow",
		Stages: []workflow.Stage{
			{
				Name: "stage1",
				Actions: []workflow.Action{
					{Ref: "test.echo"},
				},
			},
		},
	}

	workflowID, _ := gostage.MustRegisterWorkflow(def)

	queue := &captureQueue{}
	memStore := store.NewMemoryStore()
	storeAdapter := domain.NewStoreAdapter(memStore)
	pool := pools.NewLocal("local", domain.Selector{All: []string{"test"}}, 1)
	parent := gostagetest.NewParentNode()
	parent.SetQueueForTest(queue)
	parent.SetStoreForTest(storeAdapter)
	parent.SetPoolsForTest([]*gostagetest.PoolBinding{{Pool: pool}})

	// Test SubmitWorkflowID with options
	runID, err := parent.SubmitWorkflowID(ctx, workflowID, gostage.WithTags("test", "example"))
	if err != nil {
		t.Fatalf("SubmitWorkflowID with options: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty run ID")
	}
}

func TestSubmitDefinitionWithOptions(t *testing.T) {
	ctx := context.Background()
	testkit.ResetRegistry(t)

	gostage.MustRegisterAction("test.echo", func(_ rt.Context) error {
		return nil
	})

	def := workflow.Definition{
		ID: "inline-workflow",
		Stages: []workflow.Stage{
			{
				Name: "stage1",
				Actions: []workflow.Action{
					{Ref: "test.echo"},
				},
			},
		},
	}

	queue := &captureQueue{}
	memStore := store.NewMemoryStore()
	storeAdapter := domain.NewStoreAdapter(memStore)
	pool := pools.NewLocal("local", domain.Selector{}, 1)
	parent := gostagetest.NewParentNode()
	parent.SetQueueForTest(queue)
	parent.SetStoreForTest(storeAdapter)
	parent.SetPoolsForTest([]*gostagetest.PoolBinding{{Pool: pool}})

	// Test SubmitDefinition with options
	runID, err := parent.SubmitDefinition(ctx, def, gostage.WithInitialStore(map[string]any{"key": "value"}))
	if err != nil {
		t.Fatalf("SubmitDefinition with options: %v", err)
	}
	if runID == "" {
		t.Fatal("expected non-empty run ID")
	}
}

func TestSubmitOptionsPopulateRequest(t *testing.T) {
	req := bootstrap.NewSubmitRequest()

	apply := func(opt gostage.SubmitOption) {
		gostagetest.ApplySubmitOption(opt, req)
	}

	apply(gostage.WithPriority(7))
	apply(gostage.WithTags("priority", "payments"))
	initial := map[string]any{"seed": true}
	apply(gostage.WithInitialStore(initial))
	meta := map[string]any{"region": "us"}
	apply(gostage.WithMetadata(meta))

	if req.Priority != domain.Priority(7) {
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
	memStore := store.NewMemoryStore()
	storeAdapter := domain.NewStoreAdapter(memStore)
	pool := pools.NewLocal("local", domain.Selector{All: []string{"order"}}, 1)
	parent := gostagetest.NewParentNode()
	parent.SetQueueForTest(queue)
	parent.SetStoreForTest(storeAdapter)
	parent.SetPoolsForTest([]*gostagetest.PoolBinding{{Pool: pool}})

	def := workflow.Definition{
		ID:   "wf",
		Tags: []string{"order"},
		Metadata: metadata.FromMap(map[string]any{
			"region": "us",
			"tier":   "gold",
		}),
	}

	ref := gostage.WorkflowDefinition(def)
	initialStore := map[string]any{"seed": true}

	id, err := parent.Submit(context.Background(), ref,
		gostage.WithTags("priority"),
		gostage.WithMetadata(map[string]any{"region": "eu", "priority": 5}),
		gostage.WithInitialStore(initialStore),
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

func (q *captureQueue) Enqueue(_ context.Context, defBytes []byte, _ domain.Priority, metadata map[string]any) (domain.WorkflowID, error) {
	q.lastDefinition = deserializeDef(defBytes)
	if metadata != nil {
		copied := make(map[string]any, len(metadata))
		for k, v := range metadata {
			copied[k] = v
		}
		q.lastMetadata = copied
	} else {
		q.lastMetadata = nil
	}
	return domain.WorkflowID("queued"), nil
}

func (q *captureQueue) Claim(context.Context, domain.Selector, string) (*domain.ClaimedWorkflow, error) {
	return nil, domain.ErrNoPending
}
func (q *captureQueue) Release(context.Context, domain.WorkflowID) error                  { return nil }
func (q *captureQueue) Ack(context.Context, domain.WorkflowID, domain.ResultSummary) error { return nil }
func (q *captureQueue) Cancel(context.Context, domain.WorkflowID) error                   { return nil }
func (q *captureQueue) Stats(context.Context) (domain.QueueStats, error) {
	return domain.QueueStats{}, nil
}
func (q *captureQueue) PendingCount(context.Context, domain.Selector) (int, error) { return 0, nil }
func (q *captureQueue) AuditLog(context.Context, int) ([]domain.QueueAuditRecord, error) {
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
