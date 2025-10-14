package state

import (
	"context"
	"errors"
	"testing"

	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestSQLiteQueueClaimOrder(t *testing.T) {
	db := openTestDB(t)
	queue, err := NewSQLiteQueue(db)
	if err != nil {
		t.Fatalf("new sqlite queue: %v", err)
	}

	ctx := context.Background()
	def1 := workflow.Definition{ID: "wf-low", Tags: []string{"payments"}}
	def2 := workflow.Definition{ID: "wf-high", Tags: []string{"payments"}}
	if _, err := queue.Enqueue(ctx, def1, PriorityDefault, nil); err != nil {
		t.Fatalf("enqueue low: %v", err)
	}
	if id, err := queue.Enqueue(ctx, def2, PriorityHigh, map[string]any{"k": "v"}); err != nil {
		t.Fatalf("enqueue high: %v", err)
	} else {
		_ = id
	}

	claimed, err := queue.Claim(ctx, Selector{All: []string{"payments"}}, "worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Definition.ID != "wf-high" {
		t.Fatalf("expected high priority first, got %s", claimed.Definition.ID)
	}
	if claimed.Metadata["k"] != "v" {
		t.Fatalf("metadata mismatch: %#v", claimed.Metadata)
	}

	if err := queue.Ack(ctx, claimed.ID, ResultSummary{Success: true}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	claimed2, err := queue.Claim(ctx, Selector{All: []string{"payments"}}, "worker-1")
	if err != nil {
		t.Fatalf("claim second: %v", err)
	}
	if claimed2.Definition.ID != "wf-low" {
		t.Fatalf("expected remaining workflow, got %s", claimed2.Definition.ID)
	}
}

func TestSQLiteQueueSelectorMiss(t *testing.T) {
	db := openTestDB(t)
	queue, err := NewSQLiteQueue(db)
	if err != nil {
		t.Fatalf("new sqlite queue: %v", err)
	}

	ctx := context.Background()
	def := workflow.Definition{ID: "wf"}
	if _, err := queue.Enqueue(ctx, def, PriorityDefault, nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	_, err = queue.Claim(ctx, Selector{All: []string{"missing"}}, "worker")
	if !errors.Is(err, ErrNoPending) {
		t.Fatalf("expected errNoPending, got %v", err)
	}
}
