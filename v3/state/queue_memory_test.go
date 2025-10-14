package state

import (
	"context"
	"testing"

	"github.com/davidroman0O/gostage/v3/workflow"
)

func TestMemoryQueueBasic(t *testing.T) {
	q := NewMemoryQueue()

	ctx := context.Background()
	wf1 := workflow.Definition{ID: "wf1", Tags: []string{"payments"}}
	wf2 := workflow.Definition{ID: "wf2", Tags: []string{"payments"}}
	if _, err := q.Enqueue(ctx, wf1, PriorityDefault, nil); err != nil {
		t.Fatalf("enqueue wf1: %v", err)
	}
	if _, err := q.Enqueue(ctx, wf2, PriorityHigh, nil); err != nil {
		t.Fatalf("enqueue wf2: %v", err)
	}

	claimed, err := q.Claim(ctx, Selector{All: []string{"payments"}}, "worker")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Definition.ID != "wf2" {
		t.Fatalf("expected wf2 first, got %s", claimed.Definition.ID)
	}
}
