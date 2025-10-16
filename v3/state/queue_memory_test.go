package state

import (
	"context"
	"errors"
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
	if claimed.Attempt != 1 {
		t.Fatalf("expected first attempt to be 1, got %d", claimed.Attempt)
	}
}

func TestMemoryQueueReleaseRequeues(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	def := workflow.Definition{ID: "wf", Tags: []string{"payments"}}
	id, err := q.Enqueue(ctx, def, PriorityDefault, nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := q.Claim(ctx, Selector{All: []string{"payments"}}, "worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != id {
		t.Fatalf("claimed unexpected workflow: %s", claimed.ID)
	}

	stats, err := q.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Pending != 0 || stats.Claimed != 1 {
		t.Fatalf("unexpected stats before release: %+v", stats)
	}

	if err := q.Release(ctx, claimed.ID); err != nil {
		t.Fatalf("release: %v", err)
	}

	next, err := q.Claim(ctx, Selector{All: []string{"payments"}}, "worker-2")
	if err != nil {
		t.Fatalf("claim after release: %v", err)
	}
	if next.Definition.ID != def.ID {
		t.Fatalf("expected same workflow after release, got %s", next.Definition.ID)
	}
	if next.Attempt != claimed.Attempt+1 {
		t.Fatalf("expected attempt increment, got %d", next.Attempt)
	}
}

func TestMemoryQueueSelectorRespected(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()
	def := workflow.Definition{ID: "wf", Tags: []string{"payments"}}
	if _, err := q.Enqueue(ctx, def, PriorityDefault, nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := q.Claim(ctx, Selector{All: []string{"nonmatching"}}, "worker"); !errors.Is(err, ErrNoPending) {
		t.Fatalf("expected ErrNoPending, got %v", err)
	}
}

func TestMemoryQueueAnyNoneSelectors(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	blocked := workflow.Definition{ID: "wf-blocked", Tags: []string{"alpha"}}
	target := workflow.Definition{ID: "wf-target", Tags: []string{"beta"}}
	if _, err := q.Enqueue(ctx, blocked, PriorityHigh, nil); err != nil {
		t.Fatalf("enqueue blocked: %v", err)
	}
	if _, err := q.Enqueue(ctx, target, PriorityDefault, nil); err != nil {
		t.Fatalf("enqueue target: %v", err)
	}

	claimed, err := q.Claim(ctx, Selector{Any: []string{"beta"}, None: []string{"alpha"}}, "worker")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Definition.ID != "wf-target" {
		t.Fatalf("expected target workflow, got %s", claimed.Definition.ID)
	}

	if _, err := q.Claim(ctx, Selector{Any: []string{"gamma"}}, "worker"); !errors.Is(err, ErrNoPending) {
		t.Fatalf("expected ErrNoPending when Any selector has no matches, got %v", err)
	}
}
