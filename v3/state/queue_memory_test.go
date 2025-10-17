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

func TestMemoryQueueAuditLog(t *testing.T) {
	q := NewMemoryQueue()
	ctx := context.Background()

	def := workflow.Definition{ID: "wf", Tags: []string{"payments"}}
	id, err := q.Enqueue(ctx, def, PriorityDefault, nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	run1, err := q.Claim(ctx, Selector{All: []string{"payments"}}, "worker-1")
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := q.Release(ctx, run1.ID); err != nil {
		t.Fatalf("release: %v", err)
	}

	run2, err := q.Claim(ctx, Selector{All: []string{"payments"}}, "worker-2")
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if run2.ID != id {
		t.Fatalf("expected same workflow ID after release, got %s", run2.ID)
	}

	if err := q.Ack(ctx, run2.ID, ResultSummary{Attempt: run2.Attempt, Success: true}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	records, err := q.AuditLog(ctx, 10)
	if err != nil {
		t.Fatalf("audit log: %v", err)
	}
	if len(records) < 4 {
		t.Fatalf("expected at least 4 audit records, got %d", len(records))
	}

	events := []string{records[0].Event, records[1].Event, records[2].Event, records[3].Event}
	expected := []string{"ack", "claim", "release", "claim"}
	for i, want := range expected {
		if events[i] != want {
			t.Fatalf("expected event %q at position %d, got %q", want, i, events[i])
		}
	}
	if records[0].Attempt != run2.Attempt {
		t.Fatalf("ack attempt mismatch: want %d, got %d", run2.Attempt, records[0].Attempt)
	}
	if records[3].WorkerID != "worker-1" {
		t.Fatalf("expected first claim worker worker-1, got %s", records[3].WorkerID)
	}
	rawSelector, exists := records[3].Metadata["selector"]
	if !exists {
		t.Fatalf("expected selector metadata, got %#v", records[3].Metadata)
	}

	var all []string
	switch sel := rawSelector.(type) {
	case map[string][]string:
		all = sel["all"]
	case map[string]any:
		if vals, ok := sel["all"].([]any); ok {
			for _, v := range vals {
				if s, ok := v.(string); ok {
					all = append(all, s)
				}
			}
		}
	default:
		t.Fatalf("expected selector metadata, got %#v", records[3].Metadata)
	}
	if len(all) != 1 || all[0] != "payments" {
		t.Fatalf("unexpected selector metadata: %#v", rawSelector)
	}
}
