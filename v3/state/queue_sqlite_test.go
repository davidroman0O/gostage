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

func TestSQLiteQueueSelectorAnyNone(t *testing.T) {
	db := openTestDB(t)
	queue, err := NewSQLiteQueue(db)
	if err != nil {
		t.Fatalf("new sqlite queue: %v", err)
	}

	ctx := context.Background()
	blocked := workflow.Definition{ID: "wf-blocked", Tags: []string{"alpha"}}
	target := workflow.Definition{ID: "wf-target", Tags: []string{"beta"}}

	if _, err := queue.Enqueue(ctx, blocked, PriorityHigh, nil); err != nil {
		t.Fatalf("enqueue blocked: %v", err)
	}
	if _, err := queue.Enqueue(ctx, target, PriorityDefault, nil); err != nil {
		t.Fatalf("enqueue target: %v", err)
	}

	claimed, err := queue.Claim(ctx, Selector{Any: []string{"beta"}, None: []string{"alpha"}}, "worker")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Definition.ID != "wf-target" {
		t.Fatalf("expected selector to skip blocked workflow, got %s", claimed.Definition.ID)
	}

	if _, err := queue.Claim(ctx, Selector{Any: []string{"gamma"}}, "worker"); !errors.Is(err, ErrNoPending) {
		t.Fatalf("expected ErrNoPending for unmatched Any selector, got %v", err)
	}
}

func TestSQLiteQueueAuditLog(t *testing.T) {
	db := openTestDB(t)
	queue, err := NewSQLiteQueue(db)
	if err != nil {
		t.Fatalf("new sqlite queue: %v", err)
	}

	ctx := context.Background()
	def := workflow.Definition{ID: "wf", Tags: []string{"payments"}}
	id, err := queue.Enqueue(ctx, def, PriorityDefault, nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	run1, err := queue.Claim(ctx, Selector{All: []string{"payments"}}, "worker-1")
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if run1.ID != id {
		t.Fatalf("unexpected workflow claimed: %s", run1.ID)
	}
	if err := queue.Release(ctx, run1.ID); err != nil {
		t.Fatalf("release: %v", err)
	}

	run2, err := queue.Claim(ctx, Selector{All: []string{"payments"}}, "worker-2")
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if err := queue.Ack(ctx, run2.ID, ResultSummary{Attempt: run2.Attempt, Success: true}); err != nil {
		t.Fatalf("ack: %v", err)
	}

	records, err := queue.AuditLog(ctx, 10)
	if err != nil {
		t.Fatalf("audit log: %v", err)
	}
	if len(records) < 4 {
		t.Fatalf("expected at least 4 audit entries, got %d", len(records))
	}

	events := []string{records[0].Event, records[1].Event, records[2].Event, records[3].Event}
	expected := []string{"ack", "claim", "release", "claim"}
	for i, want := range expected {
		if events[i] != want {
			t.Fatalf("expected event %q at index %d, got %q", want, i, events[i])
		}
	}
	if records[3].WorkerID != "worker-1" {
		t.Fatalf("expected first claim worker worker-1, got %s", records[3].WorkerID)
	}
	if records[0].Attempt != run2.Attempt {
		t.Fatalf("expected ack attempt %d, got %d", run2.Attempt, records[0].Attempt)
	}
	if records[0].Metadata == nil {
		t.Fatalf("expected metadata on ack record")
	}
	if reason, ok := records[0].Metadata["reason"].(string); !ok || reason != string(TerminationReasonUnknown) {
		t.Fatalf("expected ack reason 'unknown', got %+v", records[0].Metadata)
	}
	if success, ok := records[0].Metadata["success"].(bool); !ok || !success {
		t.Fatalf("expected ack success metadata true, got %+v", records[0].Metadata)
	}
	if pool, ok := records[0].Metadata["pool"].(string); !ok || pool != "worker-2" {
		t.Fatalf("expected ack pool worker-2, got %+v", records[0].Metadata)
	}
	if records[2].Metadata == nil {
		t.Fatalf("expected release metadata")
	}
	if relReason, ok := records[2].Metadata["reason"].(string); !ok || relReason != "retry" {
		t.Fatalf("expected release reason retry, got %+v", records[2].Metadata)
	}
	if pool, ok := records[2].Metadata["pool"].(string); !ok || pool != "worker-1" {
		t.Fatalf("expected release pool worker-1, got %+v", records[2].Metadata)
	}
}
