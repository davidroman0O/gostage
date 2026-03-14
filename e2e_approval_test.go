package gostage

import (
	"context"
	"testing"
)

func TestE2E_ApprovalChain(t *testing.T) {
	ResetTaskRegistry()

	Condition("approval.is-urgent", func(ctx *Ctx) bool {
		return Get[string](ctx, "priority") == "urgent"
	})

	Task("approval.triage", func(ctx *Ctx) error {
		Set(ctx, "triaged", true)
		return nil
	})
	Task("approval.urgent_review", func(ctx *Ctx) error {
		Set(ctx, "review_type", "urgent")
		InsertAfter(ctx, "approval.audit")
		return nil
	})
	Task("approval.normal_review", func(ctx *Ctx) error {
		Set(ctx, "review_type", "normal")
		return nil
	})
	Task("approval.audit", func(ctx *Ctx) error {
		Set(ctx, "audited", true)
		return nil
	})
	Task("approval.finalize", func(ctx *Ctx) error {
		Set(ctx, "finalized", true)
		return nil
	})

	wf, err := NewWorkflow("approval-chain").
		Step("approval.triage").
		Branch(
			WhenNamed("approval.is-urgent").Step("approval.urgent_review"),
			Default().Step("approval.normal_review"),
		).
		Step("approval.finalize").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Urgent path
	result, err := engine.RunSync(context.Background(), wf, Params{"priority": "urgent"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	reviewType, _ := ResultGet[string](result, "review_type")
	audited, _ := ResultGet[bool](result, "audited")
	finalized, _ := ResultGet[bool](result, "finalized")
	if reviewType != "urgent" {
		t.Fatalf("expected urgent review, got %s", reviewType)
	}
	if !audited {
		t.Fatal("expected audit step to execute (inserted dynamically)")
	}
	if !finalized {
		t.Fatal("expected finalize step to execute")
	}

	// Normal path
	result2, err := engine.RunSync(context.Background(), wf, Params{"priority": "normal"})
	if err != nil {
		t.Fatal(err)
	}
	if result2.Status != Completed {
		t.Fatalf("normal: expected Completed, got %s", result2.Status)
	}
	reviewType2, _ := ResultGet[string](result2, "review_type")
	_, auditedOk := ResultGet[bool](result2, "audited")
	if reviewType2 != "normal" {
		t.Fatalf("expected normal review, got %s", reviewType2)
	}
	if auditedOk {
		t.Fatal("normal path should not have audit step")
	}
}
