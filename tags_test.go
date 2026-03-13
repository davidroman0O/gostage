package gostage

import (
	"context"
	"testing"
)

// === Tag System ===

func TestTaskTags(t *testing.T) {
	ResetTaskRegistry()

	Task("tag.email", func(ctx *Ctx) error { return nil }, WithTags("notification", "async"))
	Task("tag.sms", func(ctx *Ctx) error { return nil }, WithTags("notification"))
	Task("tag.charge", func(ctx *Ctx) error { return nil }, WithTags("billing"))

	notifTasks := ListTasksByTag("notification")
	if len(notifTasks) != 2 {
		t.Fatalf("expected 2 notification tasks, got %d", len(notifTasks))
	}

	billingTasks := ListTasksByTag("billing")
	if len(billingTasks) != 1 {
		t.Fatalf("expected 1 billing task, got %d", len(billingTasks))
	}
}

func TestStepTags(t *testing.T) {
	ResetTaskRegistry()

	Task("tag.validate", func(ctx *Ctx) error { return nil })
	Task("tag.charge", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("tagged-steps").
		Step("tag.validate", WithStepTags("validation")).
		Step("tag.charge", WithStepTags("billing", "critical")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if len(wf.steps[0].tags) != 1 || wf.steps[0].tags[0] != "validation" {
		t.Fatal("step 0 should have 'validation' tag")
	}
	if len(wf.steps[1].tags) != 2 {
		t.Fatal("step 1 should have 2 tags")
	}
}

func TestWorkflowTags(t *testing.T) {
	ResetTaskRegistry()

	Task("tag.noop", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("tagged-workflow", WithWorkflowTags("billing", "critical")).
		Step("tag.noop").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if len(wf.Tags) != 2 {
		t.Fatalf("expected 2 workflow tags, got %d", len(wf.Tags))
	}
}

func TestFindStepsByTag(t *testing.T) {
	ResetTaskRegistry()

	Task("tag.a", func(ctx *Ctx) error {
		ids := FindStepsByTag(ctx, "optional")
		Set(ctx, "optional_count", len(ids))
		return nil
	})
	Task("tag.b", func(ctx *Ctx) error { return nil })
	Task("tag.c", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("find-by-tag").
		Step("tag.a").
		Step("tag.b", WithStepTags("optional")).
		Step("tag.c", WithStepTags("optional")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		t.Fatal(err)
	}

	count, ok := result.Store["optional_count"]
	if !ok {
		t.Fatal("optional_count not in store")
	}
	// JSON round trip: int -> float64
	if count != 2 && count != float64(2) {
		t.Fatalf("expected 2 optional steps, got %v", count)
	}
}
