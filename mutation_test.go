package gostage

import (
	"context"
	"sync"
	"testing"
)

// === Dynamic Mutations ===

func TestMutationInsertAfter(t *testing.T) {
	ResetTaskRegistry()

	var order []string
	var mu sync.Mutex

	Task("mut.first", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "first")
		mu.Unlock()
		InsertAfter(ctx, "mut.dynamic")
		return nil
	})
	Task("mut.dynamic", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "dynamic")
		mu.Unlock()
		return nil
	})
	Task("mut.last", func(ctx *Ctx) error {
		mu.Lock()
		order = append(order, "last")
		mu.Unlock()
		return nil
	})

	wf, err := NewWorkflow("insert-after").
		Step("mut.first").
		Step("mut.last").
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
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	if len(order) != 3 {
		t.Fatalf("expected 3 steps, got %v", order)
	}
	if order[0] != "first" || order[1] != "dynamic" || order[2] != "last" {
		t.Fatalf("expected [first, dynamic, last], got %v", order)
	}
}

func TestMutationDisableStep(t *testing.T) {
	ResetTaskRegistry()

	Task("mut.enable", func(ctx *Ctx) error {
		Set(ctx, "enabled", true)
		DisableStep(ctx, "mut.skip")
		return nil
	})
	Task("mut.skip", func(ctx *Ctx) error {
		Set(ctx, "skipped_ran", true)
		return nil
	})
	Task("mut.end", func(ctx *Ctx) error {
		Set(ctx, "end_ran", true)
		return nil
	})

	wf, err := NewWorkflow("disable-step").
		Step("mut.enable").
		Step("mut.skip").
		Step("mut.end").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}
	if result.Store["skipped_ran"] == true {
		t.Fatal("disabled step should not have run")
	}
	if result.Store["end_ran"] != true {
		t.Fatal("end step should have run")
	}
}

func TestMutationDisableByTag(t *testing.T) {
	ResetTaskRegistry()

	Task("mut.controller", func(ctx *Ctx) error {
		DisableByTag(ctx, "optional")
		return nil
	})
	Task("mut.opt1", func(ctx *Ctx) error {
		Set(ctx, "opt1", true)
		return nil
	})
	Task("mut.opt2", func(ctx *Ctx) error {
		Set(ctx, "opt2", true)
		return nil
	})
	Task("mut.required", func(ctx *Ctx) error {
		Set(ctx, "required", true)
		return nil
	})

	wf, err := NewWorkflow("disable-by-tag").
		Step("mut.controller").
		Step("mut.opt1", WithStepTags("optional")).
		Step("mut.opt2", WithStepTags("optional")).
		Step("mut.required").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), wf, nil)
	if result.Store["opt1"] == true || result.Store["opt2"] == true {
		t.Fatal("optional steps should have been disabled")
	}
	if result.Store["required"] != true {
		t.Fatal("required step should have run")
	}
}

func TestMutationInsertSurvivesResume(t *testing.T) {
	ResetTaskRegistry()

	Task("mut.inserter", func(ctx *Ctx) error {
		InsertAfter(ctx, "mut.injected")
		Set(ctx, "inserter_ran", true)
		return nil
	})
	Task("mut.injected", func(ctx *Ctx) error {
		Set(ctx, "injected_ran", true)
		return nil
	})
	Task("mut.suspender", func(ctx *Ctx) error {
		if !IsResuming(ctx) {
			return Suspend(ctx, Params{"reason": "wait"})
		}
		Set(ctx, "resumed", true)
		return nil
	})
	Task("mut.final", func(ctx *Ctx) error {
		Set(ctx, "final_ran", true)
		return nil
	})

	wf, err := NewWorkflow("insert-survives-resume").
		Step("mut.inserter").
		Step("mut.suspender").
		Step("mut.final").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithSQLite(t.TempDir() + "/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// First run: inserter inserts "injected", then suspender suspends
	result, err := engine.RunSync(ctx, wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s (error: %v)", result.Status, result.Error)
	}

	// Resume: injected step should still be present and run
	result, err = engine.Resume(ctx, wf, result.RunID, Params{"go": true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
	if result.Store["injected_ran"] != true {
		t.Fatal("dynamically inserted step should have run after resume")
	}
	if result.Store["resumed"] != true {
		t.Fatal("suspender should have taken resume path")
	}
	if result.Store["final_ran"] != true {
		t.Fatal("final step should have run")
	}
}

func TestMutationDisableSurvivesResume(t *testing.T) {
	ResetTaskRegistry()

	Task("mut.disabler", func(ctx *Ctx) error {
		DisableStep(ctx, "mut.skipped")
		Set(ctx, "disabler_ran", true)
		return nil
	})
	Task("mut.suspender2", func(ctx *Ctx) error {
		if !IsResuming(ctx) {
			return Suspend(ctx, Params{"reason": "wait"})
		}
		Set(ctx, "resumed2", true)
		return nil
	})
	Task("mut.skipped", func(ctx *Ctx) error {
		Set(ctx, "skipped_ran", true)
		return nil
	})
	Task("mut.end2", func(ctx *Ctx) error {
		Set(ctx, "end2_ran", true)
		return nil
	})

	wf, err := NewWorkflow("disable-survives-resume").
		Step("mut.disabler").
		Step("mut.suspender2").
		Step("mut.skipped").
		Step("mut.end2").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New(WithSQLite(t.TempDir() + "/test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// First run: disabler disables "skipped", then suspender suspends
	result, err := engine.RunSync(ctx, wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Suspended {
		t.Fatalf("expected Suspended, got %s (error: %v)", result.Status, result.Error)
	}

	// Resume: "skipped" should still be disabled
	result, err = engine.Resume(ctx, wf, result.RunID, Params{"go": true})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (error: %v)", result.Status, result.Error)
	}
	if result.Store["skipped_ran"] == true {
		t.Fatal("disabled step should NOT have run after resume")
	}
	if result.Store["resumed2"] != true {
		t.Fatal("suspender should have taken resume path")
	}
	if result.Store["end2_ran"] != true {
		t.Fatal("end step should have run")
	}
}
