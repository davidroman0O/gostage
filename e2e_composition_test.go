package gostage

import (
	"context"
	"fmt"
	"testing"
)

func TestE2E_DeepComposition(t *testing.T) {
	ResetTaskRegistry()

	Condition("dc.is-even", func(ctx *Ctx) bool {
		idx := ItemIndex(ctx)
		return idx%2 == 0
	})

	Task("dc.process_even", func(ctx *Ctx) error {
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("even_%d", idx), true)
		return nil
	})
	Task("dc.process_odd", func(ctx *Ctx) error {
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("odd_%d", idx), true)
		return nil
	})
	Task("dc.enrich", func(ctx *Ctx) error {
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("enriched_%d", idx), true)
		return nil
	})
	Task("dc.validate", func(ctx *Ctx) error {
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("validated_%d", idx), true)
		return nil
	})
	Task("dc.summarize", func(ctx *Ctx) error {
		Set(ctx, "summarized", true)
		return nil
	})

	// Sub-workflow: Branch (even/odd) → Parallel (enrich + validate)
	subWf, err := NewWorkflow("dc-sub").
		Branch(
			WhenNamed("dc.is-even").Step("dc.process_even"),
			Default().Step("dc.process_odd"),
		).
		Parallel(
			Step("dc.enrich"),
			Step("dc.validate"),
		).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Main: ForEach → Sub → Summarize
	wf, err := NewWorkflow("dc-main").
		ForEach("items", Sub(subWf), WithConcurrency(2)).
		Step("dc.summarize").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, Params{
		"items": []int{0, 1, 2, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	even0, _ := ResultGet[bool](result, "even_0")
	odd1, _ := ResultGet[bool](result, "odd_1")
	even2, _ := ResultGet[bool](result, "even_2")
	odd3, _ := ResultGet[bool](result, "odd_3")
	if !even0 || !odd1 || !even2 || !odd3 {
		t.Fatalf("branch results: even_0=%v odd_1=%v even_2=%v odd_3=%v", even0, odd1, even2, odd3)
	}

	for i := 0; i < 4; i++ {
		enriched, _ := ResultGet[bool](result, fmt.Sprintf("enriched_%d", i))
		validated, _ := ResultGet[bool](result, fmt.Sprintf("validated_%d", i))
		if !enriched {
			t.Fatalf("item %d not enriched", i)
		}
		if !validated {
			t.Fatalf("item %d not validated", i)
		}
	}

	summarized, _ := ResultGet[bool](result, "summarized")
	if !summarized {
		t.Fatal("summarize step did not execute")
	}
}

func TestE2E_StageWithinForEach(t *testing.T) {
	ResetTaskRegistry()

	Task("sf.download", func(ctx *Ctx) error {
		item := Item[int](ctx)
		Set(ctx, fmt.Sprintf("downloaded_%d", item), true)
		return nil
	})
	Task("sf.process", func(ctx *Ctx) error {
		item := Item[int](ctx)
		Set(ctx, fmt.Sprintf("processed_%d", item), true)
		return nil
	})
	Task("sf.upload", func(ctx *Ctx) error {
		item := Item[int](ctx)
		Set(ctx, fmt.Sprintf("uploaded_%d", item), true)
		return nil
	})

	subWf, err := NewWorkflow("sf-sub").
		Stage("pipeline",
			Step("sf.download"),
			Step("sf.process"),
			Step("sf.upload"),
		).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	wf, err := NewWorkflow("sf-main").
		ForEach("files", Sub(subWf), WithConcurrency(3)).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	engine, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, Params{
		"files": []int{1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s", result.Status)
	}

	for i := 1; i <= 5; i++ {
		for _, phase := range []string{"downloaded", "processed", "uploaded"} {
			key := fmt.Sprintf("%s_%d", phase, i)
			val, ok := ResultGet[bool](result, key)
			if !ok || !val {
				t.Fatalf("missing or false: %s", key)
			}
		}
	}
}
