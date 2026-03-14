// Example 12: Deep Composition
//
// ForEach over a list of items with WithConcurrency(2). Each item runs a
// sub-workflow containing a Branch (even/odd based on ItemIndex) and a
// Parallel step (enrich + validate). A summary step after ForEach reads
// the per-item results. Demonstrates every step kind composing together.
package main

import (
	"context"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	gs.Task("tag.even", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		fmt.Printf("  [%d] %s -> even path\n", gs.ItemIndex(ctx), item)
		return gs.Set(ctx, fmt.Sprintf("tag_%d", gs.ItemIndex(ctx)), "even")
	})

	gs.Task("tag.odd", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		fmt.Printf("  [%d] %s -> odd path\n", gs.ItemIndex(ctx), item)
		return gs.Set(ctx, fmt.Sprintf("tag_%d", gs.ItemIndex(ctx)), "odd")
	})

	gs.Task("enrich", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		fmt.Printf("  [%d] enriching %s\n", gs.ItemIndex(ctx), item)
		return nil
	})

	gs.Task("validate", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		fmt.Printf("  [%d] validating %s\n", gs.ItemIndex(ctx), item)
		return nil
	})

	gs.Condition("is-even-index", func(ctx *gs.Ctx) bool {
		return gs.ItemIndex(ctx)%2 == 0
	})

	gs.Task("summary", func(ctx *gs.Ctx) error {
		fmt.Println("  --- summary ---")
		for i := 0; i < 4; i++ {
			tag := gs.GetOr[string](ctx, fmt.Sprintf("tag_%d", i), "?")
			fmt.Printf("  item %d: %s\n", i, tag)
		}
		return nil
	})

	inner, err := gs.NewWorkflow("per-item").
		Branch(
			gs.WhenNamed("is-even-index").Step("tag.even"),
			gs.Default().Step("tag.odd"),
		).
		Parallel(gs.Step("enrich"), gs.Step("validate")).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	wf, err := gs.NewWorkflow("deep-composition").
		ForEach("items", gs.Sub(inner), gs.WithConcurrency(2)).
		Step("summary").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	items := []string{"alpha", "beta", "gamma", "delta"}
	fmt.Println("=== Deep Composition ===")
	result, err := engine.RunSync(context.Background(), wf, gs.Params{"items": items})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", result.Status)
}
