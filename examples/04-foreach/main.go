// Example 04: ForEach with Concurrency
//
// A collection of items is stored in state. ForEach iterates over them with
// WithConcurrency(3), processing up to 3 items at a time. Each iteration
// uses Item[T] and ItemIndex to access the current element. A summary step
// counts the results.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	gs.Task("process-item", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		index := gs.ItemIndex(ctx)
		time.Sleep(50 * time.Millisecond) // simulate work
		result := fmt.Sprintf("processed(%s)", item)
		fmt.Printf("  [%d] %s -> %s\n", index, item, result)
		return gs.Set(ctx, fmt.Sprintf("result_%d", index), result)
	})

	gs.Task("summarize", func(ctx *gs.Ctx) error {
		count := 0
		for i := 0; i < 6; i++ {
			if _, ok := gs.GetOk[string](ctx, fmt.Sprintf("result_%d", i)); ok {
				count++
			}
		}
		fmt.Printf("\n[summarize] Processed %d items\n", count)
		return gs.Set(ctx, "total_processed", count)
	})

	wf, err := gs.NewWorkflow("batch-processing").
		ForEach("items", gs.Step("process-item"), gs.WithConcurrency(3)).
		Step("summarize").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	items := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}

	start := time.Now()
	result, err := engine.RunSync(context.Background(), wf, gs.Params{
		"items": items,
	})
	elapsed := time.Since(start)
	if err != nil {
		log.Fatal(err)
	}

	total, _ := gs.ResultGet[int](result, "total_processed")
	fmt.Printf("Total processed: %d\n", total)
	fmt.Printf("Elapsed: %s (6 items, concurrency=3)\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Status: %s\n", result.Status)
}
