// Example 03: Parallel Execution
//
// Three extraction tasks run concurrently inside a Parallel step. Each writes
// to a different key in state. A merge task runs after all three complete and
// combines their results. Timing output confirms concurrent execution.
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

	gs.Task("extract-users", func(ctx *gs.Ctx) error {
		time.Sleep(100 * time.Millisecond) // simulate work
		return gs.Set(ctx, "users", 150)
	})

	gs.Task("extract-orders", func(ctx *gs.Ctx) error {
		time.Sleep(100 * time.Millisecond)
		return gs.Set(ctx, "orders", 340)
	})

	gs.Task("extract-products", func(ctx *gs.Ctx) error {
		time.Sleep(100 * time.Millisecond)
		return gs.Set(ctx, "products", 89)
	})

	gs.Task("merge", func(ctx *gs.Ctx) error {
		users := gs.Get[int](ctx, "users")
		orders := gs.Get[int](ctx, "orders")
		products := gs.Get[int](ctx, "products")
		summary := fmt.Sprintf("users=%d, orders=%d, products=%d", users, orders, products)
		fmt.Printf("[merge] %s\n", summary)
		return gs.Set(ctx, "summary", summary)
	})

	wf, err := gs.NewWorkflow("parallel-extract").
		Parallel(
			gs.Step("extract-users"),
			gs.Step("extract-orders"),
			gs.Step("extract-products"),
		).
		Step("merge").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	start := time.Now()
	result, err := engine.RunSync(context.Background(), wf, nil)
	elapsed := time.Since(start)
	if err != nil {
		log.Fatal(err)
	}

	summary, _ := gs.ResultGet[string](result, "summary")
	fmt.Printf("\nResult: %s\n", summary)
	fmt.Printf("Elapsed: %s (sequential would be ~300ms)\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Status: %s\n", result.Status)
}
