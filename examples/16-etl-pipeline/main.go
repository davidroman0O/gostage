// Example 10: ETL Pipeline
//
// Parallel extraction from three simulated data sources, a named map step
// that combines the extracted counts, and a final load step that writes
// the total. Uses MapNamed for serializability.
package main

import (
	"context"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	gs.Task("extract.users", func(ctx *gs.Ctx) error {
		fmt.Println("  extracting users")
		return gs.Set(ctx, "users_count", 120)
	})
	gs.Task("extract.orders", func(ctx *gs.Ctx) error {
		fmt.Println("  extracting orders")
		return gs.Set(ctx, "orders_count", 350)
	})
	gs.Task("extract.products", func(ctx *gs.Ctx) error {
		fmt.Println("  extracting products")
		return gs.Set(ctx, "products_count", 80)
	})

	gs.MapFn("combine-counts", func(ctx *gs.Ctx) error {
		total := gs.Get[int](ctx, "users_count") +
			gs.Get[int](ctx, "orders_count") +
			gs.Get[int](ctx, "products_count")
		fmt.Printf("  combined total: %d\n", total)
		return gs.Set(ctx, "total", total)
	})

	gs.Task("load", func(ctx *gs.Ctx) error {
		total := gs.Get[int](ctx, "total")
		fmt.Printf("  loaded %d records into warehouse\n", total)
		return nil
	})

	wf, err := gs.NewWorkflow("etl").
		Parallel(gs.Step("extract.users"), gs.Step("extract.orders"), gs.Step("extract.products")).
		MapNamed("combine-counts").
		Step("load").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	fmt.Println("=== ETL Pipeline ===")
	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", result.Status)
}
