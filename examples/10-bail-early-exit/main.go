// Example 18: Bail Early Exit
//
// Demonstrates Bail() for intentional early workflow exit. A validation step
// calls Bail when an order total is invalid. Bail is NOT an error -- it signals
// a deliberate early exit. The result has Status == Bailed and a BailReason.
package main

import (
	"context"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	gs.Task("validate-order", func(ctx *gs.Ctx) error {
		total := gs.Get[float64](ctx, "total")
		if total <= 0 {
			return gs.Bail(ctx, fmt.Sprintf("invalid order total: %.2f", total))
		}
		fmt.Println("  [validate] order is valid")
		return nil
	})

	gs.Task("charge-payment", func(ctx *gs.Ctx) error {
		fmt.Println("  [charge] processing payment")
		return nil
	})

	wf, err := gs.NewWorkflow("order-check").
		Step("validate-order").
		Step("charge-payment").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// Run with an invalid total -- should bail
	fmt.Println("--- Run 1: Invalid order (should bail) ---")
	r1, err := engine.RunSync(context.Background(), wf, gs.Params{"total": -5.0})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Status: %s\n", r1.Status)
	fmt.Printf("  BailReason: %s\n", r1.BailReason)
	fmt.Printf("  Is error? %v (Bail is NOT an error)\n\n", r1.Error != nil)

	// Run with a valid total -- should complete
	fmt.Println("--- Run 2: Valid order (should complete) ---")
	r2, err := engine.RunSync(context.Background(), wf, gs.Params{"total": 49.99})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Status: %s\n", r2.Status)
}
