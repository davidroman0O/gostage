// Example 09: Order Processing (Real-World Suspend/Resume)
//
// A full e-commerce workflow demonstrating named conditions, named map
// functions, and suspend/resume with business logic:
//   1. validate   - Checks the order has items and a customer
//   2. calculate  - Named map function computes the total price
//   3. charge     - Suspends to wait for external payment confirmation
//   4. (resume)   - Payment provider calls Resume with transaction ID
//   5. ship       - Ships the order
//   6. notify     - Sends a confirmation notification
//
// All conditions and map functions use Named variants so the workflow
// definition is fully serializable and can survive a crash.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	// -- Register tasks --

	gs.Task("order.validate", func(ctx *gs.Ctx) error {
		customer := gs.Get[string](ctx, "customer")
		items := gs.Get[[]any](ctx, "items")
		if customer == "" || len(items) == 0 {
			return gs.Bail(ctx, "invalid order: missing customer or items")
		}
		fmt.Printf("[validate] Order for %s with %d item(s)\n", customer, len(items))
		gs.Set(ctx, "status", "validated")
		return nil
	})

	// Named map function for serializability
	gs.MapFn("order.calculate-total", func(ctx *gs.Ctx) error {
		items := gs.Get[[]any](ctx, "items")
		total := 0.0
		for _, item := range items {
			if m, ok := item.(map[string]any); ok {
				if price, ok := m["price"].(float64); ok {
					total += price
				}
			}
		}
		fmt.Printf("[calculate] Total: $%.2f\n", total)
		gs.Set(ctx, "total", total)
		return nil
	})

	gs.Task("order.charge", func(ctx *gs.Ctx) error {
		if gs.IsResuming(ctx) {
			txnID := gs.ResumeData[string](ctx, "transaction_id")
			fmt.Printf("[charge] Payment received, transaction: %s\n", txnID)
			gs.Set(ctx, "transaction_id", txnID)
			gs.Set(ctx, "status", "paid")
			return nil
		}
		total := gs.Get[float64](ctx, "total")
		fmt.Printf("[charge] Requesting payment of $%.2f, suspending...\n", total)
		return gs.Suspend(ctx, gs.Params{
			"action": "payment_required",
			"amount": total,
		})
	})

	gs.Task("order.ship", func(ctx *gs.Ctx) error {
		customer := gs.Get[string](ctx, "customer")
		trackingNo := fmt.Sprintf("TRACK-%s", gs.Get[string](ctx, "transaction_id")[:8])
		fmt.Printf("[ship] Shipping to %s, tracking: %s\n", customer, trackingNo)
		gs.Set(ctx, "tracking", trackingNo)
		gs.Set(ctx, "status", "shipped")
		return nil
	})

	gs.Task("order.notify", func(ctx *gs.Ctx) error {
		customer := gs.Get[string](ctx, "customer")
		tracking := gs.Get[string](ctx, "tracking")
		fmt.Printf("[notify] Sent confirmation to %s (tracking: %s)\n", customer, tracking)
		gs.Set(ctx, "status", "complete")
		return nil
	})

	// -- Build workflow --

	wf, err := gs.NewWorkflow("process-order").
		Step("order.validate").
		MapNamed("order.calculate-total").
		Step("order.charge").
		Step("order.ship").
		Step("order.notify").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	dbPath := filepath.Join(os.TempDir(), "gostage-ex09.db")
	defer os.Remove(dbPath)

	engine, err := gs.New(gs.WithSQLite(dbPath))
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// Phase 1: Submit order, workflow suspends at payment
	fmt.Println("=== Phase 1: Submit Order ===")
	result, err := engine.RunSync(context.Background(), wf, gs.Params{
		"customer": "Jane Doe",
		"items": []map[string]any{
			{"name": "Widget", "price": 29.99},
			{"name": "Gadget", "price": 49.99},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nStatus: %s\n", result.Status)
	fmt.Printf("Suspend: %v\n", result.SuspendData)

	// Phase 2: Payment provider calls back, resume the workflow
	fmt.Println("\n=== Phase 2: Payment Confirmed ===")
	result, err = engine.Resume(context.Background(), result.RunID, gs.Params{
		"transaction_id": "TXN-8f3a9b2c-1234",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\nFinal status: %s\n", result.Status)
	status, _ := gs.ResultGet[string](result, "status")
	tracking, _ := gs.ResultGet[string](result, "tracking")
	fmt.Printf("Order status: %s\n", status)
	fmt.Printf("Tracking:     %s\n", tracking)
}
