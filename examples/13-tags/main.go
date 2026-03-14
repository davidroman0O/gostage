// Example 23: Tags
//
// Demonstrates tag-based workflow control. Tasks are registered with WithTags,
// steps are tagged with WithStepTags, and at runtime FindStepsByTag discovers
// tagged steps. DisableByTag disables all steps with a given tag at once.
package main

import (
	"context"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	gs.Task("validate", func(ctx *gs.Ctx) error {
		fmt.Println("  [validate] checking order...")
		// Discover all steps tagged "billing"
		billingSteps := gs.FindStepsByTag(ctx, "billing")
		fmt.Printf("  [validate] found %d billing steps: %v\n", len(billingSteps), billingSteps)

		// If the order is a free trial, disable all billing steps
		if gs.Get[bool](ctx, "free_trial") {
			fmt.Println("  [validate] free trial -- disabling all 'billing' steps")
			gs.DisableByTag(ctx, "billing")
		}
		return nil
	}, gs.WithTags("setup"))

	gs.Task("charge", func(ctx *gs.Ctx) error {
		fmt.Println("  [charge] charging credit card")
		return nil
	}, gs.WithTags("billing", "critical"))

	gs.Task("invoice", func(ctx *gs.Ctx) error {
		fmt.Println("  [invoice] generating invoice")
		return nil
	}, gs.WithTags("billing"))

	gs.Task("provision", func(ctx *gs.Ctx) error {
		fmt.Println("  [provision] setting up account")
		return nil
	}, gs.WithTags("fulfillment"))

	wf, err := gs.NewWorkflow("order-flow").
		Step("validate", gs.WithStepTags("setup")).
		Step("charge", gs.WithStepTags("billing", "critical")).
		Step("invoice", gs.WithStepTags("billing")).
		Step("provision", gs.WithStepTags("fulfillment")).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// Run 1: Normal order -- all steps execute
	fmt.Println("--- Run 1: Paid order (all steps) ---")
	r1, err := engine.RunSync(context.Background(), wf, gs.Params{"free_trial": false})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Status: %s\n\n", r1.Status)

	// Run 2: Free trial -- billing steps are disabled
	fmt.Println("--- Run 2: Free trial (billing disabled) ---")
	r2, err := engine.RunSync(context.Background(), wf, gs.Params{"free_trial": true})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Status: %s\n", r2.Status)
}
