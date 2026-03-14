// Example 05: Conditional Branching
//
// Registers a named condition with Condition() and uses WhenNamed plus Default
// to create a Branch step. The workflow is run twice with different params to
// demonstrate both the "when" and "default" paths.
package main

import (
	"context"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	// Register the named condition for serializable branching.
	gs.Condition("is-premium", func(ctx *gs.Ctx) bool {
		return gs.Get[string](ctx, "tier") == "premium"
	})

	gs.Task("premium-flow", func(ctx *gs.Ctx) error {
		user := gs.Get[string](ctx, "user")
		msg := fmt.Sprintf("Welcome back, %s! Enjoy your premium benefits.", user)
		fmt.Printf("  [premium] %s\n", msg)
		return gs.Set(ctx, "message", msg)
	})

	gs.Task("standard-flow", func(ctx *gs.Ctx) error {
		user := gs.Get[string](ctx, "user")
		msg := fmt.Sprintf("Hello, %s. Upgrade to premium for more features!", user)
		fmt.Printf("  [standard] %s\n", msg)
		return gs.Set(ctx, "message", msg)
	})

	wf, err := gs.NewWorkflow("onboarding").
		Branch(
			gs.WhenNamed("is-premium").Step("premium-flow"),
			gs.Default().Step("standard-flow"),
		).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// Run 1: premium user
	fmt.Println("--- Run 1: Premium user ---")
	r1, err := engine.RunSync(context.Background(), wf, gs.Params{
		"user": "Alice",
		"tier": "premium",
	})
	if err != nil {
		log.Fatal(err)
	}
	msg1, _ := gs.ResultGet[string](r1, "message")
	fmt.Printf("  Result: %s\n\n", msg1)

	// Run 2: standard user (default branch)
	fmt.Println("--- Run 2: Standard user ---")
	r2, err := engine.RunSync(context.Background(), wf, gs.Params{
		"user": "Bob",
		"tier": "free",
	})
	if err != nil {
		log.Fatal(err)
	}
	msg2, _ := gs.ResultGet[string](r2, "message")
	fmt.Printf("  Result: %s\n", msg2)
}
