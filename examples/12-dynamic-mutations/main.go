// Example 19: Dynamic Mutations
//
// Demonstrates runtime workflow mutations. A step calls InsertAfter to inject
// a new step after itself, and another step calls DisableStep to skip a step.
// The execution order changes dynamically and is printed to show the effect.
package main

import (
	"context"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	gs.Task("step-a", func(ctx *gs.Ctx) error {
		fmt.Println("  [1] step-a: inserting 'bonus-step' after me")
		gs.InsertAfter(ctx, "bonus-step")
		return nil
	})

	gs.Task("bonus-step", func(ctx *gs.Ctx) error {
		fmt.Println("  [2] bonus-step: I was dynamically inserted!")
		return nil
	})

	gs.Task("step-b", func(ctx *gs.Ctx) error {
		fmt.Println("  [3] step-b: disabling step-c")
		gs.DisableStep(ctx, "step-c")
		return nil
	})

	gs.Task("step-c", func(ctx *gs.Ctx) error {
		fmt.Println("  [4] step-c: you should NOT see this")
		return nil
	})

	gs.Task("step-d", func(ctx *gs.Ctx) error {
		fmt.Println("  [4] step-d: final step runs normally")
		return nil
	})

	wf, err := gs.NewWorkflow("mutations-demo").
		Step("step-a").
		Step("step-b").
		Step("step-c").
		Step("step-d").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	fmt.Println("--- Dynamic Mutations ---")
	fmt.Println("Original order: step-a, step-b, step-c, step-d")
	fmt.Println("Expected: step-a, bonus-step (inserted), step-b, step-c (disabled/skipped), step-d")
	fmt.Println()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nStatus: %s\n", result.Status)
}
