// Example 22: Loops (DoUntil and DoWhile)
//
// Demonstrates loop constructs. DoUntil runs the body first, then checks the
// condition (repeat-until). DoWhile checks first, then runs (while-do).
// Named variants (DoUntilNamed, DoWhileNamed) use registered conditions for
// serializability.
package main

import (
	"context"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	// --- Shared tasks ---
	gs.Task("increment", func(ctx *gs.Ctx) error {
		n := gs.GetOr[int](ctx, "counter", 0)
		n++
		fmt.Printf("  [increment] counter = %d\n", n)
		return gs.Set(ctx, "counter", n)
	})

	gs.Task("decrement", func(ctx *gs.Ctx) error {
		n := gs.Get[int](ctx, "countdown")
		n--
		fmt.Printf("  [decrement] countdown = %d\n", n)
		return gs.Set(ctx, "countdown", n)
	})

	// --- Named conditions for serializable loops ---
	gs.Condition("counter-reached-5", func(ctx *gs.Ctx) bool {
		return gs.Get[int](ctx, "counter") >= 5
	})

	gs.Condition("countdown-positive", func(ctx *gs.Ctx) bool {
		return gs.Get[int](ctx, "countdown") > 0
	})

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// --- DoUntilNamed: repeat body, then check condition ---
	fmt.Println("--- DoUntilNamed: increment until counter >= 5 ---")
	wf1, err := gs.NewWorkflow("do-until-demo").
		DoUntilNamed(gs.Step("increment"), "counter-reached-5").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	r1, err := engine.RunSync(context.Background(), wf1, gs.Params{"counter": 0})
	if err != nil {
		log.Fatal(err)
	}
	final, _ := gs.ResultGet[int](r1, "counter")
	fmt.Printf("  Final counter: %d  Status: %s\n\n", final, r1.Status)

	// --- DoWhileNamed: check condition, then run body ---
	fmt.Println("--- DoWhileNamed: decrement while countdown > 0 ---")
	wf2, err := gs.NewWorkflow("do-while-demo").
		DoWhileNamed(gs.Step("decrement"), "countdown-positive").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	r2, err := engine.RunSync(context.Background(), wf2, gs.Params{"countdown": 3})
	if err != nil {
		log.Fatal(err)
	}
	remaining, _ := gs.ResultGet[int](r2, "countdown")
	fmt.Printf("  Final countdown: %d  Status: %s\n", remaining, r2.Status)
}
