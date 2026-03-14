// Example 15: Custom Registry
//
// Creates two separate registries with NewRegistry, registers different task
// implementations under the same name in each, creates two engines with
// WithRegistry, and runs the same workflow on both. The outputs differ,
// demonstrating complete engine isolation.
package main

import (
	"context"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	// Registry A: "compute" doubles the value.
	regA := gs.NewRegistry()
	regA.RegisterTask("compute", func(ctx *gs.Ctx) error {
		n := gs.GetOr[int](ctx, "input", 0)
		return gs.Set(ctx, "output", n*2)
	})

	// Registry B: "compute" squares the value.
	regB := gs.NewRegistry()
	regB.RegisterTask("compute", func(ctx *gs.Ctx) error {
		n := gs.GetOr[int](ctx, "input", 0)
		return gs.Set(ctx, "output", n*n)
	})

	// Build the same workflow definition for both engines.
	wfA, err := gs.NewWorkflow("math").Step("compute").Commit()
	if err != nil {
		log.Fatal(err)
	}
	wfB, err := gs.NewWorkflow("math").Step("compute").Commit()
	if err != nil {
		log.Fatal(err)
	}

	// Engine A uses registry A (doubler).
	engineA, err := gs.New(gs.WithRegistry(regA))
	if err != nil {
		log.Fatal(err)
	}
	defer engineA.Close()

	// Engine B uses registry B (squarer).
	engineB, err := gs.New(gs.WithRegistry(regB))
	if err != nil {
		log.Fatal(err)
	}
	defer engineB.Close()

	params := gs.Params{"input": 5}

	resultA, err := engineA.RunSync(context.Background(), wfA, params)
	if err != nil {
		log.Fatal(err)
	}
	resultB, err := engineB.RunSync(context.Background(), wfB, params)
	if err != nil {
		log.Fatal(err)
	}

	outA, _ := gs.ResultGet[int](resultA, "output")
	outB, _ := gs.ResultGet[int](resultB, "output")

	fmt.Printf("Input:    %d\n", 5)
	fmt.Printf("Engine A (doubler): %d\n", outA) // 10
	fmt.Printf("Engine B (squarer): %d\n", outB) // 25
	fmt.Printf("Isolated: %v\n", outA != outB)   // true
}
