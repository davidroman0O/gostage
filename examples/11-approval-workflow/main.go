// Example 11: Approval Workflow
//
// A triage step evaluates priority. A branch routes urgent tickets through
// an expedited path (which dynamically inserts an audit step via InsertAfter)
// and normal tickets through a standard review. Uses WhenNamed for
// serializable conditions. Run twice to show both execution paths.
package main

import (
	"context"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	gs.Task("triage", func(ctx *gs.Ctx) error {
		p := gs.Get[string](ctx, "priority")
		fmt.Printf("  triage: priority=%s\n", p)
		return nil
	})

	gs.Task("urgent.process", func(ctx *gs.Ctx) error {
		fmt.Println("  urgent: fast-tracking")
		gs.InsertAfter(ctx, "audit")
		return nil
	})

	gs.Task("audit", func(ctx *gs.Ctx) error {
		fmt.Println("  audit: compliance check passed")
		return nil
	})

	gs.Task("normal.review", func(ctx *gs.Ctx) error {
		fmt.Println("  normal: standard review complete")
		return nil
	})

	gs.Task("finalize", func(ctx *gs.Ctx) error {
		fmt.Println("  finalized")
		return nil
	})

	gs.Condition("is-urgent", func(ctx *gs.Ctx) bool {
		return gs.Get[string](ctx, "priority") == "urgent"
	})

	wf, err := gs.NewWorkflow("approval").
		Step("triage").
		Branch(
			gs.WhenNamed("is-urgent").Step("urgent.process"),
			gs.Default().Step("normal.review"),
		).
		Step("finalize").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	fmt.Println("=== Run 1: urgent ===")
	r1, err := engine.RunSync(context.Background(), wf, gs.Params{"priority": "urgent"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n\n", r1.Status)

	fmt.Println("=== Run 2: normal ===")
	r2, err := engine.RunSync(context.Background(), wf, gs.Params{"priority": "normal"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %s\n", r2.Status)
}
