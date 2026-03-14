// Example 21: Async Run, Wait, and Cancel
//
// Demonstrates non-blocking workflow execution with Run(), blocking with Wait(),
// and cancellation with Cancel(). Run returns a RunID immediately. Wait blocks
// until the run completes. Cancel aborts a slow workflow mid-execution.
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

	gs.Task("fast-task", func(ctx *gs.Ctx) error {
		fmt.Println("  [fast-task] completed instantly")
		return gs.Set(ctx, "done", true)
	})

	gs.Task("slow-task", func(ctx *gs.Ctx) error {
		fmt.Println("  [slow-task] starting long operation...")
		select {
		case <-time.After(10 * time.Second):
			fmt.Println("  [slow-task] finished")
		case <-ctx.Context().Done():
			fmt.Println("  [slow-task] cancelled!")
			return ctx.Context().Err()
		}
		return nil
	})

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// --- Part 1: Async Run + Wait ---
	fmt.Println("--- Part 1: Async Run + Wait ---")
	fastWf, err := gs.NewWorkflow("fast").Step("fast-task").Commit()
	if err != nil {
		log.Fatal(err)
	}

	runID, err := engine.Run(context.Background(), fastWf, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Workflow started! RunID: %s\n", runID)
	fmt.Println("  (non-blocking -- we can do other work here)")

	result, err := engine.Wait(context.Background(), runID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Wait returned: status=%s\n\n", result.Status)

	// --- Part 2: Cancel a slow workflow ---
	fmt.Println("--- Part 2: Cancel ---")
	slowWf, err := gs.NewWorkflow("slow").Step("slow-task").Commit()
	if err != nil {
		log.Fatal(err)
	}

	slowID, err := engine.Run(context.Background(), slowWf, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Slow workflow started: %s\n", slowID)

	// Give the task a moment to start, then cancel
	time.Sleep(100 * time.Millisecond)
	fmt.Println("  Cancelling...")
	if err := engine.Cancel(context.Background(), slowID); err != nil {
		log.Fatal(err)
	}

	cancelResult, err := engine.Wait(context.Background(), slowID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  After cancel: status=%s\n", cancelResult.Status)
}
