// Example 06: Suspend and Resume
//
// Demonstrates the full suspend/resume lifecycle. A workflow suspends
// itself to wait for external "approval" input. The program:
//   1. Runs the workflow and gets a Suspended result
//   2. Inspects the suspend data
//   3. Calls Resume with approval data
//   4. Gets a Completed result with the approval recorded in state
//
// Uses SQLite persistence because suspend/resume requires durable state.
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

	gs.Task("submit-request", func(ctx *gs.Ctx) error {
		fmt.Println("[submit] Request submitted for approval")
		gs.Set(ctx, "request_id", "REQ-42")
		return nil
	})

	gs.Task("wait-for-approval", func(ctx *gs.Ctx) error {
		if gs.IsResuming(ctx) {
			approved := gs.ResumeData[bool](ctx, "approved")
			reviewer := gs.ResumeData[string](ctx, "reviewer")
			fmt.Printf("[approval] Resumed - approved=%v reviewer=%s\n", approved, reviewer)
			gs.Set(ctx, "approved", approved)
			gs.Set(ctx, "reviewer", reviewer)
			return nil
		}
		reqID := gs.Get[string](ctx, "request_id")
		fmt.Printf("[approval] Suspending, waiting for approval on %s\n", reqID)
		return gs.Suspend(ctx, gs.Params{"reason": "needs_approval", "request_id": reqID})
	})

	gs.Task("finalize", func(ctx *gs.Ctx) error {
		approved := gs.Get[bool](ctx, "approved")
		reviewer := gs.Get[string](ctx, "reviewer")
		fmt.Printf("[finalize] Request finalized (approved=%v, reviewer=%s)\n", approved, reviewer)
		return nil
	})

	wf, err := gs.NewWorkflow("approval-flow").
		Step("submit-request").
		Step("wait-for-approval").
		Step("finalize").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	dbPath := filepath.Join(os.TempDir(), "gostage-ex06.db")
	defer os.Remove(dbPath)

	engine, err := gs.New(gs.WithSQLite(dbPath))
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// Phase 1: Run until suspension
	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nStatus after run: %s\n", result.Status)
	fmt.Printf("Suspend data:    %v\n\n", result.SuspendData)

	// Phase 2: Resume with approval
	result, err = engine.Resume(context.Background(), result.RunID, gs.Params{
		"approved": true,
		"reviewer": "alice@example.com",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nStatus after resume: %s\n", result.Status)
}
