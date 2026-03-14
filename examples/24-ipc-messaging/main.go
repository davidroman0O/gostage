// Example 20: IPC Messaging
//
// Demonstrates the engine's IPC messaging system. A task calls Send() to emit
// messages. An engine-wide OnMessage handler receives all messages, while
// OnMessageForRun receives only messages from a specific run.
package main

import (
	"context"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	gs.Task("process-data", func(ctx *gs.Ctx) error {
		gs.Send(ctx, "progress", gs.Params{"pct": 25, "step": "loading"})
		gs.Send(ctx, "progress", gs.Params{"pct": 75, "step": "transforming"})
		gs.Send(ctx, "progress", gs.Params{"pct": 100, "step": "done"})
		return gs.Set(ctx, "result", "all data processed")
	})

	wf, err := gs.NewWorkflow("ipc-demo").Step("process-data").Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// Engine-wide handler: receives messages from ALL runs
	hID := engine.OnMessage("progress", func(msgType string, payload map[string]any) {
		fmt.Printf("  [engine-wide] type=%s pct=%v step=%v\n", msgType, payload["pct"], payload["step"])
	})
	defer engine.OffMessage(hID)

	fmt.Println("--- Run 1: With engine-wide handler ---")
	r1, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		log.Fatal(err)
	}
	result, _ := gs.ResultGet[string](r1, "result")
	fmt.Printf("  Result: %s\n\n", result)

	// Run-scoped handler: only fires for a specific run
	fmt.Println("--- Run 2: With run-scoped handler ---")
	runID, err := engine.Run(context.Background(), wf, nil)
	if err != nil {
		log.Fatal(err)
	}
	scopedID := engine.OnMessageForRun("progress", runID, func(msgType string, payload map[string]any) {
		fmt.Printf("  [scoped] run=%s pct=%v\n", runID, payload["pct"])
	})
	defer engine.OffMessage(scopedID)

	r2, err := engine.Wait(context.Background(), runID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  Status: %s\n", r2.Status)
}
