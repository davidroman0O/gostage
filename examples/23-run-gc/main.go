// Example 16: Run Garbage Collection
//
// Creates an engine with WithRunGC(0, 100ms) so completed runs are purged
// every 100ms. Runs 5 workflows, lists them to confirm they exist, waits
// for the GC interval, and lists again to show they were purged. Also
// demonstrates manual PurgeRuns.
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

	gs.Task("noop", func(ctx *gs.Ctx) error {
		return gs.Set(ctx, "done", true)
	})

	engine, err := gs.New(gs.WithRunGC(0, 100*time.Millisecond))
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()

	// Run 5 workflows.
	for i := 0; i < 5; i++ {
		wf, err := gs.NewWorkflow(fmt.Sprintf("job-%d", i)).Step("noop").Commit()
		if err != nil {
			log.Fatal(err)
		}
		if _, err := engine.RunSync(ctx, wf, nil); err != nil {
			log.Fatal(err)
		}
	}

	// List runs immediately after completion.
	runs, _ := engine.ListRuns(ctx, gs.RunFilter{})
	fmt.Printf("Runs after execution: %d\n", len(runs))

	// Wait for GC to fire.
	time.Sleep(250 * time.Millisecond)

	runs, _ = engine.ListRuns(ctx, gs.RunFilter{})
	fmt.Printf("Runs after GC:        %d\n", len(runs))

	// Run 3 more, then manually purge.
	for i := 5; i < 8; i++ {
		wf, err := gs.NewWorkflow(fmt.Sprintf("job-%d", i)).Step("noop").Commit()
		if err != nil {
			log.Fatal(err)
		}
		if _, err := engine.RunSync(ctx, wf, nil); err != nil {
			log.Fatal(err)
		}
	}

	runs, _ = engine.ListRuns(ctx, gs.RunFilter{})
	fmt.Printf("Runs before purge:    %d\n", len(runs))

	purged, err := engine.PurgeRuns(ctx, 0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Manually purged:      %d\n", purged)

	runs, _ = engine.ListRuns(ctx, gs.RunFilter{})
	fmt.Printf("Runs after purge:     %d\n", len(runs))
}
