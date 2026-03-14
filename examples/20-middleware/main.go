// Example 13: Middleware
//
// Registers a StepMiddleware that logs each step's name and duration, and a
// TaskMiddleware that logs each task invocation with timing. Both are attached
// via engine options. A 3-step workflow runs to show the middleware output.
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

	gs.Task("fetch", func(ctx *gs.Ctx) error {
		time.Sleep(20 * time.Millisecond)
		return gs.Set(ctx, "data", "raw-payload")
	})
	gs.Task("transform", func(ctx *gs.Ctx) error {
		time.Sleep(10 * time.Millisecond)
		raw := gs.Get[string](ctx, "data")
		return gs.Set(ctx, "data", raw+"-transformed")
	})
	gs.Task("store", func(ctx *gs.Ctx) error {
		time.Sleep(5 * time.Millisecond)
		return gs.Set(ctx, "stored", true)
	})

	// StepMiddleware: wraps every step (tasks, parallel groups, etc.)
	stepMW := func(ctx context.Context, info gs.StepInfo, runID gs.RunID, next func() error) error {
		start := time.Now()
		err := next()
		fmt.Printf("[step-mw]  %-12s  %s\n", info.Name, time.Since(start).Round(time.Millisecond))
		return err
	}

	// TaskMiddleware: wraps every task function invocation
	taskMW := func(tctx *gs.Ctx, taskName string, next func() error) error {
		start := time.Now()
		err := next()
		fmt.Printf("[task-mw]  %-12s  %s\n", taskName, time.Since(start).Round(time.Millisecond))
		return err
	}

	wf, err := gs.NewWorkflow("pipeline").
		Step("fetch").
		Step("transform").
		Step("store").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New(
		gs.WithStepMiddleware(stepMW),
		gs.WithTaskMiddleware(taskMW),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\nStatus: %s\n", result.Status)
	stored, _ := gs.ResultGet[bool](result, "stored")
	fmt.Printf("Stored: %v\n", stored)
}
