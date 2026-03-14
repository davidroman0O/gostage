// Example 14: Event Handler
//
// Implements the EventHandler interface to collect all engine lifecycle events.
// Registers the handler via WithEventHandler, runs a workflow, and prints
// the full event timeline with timestamps and run metadata.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	gs "github.com/davidroman0O/gostage"
)

// timeline collects events in order.
type timeline struct {
	mu     sync.Mutex
	events []gs.EngineEvent
}

func (t *timeline) OnEvent(event gs.EngineEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, event)
}

func eventName(et gs.EventType) string {
	switch et {
	case gs.EventRunCreated:
		return "Created"
	case gs.EventRunCompleted:
		return "Completed"
	case gs.EventRunFailed:
		return "Failed"
	case gs.EventRunBailed:
		return "Bailed"
	case gs.EventRunSuspended:
		return "Suspended"
	case gs.EventRunSleeping:
		return "Sleeping"
	case gs.EventRunCancelled:
		return "Cancelled"
	case gs.EventRunDeleted:
		return "Deleted"
	case gs.EventRunWoken:
		return "Woken"
	default:
		return "Unknown"
	}
}

func main() {
	gs.ResetTaskRegistry()

	gs.Task("step-a", func(ctx *gs.Ctx) error { return gs.Set(ctx, "a", 1) })
	gs.Task("step-b", func(ctx *gs.Ctx) error { return gs.Set(ctx, "b", 2) })

	wf, err := gs.NewWorkflow("event-demo").
		Step("step-a").
		Step("step-b").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	tl := &timeline{}
	engine, err := gs.New(gs.WithEventHandler(tl))
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Event timeline:")
	tl.mu.Lock()
	for i, ev := range tl.events {
		fmt.Printf("  %d. %-10s  workflow=%-12s  status=%-10s  run=%s\n",
			i+1, eventName(ev.Type), ev.WorkflowID, ev.Status, ev.RunID[:8])
	}
	tl.mu.Unlock()

	fmt.Printf("\nFinal status: %s\n", result.Status)
}
