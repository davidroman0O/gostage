// Example 24: Serialization Round-Trip
//
// Demonstrates full workflow serialization. A workflow is built with named tasks
// and conditions (serializable). It is converted to a WorkflowDef, marshalled
// to JSON, printed, unmarshalled back, rebuilt with NewWorkflowFromDef, and
// executed to verify the round-trip produces an identical workflow.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	// Register tasks and named condition
	gs.Task("fetch-data", func(ctx *gs.Ctx) error {
		fmt.Println("  [fetch-data] loading records")
		return gs.Set(ctx, "count", 3)
	})

	gs.Task("process", func(ctx *gs.Ctx) error {
		n := gs.Get[int](ctx, "count")
		fmt.Printf("  [process] handling %d records\n", n)
		return gs.Set(ctx, "processed", true)
	})

	gs.Condition("has-data", func(ctx *gs.Ctx) bool {
		return gs.Get[int](ctx, "count") > 0
	})

	// Build a serializable workflow (uses WhenNamed, not anonymous When)
	wf, err := gs.NewWorkflow("pipeline").
		Step("fetch-data").
		Branch(
			gs.WhenNamed("has-data").Step("process"),
			gs.Default().Step("fetch-data"),
		).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	// --- Step 1: Convert to definition ---
	def, err := gs.WorkflowToDefinition(wf)
	if err != nil {
		log.Fatal("WorkflowToDefinition:", err)
	}

	// --- Step 2: Marshal to JSON ---
	data, err := gs.MarshalWorkflowDefinition(def)
	if err != nil {
		log.Fatal("Marshal:", err)
	}

	// Pretty-print the JSON
	fmt.Println("--- Serialized workflow definition ---")
	var pretty json.RawMessage = data
	formatted, _ := json.MarshalIndent(pretty, "", "  ")
	fmt.Println(string(formatted))

	// --- Step 3: Unmarshal back ---
	def2, err := gs.UnmarshalWorkflowDefinition(data)
	if err != nil {
		log.Fatal("Unmarshal:", err)
	}

	// --- Step 4: Rebuild workflow from definition ---
	rebuilt, err := gs.NewWorkflowFromDef(def2)
	if err != nil {
		log.Fatal("NewWorkflowFromDef:", err)
	}

	// --- Step 5: Run the rebuilt workflow ---
	fmt.Println("\n--- Running rebuilt workflow ---")
	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), rebuilt, nil)
	if err != nil {
		log.Fatal(err)
	}

	processed, _ := gs.ResultGet[bool](result, "processed")
	fmt.Printf("  Status: %s  processed=%v\n", result.Status, processed)
}
