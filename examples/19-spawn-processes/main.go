// Example 17: Child Process Spawning
//
// Demonstrates ForEach with child process spawning — each item runs in an
// isolated OS process for crash isolation. If one child crashes, others
// continue. The parent and child communicate via gRPC.
//
// Key concepts:
//   - spawn.IsChild() / spawn.HandleChild() detect and run child processes
//   - spawn.WithSpawn() engine option enables the spawn runner
//   - gostage.WithSpawn() ForEach option runs items in child processes
//   - Tasks must be registered in BOTH parent and child
//   - The same binary is re-executed as a child with --gostage-child flag
//
// Run:
//
//	go run ./examples/17-spawn-processes/
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	gs "github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/spawn"
)

// registerTasks registers tasks used by both parent and child processes.
// This function MUST be called in both code paths.
func registerTasks() {
	gs.Task("process-item", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		result := fmt.Sprintf("[child pid=%d] processed item %d: %s", os.Getpid(), idx, item)
		fmt.Println(result)
		gs.Set(ctx, fmt.Sprintf("result_%d", idx), result)
		return nil
	})

	gs.Task("summarize", func(ctx *gs.Ctx) error {
		fmt.Printf("[parent pid=%d] all items processed\n", os.Getpid())
		gs.Set(ctx, "done", true)
		return nil
	})
}

func main() {
	// Child process detection: if this binary was re-executed by gostage
	// as a child worker, run the child lifecycle and exit.
	if spawn.IsChild() {
		registerTasks()
		spawn.HandleChild() // never returns — calls os.Exit
	}

	// === Parent process ===
	gs.ResetTaskRegistry()
	registerTasks()

	wf, err := gs.NewWorkflow("spawn-demo").
		ForEach("items", gs.Step("process-item"), gs.WithSpawn()). // each item = separate OS process
		Step("summarize").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	dbPath := filepath.Join(os.TempDir(), "gostage-ex17.db")
	defer os.Remove(dbPath)

	engine, err := gs.New(
		gs.WithSQLite(dbPath),
		spawn.WithSpawn(), // enable the gRPC spawn runner
	)
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	fmt.Printf("=== Spawn Demo (parent pid=%d) ===\n\n", os.Getpid())

	result, err := engine.RunSync(context.Background(), wf, gs.Params{
		"items": []string{"alpha", "bravo", "charlie", "delta"},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\nStatus: %s\n", result.Status)
	for i := 0; i < 4; i++ {
		val, _ := gs.ResultGet[string](result, fmt.Sprintf("result_%d", i))
		fmt.Printf("  result_%d: %s\n", i, val)
	}
}
