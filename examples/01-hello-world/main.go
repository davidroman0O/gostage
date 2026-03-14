// Example 01: Hello World
//
// The simplest possible gostage workflow. Registers one task that reads a
// name from state and writes a greeting back. Builds a single-step workflow,
// runs it synchronously, and prints the result.
package main

import (
	"context"
	"fmt"
	"log"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	gs.Task("greet", func(ctx *gs.Ctx) error {
		name := gs.GetOr[string](ctx, "name", "World")
		return gs.Set(ctx, "greeting", fmt.Sprintf("Hello, %s!", name))
	})

	wf, err := gs.NewWorkflow("hello").Step("greet").Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, gs.Params{"name": "Alice"})
	if err != nil {
		log.Fatal(err)
	}

	greeting, _ := gs.ResultGet[string](result, "greeting")
	fmt.Println(greeting) // Hello, Alice!
}
