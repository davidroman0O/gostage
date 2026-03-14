// Example 02: Multi-Step Pipeline
//
// Three sequential steps that pass data through shared state:
//   1. fetch  - reads a URL from params and writes raw data
//   2. transform - uppercases the raw data
//   3. save   - marks the result as saved
//
// Demonstrates state flowing between steps via Get/Set.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	gs "github.com/davidroman0O/gostage"
)

func main() {
	gs.ResetTaskRegistry()

	gs.Task("fetch", func(ctx *gs.Ctx) error {
		url := gs.Get[string](ctx, "url")
		fmt.Printf("[fetch] Fetching from %s\n", url)
		return gs.Set(ctx, "raw_data", "hello from "+url)
	})

	gs.Task("transform", func(ctx *gs.Ctx) error {
		raw := gs.Get[string](ctx, "raw_data")
		transformed := strings.ToUpper(raw)
		fmt.Printf("[transform] %q -> %q\n", raw, transformed)
		return gs.Set(ctx, "transformed", transformed)
	})

	gs.Task("save", func(ctx *gs.Ctx) error {
		data := gs.Get[string](ctx, "transformed")
		fmt.Printf("[save] Saving: %s\n", data)
		return gs.Set(ctx, "saved", true)
	})

	wf, err := gs.NewWorkflow("pipeline").
		Step("fetch").
		Step("transform").
		Step("save").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, err := gs.New()
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	result, err := engine.RunSync(context.Background(), wf, gs.Params{
		"url": "https://api.example.com/data",
	})
	if err != nil {
		log.Fatal(err)
	}

	final, _ := gs.ResultGet[string](result, "transformed")
	saved, _ := gs.ResultGet[bool](result, "saved")
	fmt.Printf("\nResult: %s (saved=%v, status=%s)\n", final, saved, result.Status)
}
