// Package gostage provides a workflow engine with persistence, child process
// spawning, and type-safe state management.
//
// Quick start:
//
//	gostage.Task("greet", func(ctx *gostage.Ctx) error {
//	    name := gostage.GetOr[string](ctx, "name", "World")
//	    ctx.Log.Info("Hello, %s!", name)
//	    return nil
//	})
//
//	wf := gostage.NewWorkflow("hello").Step("greet").Commit()
//	engine, _ := gostage.New()
//	result, _ := engine.RunSync(ctx, wf, gostage.P{"name": "Alice"})
package gostage
