package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	gs "github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/spawn"
)

// ===========================================================================
// Custom Logger — implements gs.Logger
// ===========================================================================

type consoleLogger struct{ prefix string }

func (l *consoleLogger) Debug(f string, a ...any) {
	fmt.Printf("[DEBUG] %s "+f+"\n", append([]any{l.prefix}, a...)...)
}
func (l *consoleLogger) Info(f string, a ...any) {
	fmt.Printf("[INFO]  %s "+f+"\n", append([]any{l.prefix}, a...)...)
}
func (l *consoleLogger) Warn(f string, a ...any) {
	fmt.Printf("[WARN]  %s "+f+"\n", append([]any{l.prefix}, a...)...)
}
func (l *consoleLogger) Error(f string, a ...any) {
	fmt.Printf("[ERROR] %s "+f+"\n", append([]any{l.prefix}, a...)...)
}

func main() {
	// ── Child process support ──────────────────────────────────────────
	// If this binary was spawned by gostage as a child worker, run child
	// lifecycle and exit. Tasks must be registered before HandleChild().
	if spawn.IsChild() {
		registerAllTasks()
		registerNamedFunctions()
		spawn.HandleChild()
		return
	}

	fmt.Println("=== GoStage Full Feature Demo ===")
	fmt.Println()

	registerAllTasks()
	registerNamedFunctions()

	ctx := context.Background()

	// ═══════════════════════════════════════════════════════════════════
	// PART 1: All 10 Step Kinds
	// ═══════════════════════════════════════════════════════════════════

	demoStep(ctx)             // 1.  StepSingle
	demoParallel(ctx)         // 2.  StepParallel (tasks + sub-workflows)
	demoBranch(ctx)           // 3.  StepBranch (tasks + sub-workflows)
	demoForEach(ctx)          // 4.  StepForEach (tasks, sub-workflows, spawn)
	demoMap(ctx)              // 5.  StepMap (closure + named)
	demoDoUntil(ctx)          // 6.  StepDoUntil (closure, named, sub-workflow)
	demoDoWhile(ctx)          // 7.  StepDoWhile (closure, named)
	demoSubWorkflow(ctx)      // 8.  StepSub (single, nested)
	demoSleep(ctx)            // 9.  StepSleep
	demoStage(ctx)            // 10. StepStage (tasks + sub-workflows)

	// ═══════════════════════════════════════════════════════════════════
	// PART 2: Control Flow
	// ═══════════════════════════════════════════════════════════════════

	demoBail(ctx)             // 11. Bail (early exit)
	demoSuspendResume(ctx)    // 12. Suspend + Resume
	demoCancellation(ctx)     // 13. ctx.Context() cancellation check

	// ═══════════════════════════════════════════════════════════════════
	// PART 3: Dynamic Mutations
	// ═══════════════════════════════════════════════════════════════════

	demoInsertAfter(ctx)      // 14. InsertAfter
	demoDisableEnable(ctx)    // 15. DisableStep + EnableStep
	demoTagMutations(ctx)     // 16. FindStepsByTag + DisableByTag + EnableByTag

	// ═══════════════════════════════════════════════════════════════════
	// PART 4: Tags & IPC
	// ═══════════════════════════════════════════════════════════════════

	demoTags(ctx)             // 17. Step tags, task tags, ListTasksByTag
	demoIPC(ctx)              // 18. Send + OnMessage + wildcard handler

	// ═══════════════════════════════════════════════════════════════════
	// PART 5: Middleware & Plugins
	// ═══════════════════════════════════════════════════════════════════

	demoAllMiddleware(ctx)    // 19. Engine + Task middleware
	demoPlugin(ctx)           // 20. LoggingPlugin (step + child via Plugin)

	// ═══════════════════════════════════════════════════════════════════
	// PART 6: Engine Features
	// ═══════════════════════════════════════════════════════════════════

	demoAsync(ctx)            // 21. Run + Wait + Cancel
	demoRetry(ctx)            // 22. WithRetry + WithRetryDelay
	demoWorkflowOptions(ctx)  // 23. DefaultRetry + callbacks + tags
	demoEngineOptions(ctx)    // 24. Timeout, WorkerPool, CacheSize
	demoSQLite(ctx)           // 25. SQLite persistence
	demoAutoRecover(ctx)      // 26. WithAutoRecover + explicit Recover

	// ═══════════════════════════════════════════════════════════════════
	// PART 7: Serialization
	// ═══════════════════════════════════════════════════════════════════

	demoSerialization(ctx)    // 27. Full round-trip: all named variants

	// ═══════════════════════════════════════════════════════════════════
	// PART 8: Complex Composition
	// ═══════════════════════════════════════════════════════════════════

	demoOrderPipeline(ctx)    // 28. Realistic order processing

	fmt.Println("\n=== All demos completed ===")
}

// ===========================================================================
// Task & Function Registration
// ===========================================================================

func registerAllTasks() {
	// ── Basic tasks ────────────────────────────────────────────────────
	gs.Task("greet", func(ctx *gs.Ctx) error {
		name := gs.GetOr[string](ctx, "name", "World")
		ctx.Log.Info("Hello, %s!", name)
		if err := gs.Set(ctx, "greeted", true); err != nil {
			return err
		}
		return nil
	}, gs.WithDescription("Greet the user"))

	gs.Task("validate", func(ctx *gs.Ctx) error {
		orderID := gs.Get[string](ctx, "order_id")
		if orderID == "" {
			return fmt.Errorf("missing order_id")
		}
		ctx.Log.Info("Validated order %s", orderID)
		if err := gs.Set(ctx, "validated", true); err != nil {
			return err
		}
		return nil
	})

	gs.Task("charge", func(ctx *gs.Ctx) error {
		amount := gs.GetOr[float64](ctx, "amount", 0)
		ctx.Log.Info("Charged $%.2f", amount)
		if err := gs.Set(ctx, "charged", true); err != nil {
			return err
		}
		return nil
	})

	gs.Task("reserve", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Reserved inventory")
		if err := gs.Set(ctx, "reserved", true); err != nil {
			return err
		}
		return nil
	})

	gs.Task("check.fraud", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Fraud check passed")
		if err := gs.Set(ctx, "fraud_ok", true); err != nil {
			return err
		}
		return nil
	})

	// ── Branch targets ─────────────────────────────────────────────────
	gs.Task("urgent.process", func(ctx *gs.Ctx) error {
		ctx.Log.Info("URGENT path")
		if err := gs.Set(ctx, "route", "urgent"); err != nil {
			return err
		}
		return nil
	})

	gs.Task("normal.process", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Normal path")
		if err := gs.Set(ctx, "route", "normal"); err != nil {
			return err
		}
		return nil
	})

	// ── ForEach targets ────────────────────────────────────────────────
	gs.Task("download.track", func(ctx *gs.Ctx) error {
		track := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		ctx.Log.Info("Download [%d]: %s", idx, track)
		return nil
	})

	gs.Task("process.item", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		ctx.Log.Info("Processing item: %s", item)
		if err := gs.Set(ctx, "last_item", item); err != nil {
			return err
		}
		return nil
	})

	// ── Spawn-proof tasks (use fmt.Printf because child logger is no-op) ─
	gs.Task("spawn.echo", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		idx := gs.ItemIndex(ctx)
		fmt.Printf("  [child] spawn.echo: item[%d]=%s\n", idx, item)
		if err := gs.Set(ctx, fmt.Sprintf("spawned_%d", idx), item); err != nil {
			return err
		}
		// IPC message back to parent via gRPC
		gs.Send(ctx, "spawn.done", gs.Params{"item": item, "index": idx})
		return nil
	})

	gs.Task("spawn.step1", func(ctx *gs.Ctx) error {
		item := gs.Item[string](ctx)
		fmt.Printf("  [child] step1: received item=%s\n", item)
		if err := gs.Set(ctx, "step1_item", item); err != nil {
			return err
		}
		return nil
	})

	gs.Task("spawn.step2", func(ctx *gs.Ctx) error {
		prev := gs.Get[string](ctx, "step1_item")
		fmt.Printf("  [child] step2: step1 passed us %q → enriching\n", prev)
		if err := gs.Set(ctx, "step2_done", true); err != nil {
			return err
		}
		return nil
	})

	// ── Loop tasks ─────────────────────────────────────────────────────
	gs.Task("poll.status", func(ctx *gs.Ctx) error {
		count := gs.GetOr[int](ctx, "poll_count", 0) + 1
		if err := gs.Set(ctx, "poll_count", count); err != nil {
			return err
		}
		if count >= 3 {
			if err := gs.Set(ctx, "status", "ready"); err != nil {
				return err
			}
		}
		ctx.Log.Info("Poll #%d → %s", count, gs.GetOr[string](ctx, "status", "pending"))
		return nil
	})

	gs.Task("fetch.page", func(ctx *gs.Ctx) error {
		page := gs.GetOr[int](ctx, "page", 0) + 1
		if err := gs.Set(ctx, "page", page); err != nil {
			return err
		}
		if err := gs.Set(ctx, "has_more", page < 3); err != nil {
			return err
		}
		ctx.Log.Info("Fetched page %d", page)
		return nil
	})

	// ── Sub-workflow inner tasks ───────────────────────────────────────
	gs.Task("enrich.data", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Enriching data")
		if err := gs.Set(ctx, "enriched", true); err != nil {
			return err
		}
		return nil
	})

	gs.Task("deep.task", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Deep nested task executed")
		if err := gs.Set(ctx, "deep", true); err != nil {
			return err
		}
		return nil
	})

	// ── Bail ───────────────────────────────────────────────────────────
	gs.Task("age.check", func(ctx *gs.Ctx) error {
		age := gs.GetOr[int](ctx, "age", 0)
		if age < 18 {
			return gs.Bail(ctx, fmt.Sprintf("Must be 18+, got %d", age))
		}
		if err := gs.Set(ctx, "age_ok", true); err != nil {
			return err
		}
		return nil
	})

	// ── Suspend / Resume ───────────────────────────────────────────────
	gs.Task("request.approval", func(ctx *gs.Ctx) error {
		if gs.IsResuming(ctx) {
			approved := gs.ResumeData[bool](ctx, "approved")
			ctx.Log.Info("Resumed with approved=%v", approved)
			if err := gs.Set(ctx, "approved", approved); err != nil {
				return err
			}
			return nil
		}
		ctx.Log.Info("Awaiting approval...")
		return gs.Suspend(ctx, gs.Params{"reason": "needs manager approval"})
	})

	// ── Cancellation-aware task ────────────────────────────────────────
	gs.Task("long.running", func(ctx *gs.Ctx) error {
		for i := 0; i < 100; i++ {
			select {
			case <-ctx.Context().Done():
				ctx.Log.Info("Cancelled at iteration %d", i)
				if err := gs.Set(ctx, "cancelled_at", i); err != nil {
					return err
				}
				return ctx.Context().Err()
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}
		if err := gs.Set(ctx, "finished", true); err != nil {
			return err
		}
		return nil
	})

	// ── Mutation tasks ─────────────────────────────────────────────────
	gs.Task("decide.extras", func(ctx *gs.Ctx) error {
		if gs.GetOr[bool](ctx, "add_bonus", false) {
			gs.InsertAfter(ctx, "bonus.step")
			ctx.Log.Info("InsertAfter: bonus.step")
		}
		return nil
	})

	gs.Task("bonus.step", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Bonus step executed!")
		if err := gs.Set(ctx, "bonus_applied", true); err != nil {
			return err
		}
		return nil
	})

	gs.Task("final.step", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Final step")
		return nil
	})

	// Task that disables/enables steps at runtime
	gs.Task("gate.keeper", func(ctx *gs.Ctx) error {
		skipShipping := gs.GetOr[bool](ctx, "skip_shipping", false)
		if skipShipping {
			// Find and disable all steps tagged "shipping"
			ids := gs.FindStepsByTag(ctx, "shipping")
			ctx.Log.Info("Found %d shipping steps, disabling them", len(ids))
			gs.DisableByTag(ctx, "shipping")
		}
		return nil
	})

	gs.Task("step.a", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Step A ran")
		if err := gs.Set(ctx, "a_ran", true); err != nil {
			return err
		}
		return nil
	})

	gs.Task("step.b", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Step B ran")
		if err := gs.Set(ctx, "b_ran", true); err != nil {
			return err
		}
		return nil
	})

	gs.Task("step.c", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Step C ran")
		if err := gs.Set(ctx, "c_ran", true); err != nil {
			return err
		}
		return nil
	})

	gs.Task("disable.b", func(ctx *gs.Ctx) error {
		// Disable step B by its ID (workflow_id:step_index)
		gs.DisableStep(ctx, "disable-demo:1")
		ctx.Log.Info("Disabled step B")
		return nil
	})

	gs.Task("enable.check", func(ctx *gs.Ctx) error {
		// Re-enable it (would take effect on next run, not this one)
		gs.EnableStep(ctx, "disable-demo:1")
		ctx.Log.Info("Re-enabled step B for future runs")
		return nil
	})

	// ── Tag demo tasks ─────────────────────────────────────────────────
	gs.Task("billing.charge", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Billing: charge")
		return nil
	}, gs.WithTags("billing", "critical"))

	gs.Task("billing.receipt", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Billing: receipt")
		return nil
	}, gs.WithTags("billing"))

	gs.Task("shipping.label", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Shipping: label")
		return nil
	}, gs.WithTags("shipping"))

	gs.Task("shipping.dispatch", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Shipping: dispatch")
		return nil
	}, gs.WithTags("shipping"))

	// ── IPC tasks ──────────────────────────────────────────────────────
	gs.Task("report.progress", func(ctx *gs.Ctx) error {
		gs.Send(ctx, "progress", gs.Params{"pct": 50})
		gs.Send(ctx, "progress", gs.Params{"pct": 100})
		gs.Send(ctx, "metric", gs.Params{"name": "items_processed", "value": 42})
		return nil
	})

	// ── Retry demo task ────────────────────────────────────────────────
	retryAttempt := 0
	gs.Task("flaky.service", func(ctx *gs.Ctx) error {
		retryAttempt++
		if retryAttempt < 3 {
			return fmt.Errorf("timeout (attempt %d)", retryAttempt)
		}
		ctx.Log.Info("Connected on attempt %d", retryAttempt)
		if err := gs.Set(ctx, "connected", true); err != nil {
			return err
		}
		return nil
	}, gs.WithRetry(5), gs.WithRetryDelay(10*time.Millisecond))

	gs.Task("after.sleep", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Woke up!")
		return nil
	})

	gs.Task("noop", func(ctx *gs.Ctx) error { return nil })

	// ── Order pipeline tasks ───────────────────────────────────────────
	gs.Task("order.validate", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Validated: %s", gs.Get[string](ctx, "order_id"))
		return nil
	})
	gs.Task("order.charge", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Charged: $%.2f", gs.GetOr[float64](ctx, "total", 0))
		return nil
	})
	gs.Task("order.reserve", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Reserved inventory")
		return nil
	})
	gs.Task("order.express", func(ctx *gs.Ctx) error {
		if err := gs.Set(ctx, "shipping", "express"); err != nil {
			return err
		}
		return nil
	})
	gs.Task("order.standard", func(ctx *gs.Ctx) error {
		if err := gs.Set(ctx, "shipping", "standard"); err != nil {
			return err
		}
		return nil
	})
	gs.Task("order.pack.item", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Packing: %s", gs.Item[string](ctx))
		return nil
	})
	gs.Task("order.confirm", func(ctx *gs.Ctx) error {
		ctx.Log.Info("Order confirmed!")
		if err := gs.Set(ctx, "confirmed", true); err != nil {
			return err
		}
		return nil
	})
}

func registerNamedFunctions() {
	// Named conditions — for serializable Branch + Loop steps
	gs.Condition("is-high-priority", func(ctx *gs.Ctx) bool {
		return gs.Get[string](ctx, "priority") == "high"
	})
	gs.Condition("status-is-ready", func(ctx *gs.Ctx) bool {
		return gs.Get[string](ctx, "status") == "ready"
	})
	gs.Condition("has-more-pages", func(ctx *gs.Ctx) bool {
		return gs.Get[bool](ctx, "has_more")
	})

	// Named map function — for serializable Map steps
	gs.MapFn("normalize-name", func(ctx *gs.Ctx) error {
		raw := gs.GetOr[string](ctx, "name", "")
		_ = gs.Set(ctx, "name", strings.TrimSpace(titleCase(strings.ToLower(raw))))
		return nil
	})
}

// ===========================================================================
// PART 1: All 10 Step Kinds
// ===========================================================================

// 1. StepSingle — basic sequential task execution
func demoStep(ctx context.Context) {
	section("1. Step (sequential tasks)")

	wf, err := gs.NewWorkflow("step-demo").
		Step("greet").
		Step("validate").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"step"}))
	defer engine.Close()

	result, err := engine.RunSync(ctx, wf, gs.Params{"name": "David", "order_id": "ORD-001"})
	printResult(result, err)
}

// 2. StepParallel — concurrent execution with task refs AND sub-workflow refs
func demoParallel(ctx context.Context) {
	section("2. Parallel (tasks + sub-workflow refs)")

	// Sub-workflow used as a parallel branch
	subWf, err := gs.NewWorkflow("fraud-check-wf").
		Step("check.fraud").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	wf, err := gs.NewWorkflow("parallel-demo").
		Parallel(
			gs.Step("charge"),     // task ref
			gs.Step("reserve"),    // task ref
			gs.Sub(subWf),         // sub-workflow ref
		).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"par"}))
	defer engine.Close()

	result, err := engine.RunSync(ctx, wf, gs.Params{"amount": 99.99})
	printResult(result, err)
	fmt.Printf("  Charged=%v Reserved=%v FraudOK=%v\n",
		result.Store["charged"], result.Store["reserved"], result.Store["fraud_ok"])
}

// 3. StepBranch — conditional routing with task refs AND sub-workflow refs
func demoBranch(ctx context.Context) {
	section("3. Branch (tasks + sub-workflow refs)")

	urgentWf, err := gs.NewWorkflow("urgent-wf").
		Step("urgent.process").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	wf, err := gs.NewWorkflow("branch-demo").
		Branch(
			gs.When(func(ctx *gs.Ctx) bool {
				return gs.Get[string](ctx, "priority") == "high"
			}).Sub(urgentWf),                      // sub-workflow ref
			gs.Default().Step("normal.process"),   // task ref
		).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"br"}))
	defer engine.Close()

	fmt.Println("  → priority=high (routes to sub-workflow):")
	r1, _ := engine.RunSync(ctx, wf, gs.Params{"priority": "high"})
	fmt.Printf("    Route: %s\n", r1.Store["route"])

	fmt.Println("  → priority=low (routes to task):")
	r2, _ := engine.RunSync(ctx, wf, gs.Params{"priority": "low"})
	fmt.Printf("    Route: %s\n", r2.Store["route"])
}

// 4. StepForEach — iteration with tasks, sub-workflows, and spawn
func demoForEach(ctx context.Context) {
	section("4a. ForEach (task ref + concurrency)")

	wf1, err := gs.NewWorkflow("foreach-task").
		ForEach("tracks", gs.Step("download.track"), gs.WithConcurrency(2)).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"each"}), spawn.WithSpawn())
	defer engine.Close()

	r1, _ := engine.RunSync(ctx, wf1, gs.Params{
		"tracks": []any{"Bohemian Rhapsody", "Stairway to Heaven", "Hotel California"},
	})
	printResult(r1, nil)

	section("4b. ForEach (sub-workflow ref)")

	// Each iteration runs a full sub-workflow instead of a single task
	itemWf, err := gs.NewWorkflow("item-pipeline").
		Step("process.item").
		Step("noop").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	wf2, err := gs.NewWorkflow("foreach-sub").
		ForEach("items", gs.Sub(itemWf), gs.WithConcurrency(2)).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	r2, _ := engine.RunSync(ctx, wf2, gs.Params{
		"items": []any{"Widget", "Gadget"},
	})
	printResult(r2, nil)

	section("4c. ForEach (WithSpawn — child process isolation)")

	// WithSpawn() spawns each iteration as an isolated child process.
	// Each item runs in a separate OS process communicating via gRPC.
	// Proof: child output via fmt.Printf, store values merged back, IPC messages.
	wf3, err := gs.NewWorkflow("foreach-spawn").
		ForEach("items", gs.Step("spawn.echo"), gs.WithConcurrency(1), gs.WithSpawn()).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	// Register IPC handler — child sends "spawn.done" messages via gRPC
	engine.OnMessage("spawn.done", func(msgType string, payload map[string]any) {
		fmt.Printf("  [parent←child IPC] item=%v completed\n", payload["item"])
	})

	r3, err := engine.RunSync(ctx, wf3, gs.Params{
		"items": []any{"Alpha", "Beta"},
	})
	if err != nil {
		fmt.Printf("  Spawn error: %v (expected if using 'go run')\n", err)
	} else {
		printResult(r3, nil)
		fmt.Printf("  Store round-trip: spawned_0=%v, spawned_1=%v\n",
			r3.Store["spawned_0"], r3.Store["spawned_1"])
	}

	section("4d. ForEach (sub-workflow + WithSpawn)")

	// Each child process runs a FULL sub-workflow (2 steps) — not just one task.
	// step1 reads the ForEach item, step2 reads step1's output.
	// Proves: sub-workflow execution + data flow between steps inside child.
	spawnSubWf, err := gs.NewWorkflow("spawn-sub-pipeline").
		Step("spawn.step1").
		Step("spawn.step2").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	wf4, err := gs.NewWorkflow("foreach-sub-spawn").
		ForEach("items", gs.Sub(spawnSubWf), gs.WithConcurrency(1), gs.WithSpawn()).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	r4, err := engine.RunSync(ctx, wf4, gs.Params{
		"items": []any{"Gamma", "Delta"},
	})
	if err != nil {
		fmt.Printf("  Spawn+Sub error: %v (expected if using 'go run')\n", err)
	} else {
		printResult(r4, nil)
		fmt.Printf("  Store round-trip: step1_item=%v, step2_done=%v\n",
			r4.Store["step1_item"], r4.Store["step2_done"])
	}
}

// 5. StepMap — inline data transformation (closure + named)
func demoMap(ctx context.Context) {
	section("5a. Map (inline closure)")

	wf1, err := gs.NewWorkflow("map-closure").
		Map(func(ctx *gs.Ctx) error {
			prices := gs.GetOr[[]any](ctx, "prices", nil)
			total := 0.0
			for _, p := range prices {
				if v, ok := p.(float64); ok {
					total += v
				}
			}
			_ = gs.Set(ctx, "total", total)
			return nil
		}).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"map"}))
	defer engine.Close()

	r1, _ := engine.RunSync(ctx, wf1, gs.Params{"prices": []any{19.99, 29.99, 9.99}})
	fmt.Printf("  Total: $%.2f\n", r1.Store["total"])

	section("5b. MapNamed (registered function — serializable)")

	wf2, err := gs.NewWorkflow("map-named").
		MapNamed("normalize-name").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	r2, _ := engine.RunSync(ctx, wf2, gs.Params{"name": "  jOHN dOE  "})
	fmt.Printf("  Normalized: %q\n", r2.Store["name"])
}

// 6. StepDoUntil — repeat-until loop (closure, named, sub-workflow ref)
func demoDoUntil(ctx context.Context) {
	section("6a. DoUntil (closure condition)")

	wf1, err := gs.NewWorkflow("until-closure").
		DoUntil(gs.Step("poll.status"), func(ctx *gs.Ctx) bool {
			return gs.Get[string](ctx, "status") == "ready"
		}).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"until"}))
	defer engine.Close()

	r1, _ := engine.RunSync(ctx, wf1, nil)
	fmt.Printf("  Polls: %d\n", r1.Store["poll_count"])

	section("6b. DoUntilNamed (registered condition — serializable)")

	wf2, err := gs.NewWorkflow("until-named").
		DoUntilNamed(gs.Step("poll.status"), "status-is-ready").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	r2, _ := engine.RunSync(ctx, wf2, nil)
	fmt.Printf("  Polls: %d\n", r2.Store["poll_count"])

	section("6c. DoUntil (sub-workflow ref)")

	// Loop body is a full sub-workflow instead of a single task
	loopBody, err := gs.NewWorkflow("poll-wf").
		Step("poll.status").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	wf3, err := gs.NewWorkflow("until-sub").
		DoUntil(gs.Sub(loopBody), func(ctx *gs.Ctx) bool {
			return gs.Get[string](ctx, "status") == "ready"
		}).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	r3, _ := engine.RunSync(ctx, wf3, nil)
	fmt.Printf("  Polls (via sub-wf): %d\n", r3.Store["poll_count"])
}

// 7. StepDoWhile — while-do loop (closure + named)
func demoDoWhile(ctx context.Context) {
	section("7a. DoWhile (closure condition)")

	wf1, err := gs.NewWorkflow("while-closure").
		Map(func(ctx *gs.Ctx) error { _ = gs.Set(ctx, "has_more", true); return nil }).
		DoWhile(gs.Step("fetch.page"), func(ctx *gs.Ctx) bool {
			return gs.Get[bool](ctx, "has_more")
		}).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"while"}))
	defer engine.Close()

	r1, _ := engine.RunSync(ctx, wf1, nil)
	fmt.Printf("  Pages: %d\n", r1.Store["page"])

	section("7b. DoWhileNamed (registered condition — serializable)")

	wf2, err := gs.NewWorkflow("while-named").
		Map(func(ctx *gs.Ctx) error { _ = gs.Set(ctx, "has_more", true); return nil }).
		DoWhileNamed(gs.Step("fetch.page"), "has-more-pages").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	r2, _ := engine.RunSync(ctx, wf2, nil)
	fmt.Printf("  Pages: %d\n", r2.Store["page"])
}

// 8. StepSub — nested sub-workflow (single level + deep nesting)
func demoSubWorkflow(ctx context.Context) {
	section("8a. Sub (single-level nesting)")

	inner, err := gs.NewWorkflow("enrichment").
		Step("enrich.data").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	outer, err := gs.NewWorkflow("pipeline-single").
		Step("validate").
		Sub(inner).
		Step("greet").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"sub"}))
	defer engine.Close()

	r1, _ := engine.RunSync(ctx, outer, gs.Params{"order_id": "ORD-SUB", "name": "Alice"})
	printResult(r1, nil)
	fmt.Printf("  Enriched: %v\n", r1.Store["enriched"])

	section("8b. Sub (deep nesting — sub within sub)")

	deepest, err := gs.NewWorkflow("deep-inner").
		Step("deep.task").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	middle, err := gs.NewWorkflow("middle").
		Step("enrich.data").
		Sub(deepest).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	outerDeep, err := gs.NewWorkflow("pipeline-deep").
		Sub(middle).
		Step("greet").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	r2, _ := engine.RunSync(ctx, outerDeep, gs.Params{"name": "Nested"})
	printResult(r2, nil)
	fmt.Printf("  Enriched=%v Deep=%v\n", r2.Store["enriched"], r2.Store["deep"])
}

// 9. StepSleep — timed delay
func demoSleep(ctx context.Context) {
	section("9. Sleep")

	wf, err := gs.NewWorkflow("sleep-demo").
		Step("greet").
		Sleep(50 * time.Millisecond).
		Step("after.sleep").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"slp"}))
	defer engine.Close()

	start := time.Now()
	result, _ := engine.RunSync(ctx, wf, gs.Params{"name": "Sleeper"})
	printResult(result, nil)
	fmt.Printf("  Elapsed: %s\n", time.Since(start).Round(time.Millisecond))
}

// 10. StepStage — named groups with task refs AND sub-workflow refs
func demoStage(ctx context.Context) {
	section("10. Stage (tasks + sub-workflow refs)")

	fraudWf, err := gs.NewWorkflow("fraud-wf").
		Step("check.fraud").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	wf, err := gs.NewWorkflow("stage-demo").
		Stage("validation",
			gs.Step("validate"),   // task ref
			gs.Sub(fraudWf),       // sub-workflow ref
		).
		Stage("payment",
			gs.Step("charge"),
			gs.Step("reserve"),
		).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"stg"}))
	defer engine.Close()

	result, _ := engine.RunSync(ctx, wf, gs.Params{"order_id": "ORD-STG", "amount": 49.99})
	printResult(result, nil)
}

// ===========================================================================
// PART 2: Control Flow
// ===========================================================================

// 11. Bail — early exit (not an error)
func demoBail(ctx context.Context) {
	section("11. Bail (early exit)")

	wf, err := gs.NewWorkflow("bail-demo").
		Step("age.check").
		Step("greet"). // should not execute
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"bail"}))
	defer engine.Close()

	result, _ := engine.RunSync(ctx, wf, gs.Params{"age": 15})
	fmt.Printf("  Status: %s, Reason: %s\n", result.Status, result.BailReason)
	fmt.Printf("  Greeted: %v (should be <nil>)\n", result.Store["greeted"])
}

// 12. Suspend + Resume
func demoSuspendResume(ctx context.Context) {
	section("12. Suspend / Resume")

	wf, err := gs.NewWorkflow("suspend-demo").
		Step("request.approval").
		Step("greet").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(
		gs.WithLogger(&consoleLogger{"sus"}),
		gs.WithSQLite(":memory:"),
	)
	defer engine.Close()

	r1, _ := engine.RunSync(ctx, wf, gs.Params{"name": "Bob"})
	fmt.Printf("  First run: status=%s, suspend_data=%v\n", r1.Status, r1.SuspendData)

	r2, err := engine.Resume(ctx, r1.RunID, gs.Params{"approved": true})
	if err != nil {
		fmt.Printf("  Resume error: %v\n", err)
	} else {
		fmt.Printf("  Resumed: status=%s, approved=%v\n", r2.Status, r2.Store["approved"])
	}
}

// 13. ctx.Context() — cancellation awareness inside tasks
func demoCancellation(ctx context.Context) {
	section("13. ctx.Context() cancellation")

	wf, err := gs.NewWorkflow("cancel-ctx-demo").
		Step("long.running").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"cancel"}))
	defer engine.Close()

	// Create a context that cancels after 50ms
	cancelCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	result, _ := engine.RunSync(cancelCtx, wf, nil)
	fmt.Printf("  Status: %s\n", result.Status)
	if v := result.Store["cancelled_at"]; v != nil {
		fmt.Printf("  Task detected cancellation at iteration %v\n", v)
	}
}

// ===========================================================================
// PART 3: Dynamic Mutations
// ===========================================================================

// 14. InsertAfter — dynamically add steps
func demoInsertAfter(ctx context.Context) {
	section("14. InsertAfter (dynamic step injection)")

	wf, err := gs.NewWorkflow("insert-demo").
		Step("decide.extras").
		Step("final.step").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"ins"}))
	defer engine.Close()

	result, _ := engine.RunSync(ctx, wf, gs.Params{"add_bonus": true})
	printResult(result, nil)
	fmt.Printf("  Bonus applied: %v\n", result.Store["bonus_applied"])
}

// 15. DisableStep + EnableStep — disable/enable steps by ID at runtime
func demoDisableEnable(ctx context.Context) {
	section("15. DisableStep + EnableStep")

	wf, err := gs.NewWorkflow("disable-demo").
		Step("disable.b").   // step 0: disables step B
		Step("step.b").      // step 1: will be disabled
		Step("enable.check"). // step 2: re-enables for future
		Step("step.c").      // step 3: runs normally
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"dis"}))
	defer engine.Close()

	result, _ := engine.RunSync(ctx, wf, nil)
	printResult(result, nil)
	fmt.Printf("  B ran: %v (should be <nil>), C ran: %v\n",
		result.Store["b_ran"], result.Store["c_ran"])
}

// 16. FindStepsByTag + DisableByTag + EnableByTag
func demoTagMutations(ctx context.Context) {
	section("16. Tag Mutations (FindStepsByTag + DisableByTag)")

	wf, err := gs.NewWorkflow("tag-mut-demo").
		Step("gate.keeper").
		Step("billing.charge", gs.WithStepTags("billing")).
		Step("shipping.label", gs.WithStepTags("shipping")).
		Step("shipping.dispatch", gs.WithStepTags("shipping")).
		Step("final.step").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"tmut"}))
	defer engine.Close()

	fmt.Println("  → skip_shipping=true:")
	r1, _ := engine.RunSync(ctx, wf, gs.Params{"skip_shipping": true})
	printResult(r1, nil)

	fmt.Println("  → skip_shipping=false:")
	r2, _ := engine.RunSync(ctx, wf, gs.Params{"skip_shipping": false})
	printResult(r2, nil)
}

// ===========================================================================
// PART 4: Tags & IPC
// ===========================================================================

// 17. Tags — step tags, task tags, ListTasksByTag
func demoTags(ctx context.Context) {
	section("17. Tags (step + task tags)")

	wf, err := gs.NewWorkflow("tags-demo").
		Step("billing.charge", gs.WithStepTags("billing", "critical")).
		Step("billing.receipt", gs.WithStepTags("billing")).
		Step("shipping.label", gs.WithStepTags("shipping")).
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"tag"}))
	defer engine.Close()

	result, _ := engine.RunSync(ctx, wf, nil)
	printResult(result, nil)

	// Query the task registry by tag
	fmt.Printf("  Tasks tagged 'billing': %v\n", gs.ListTasksByTag("billing"))
	fmt.Printf("  Tasks tagged 'shipping': %v\n", gs.ListTasksByTag("shipping"))
}

// 18. IPC — Send + OnMessage + wildcard handler
func demoIPC(ctx context.Context) {
	section("18. IPC (Send + OnMessage + wildcard)")

	wf, err := gs.NewWorkflow("ipc-demo").
		Step("report.progress").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"ipc"}))
	defer engine.Close()

	// Specific handler
	engine.OnMessage("progress", func(msgType string, payload map[string]any) {
		fmt.Printf("  [progress] %v%%\n", payload["pct"])
	})

	// Wildcard handler — catches ALL message types
	engine.OnMessage("*", func(msgType string, payload map[string]any) {
		fmt.Printf("  [wildcard] type=%s payload=%v\n", msgType, payload)
	})

	result, _ := engine.RunSync(ctx, wf, nil)
	printResult(result, nil)
}

// ===========================================================================
// PART 5: Middleware & Plugins
// ===========================================================================

// 19. Middleware — engine-level and task-level (externally usable)
// Note: StepMiddleware and ChildMiddleware use unexported types (*step, *spawnJob),
// so they can only be used via Plugin implementations (see demo 21).
func demoAllMiddleware(ctx context.Context) {
	section("19. Middleware (engine + task)")

	engineMW := func(ctx context.Context, wf *gs.Workflow, runID gs.RunID, next func() error) error {
		fmt.Printf("  [engine-mw] %s start\n", wf.ID)
		err := next()
		fmt.Printf("  [engine-mw] %s end (err=%v)\n", wf.ID, err)
		return err
	}

	taskMW := func(tctx *gs.Ctx, taskName string, next func() error) error {
		start := time.Now()
		err := next()
		fmt.Printf("  [task-mw] %s took %s\n", taskName, time.Since(start).Round(time.Microsecond))
		return err
	}

	wf, err := gs.NewWorkflow("mw-demo").
		Step("greet").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(
		gs.WithLogger(&consoleLogger{"mw"}),
		gs.WithEngineMiddleware(engineMW),
		gs.WithTaskMiddleware(taskMW),
	)
	defer engine.Close()

	result, _ := engine.RunSync(ctx, wf, gs.Params{"name": "Middleware"})
	printResult(result, nil)
}

// 20. Plugin system — LoggingPlugin (provides step + child middleware via Plugin interface)
func demoPlugin(ctx context.Context) {
	section("20. LoggingPlugin")

	wf, err := gs.NewWorkflow("plugin-demo").
		Step("greet").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(
		gs.WithPlugin(gs.LoggingPlugin(&consoleLogger{"plugin"})),
	)
	defer engine.Close()

	result, _ := engine.RunSync(ctx, wf, gs.Params{"name": "Plugin"})
	printResult(result, nil)
}

// ===========================================================================
// PART 6: Engine Features
// ===========================================================================

// 21. Async — Run + Wait + Cancel
func demoAsync(ctx context.Context) {
	section("21. Async (Run + Wait + Cancel)")

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"async"}))
	defer engine.Close()

	// Run async and wait
	wf, err := gs.NewWorkflow("async-demo").Step("greet").Commit()
	if err != nil {
		log.Fatal(err)
	}
	runID, _ := engine.Run(ctx, wf, gs.Params{"name": "Async"})
	fmt.Printf("  Started: %s\n", runID)

	result, _ := engine.Wait(ctx, runID)
	printResult(result, nil)

	// Cancel demo
	slowWf, err := gs.NewWorkflow("slow").Sleep(10 * time.Second).Commit()
	if err != nil {
		log.Fatal(err)
	}
	slowID, _ := engine.Run(ctx, slowWf, nil)
	time.Sleep(10 * time.Millisecond)
	engine.Cancel(ctx, slowID)
	fmt.Println("  Cancelled slow run")
}

// 22. Retry — WithRetry + WithRetryDelay
func demoRetry(ctx context.Context) {
	section("22. Retry (WithRetry + WithRetryDelay)")

	wf, err := gs.NewWorkflow("retry-demo").Step("flaky.service").Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"retry"}))
	defer engine.Close()

	result, _ := engine.RunSync(ctx, wf, nil)
	printResult(result, nil)
	fmt.Printf("  Connected: %v\n", result.Store["connected"])
}

// 23. Workflow options — DefaultRetry, callbacks, tags
func demoWorkflowOptions(ctx context.Context) {
	section("23. Workflow Options (DefaultRetry + callbacks + tags)")

	var completed []string

	wf, err := gs.NewWorkflow("opts-demo",
		gs.WithDefaultRetry(3, 10*time.Millisecond),
		gs.OnStepComplete(func(step string, ctx *gs.Ctx) {
			completed = append(completed, step)
		}),
		gs.OnError(func(err error) {
			fmt.Printf("  [on-error] %v\n", err)
		}),
		gs.WithWorkflowTags("demo", "featured"),
	).
		Step("validate").
		Step("greet").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"opts"}))
	defer engine.Close()

	result, _ := engine.RunSync(ctx, wf, gs.Params{"order_id": "ORD-OPT", "name": "Callbacks"})
	printResult(result, nil)
	fmt.Printf("  Completed (callback): %v\n", completed)
	fmt.Printf("  Workflow tags: %v\n", wf.Tags)
}

// 24. Engine options — Timeout, WorkerPoolSize, CacheSize
func demoEngineOptions(ctx context.Context) {
	section("24. Engine Options (Timeout + WorkerPool + CacheSize)")

	engine, err := gs.New(
		gs.WithLogger(&consoleLogger{"eng"}),
		gs.WithTimeout(5*time.Second),       // global timeout
		gs.WithWorkerPoolSize(4),            // 4 worker goroutines
		gs.WithCacheSize(100),               // cache up to 100 workflows
	)
	if err != nil {
		log.Fatalf("Engine: %v", err)
	}
	defer engine.Close()

	wf, err := gs.NewWorkflow("engine-opts").Step("greet").Commit()
	if err != nil {
		log.Fatal(err)
	}
	result, _ := engine.RunSync(ctx, wf, gs.Params{"name": "EngineOpts"})
	printResult(result, nil)
}

// 25. SQLite persistence
func demoSQLite(ctx context.Context) {
	section("25. SQLite Persistence")

	engine, _ := gs.New(
		gs.WithSQLite(":memory:"),
		gs.WithLogger(&consoleLogger{"sql"}),
	)
	defer engine.Close()

	wf, err := gs.NewWorkflow("sqlite-demo").Step("greet").Commit()
	if err != nil {
		log.Fatal(err)
	}
	result, _ := engine.RunSync(ctx, wf, gs.Params{"name": "SQLite"})
	printResult(result, nil)
	fmt.Printf("  Persisted run: %s\n", result.RunID)
}

// 26. AutoRecover + explicit Recover
func demoAutoRecover(ctx context.Context) {
	section("26. AutoRecover + explicit Recover")

	// Engine with auto-recover (scans persistence on startup for interrupted runs)
	engine, err := gs.New(
		gs.WithSQLite(":memory:"),
		gs.WithAutoRecover(),
		gs.WithLogger(&consoleLogger{"rec"}),
	)
	if err != nil {
		log.Fatalf("AutoRecover engine: %v", err)
	}
	defer engine.Close()

	wf, err := gs.NewWorkflow("recover-demo").Step("greet").Commit()
	if err != nil {
		log.Fatal(err)
	}
	result, _ := engine.RunSync(ctx, wf, gs.Params{"name": "Recover"})
	printResult(result, nil)

	// Explicit Recover call (normally used after restart)
	if err := engine.Recover(ctx); err != nil {
		fmt.Printf("  Recover error: %v\n", err)
	} else {
		fmt.Println("  Explicit Recover: ok (no interrupted runs)")
	}
}

// ===========================================================================
// PART 7: Serialization
// ===========================================================================

// 27. Full round-trip serialization with all named variants
func demoSerialization(ctx context.Context) {
	section("27. Serialization (full round-trip)")

	// Build a serializable workflow using only named functions
	wf, err := gs.NewWorkflow("serializable").
		Step("validate").
		MapNamed("normalize-name").
		Branch(
			gs.WhenNamed("is-high-priority").Step("urgent.process"),
			gs.Default().Step("normal.process"),
		).
		DoUntilNamed(gs.Step("poll.status"), "status-is-ready").
		DoWhileNamed(gs.Step("fetch.page"), "has-more-pages").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	// Serialize → JSON
	def, _ := gs.WorkflowToDefinition(wf)
	jsonBytes, _ := gs.MarshalWorkflowDefinition(def)

	var pretty json.RawMessage = jsonBytes
	prettyBytes, _ := json.MarshalIndent(pretty, "  ", "  ")
	fmt.Printf("  JSON:\n  %s\n", string(prettyBytes))

	// Deserialize → rebuild → execute
	def2, _ := gs.UnmarshalWorkflowDefinition(jsonBytes)
	wf2, err := gs.NewWorkflowFromDef(def2)
	if err != nil {
		log.Fatalf("FromDef: %v", err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"ser"}))
	defer engine.Close()

	result, _ := engine.RunSync(ctx, wf2, gs.Params{
		"order_id": "ORD-SER",
		"name":     "  john doe  ",
		"priority": "low",
		"has_more": true,
	})
	printResult(result, nil)
	fmt.Printf("  Normalized: %q, Pages: %v, Polls: %v\n",
		result.Store["name"], result.Store["page"], result.Store["poll_count"])
}

// ===========================================================================
// PART 8: Complex Composition
// ===========================================================================

// 28. Realistic order processing pipeline
func demoOrderPipeline(ctx context.Context) {
	section("28. Order Pipeline (all step kinds combined)")

	wf, err := gs.NewWorkflow("order-pipeline",
		gs.WithDefaultRetry(2, 50*time.Millisecond),
		gs.OnStepComplete(func(step string, ctx *gs.Ctx) {
			fmt.Printf("  [done] %s\n", step)
		}),
	).
		// Stage 1: Validation
		Stage("validation", gs.Step("order.validate")).
		// Stage 2: Payment + Inventory in parallel
		Parallel(gs.Step("order.charge"), gs.Step("order.reserve")).
		// Stage 3: Shipping route
		Branch(
			gs.When(func(ctx *gs.Ctx) bool {
				return gs.Get[bool](ctx, "express")
			}).Step("order.express"),
			gs.Default().Step("order.standard"),
		).
		// Stage 4: Pack items concurrently
		ForEach("items", gs.Step("order.pack.item"), gs.WithConcurrency(3)).
		// Stage 5: Confirm
		Step("order.confirm").
		Commit()
	if err != nil {
		log.Fatal(err)
	}

	engine, _ := gs.New(gs.WithLogger(&consoleLogger{"pipe"}))
	defer engine.Close()

	result, _ := engine.RunSync(ctx, wf, gs.Params{
		"order_id": fmt.Sprintf("ORD-%04d", rand.Intn(10000)),
		"total":    149.97,
		"express":  true,
		"items":    []any{"Widget A", "Gadget B", "Doohickey C"},
	})
	printResult(result, nil)
	fmt.Printf("  Shipping: %s, Confirmed: %v\n",
		result.Store["shipping"], result.Store["confirmed"])
}

// ===========================================================================
// Helpers
// ===========================================================================

func titleCase(s string) string {
	words := strings.Fields(s)
	for i, w := range words {
		if len(w) > 0 {
			words[i] = strings.ToUpper(w[:1]) + w[1:]
		}
	}
	return strings.Join(words, " ")
}

func section(title string) {
	fmt.Printf("\n── %s ──\n", title)
}

func printResult(r *gs.Result, err error) {
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
		return
	}
	if r == nil {
		fmt.Println("  Result: nil")
		return
	}
	fmt.Printf("  Status: %s\n", r.Status)
}
