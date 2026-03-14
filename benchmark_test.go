package gostage

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// === Workflow Throughput ===

func BenchmarkRunSync_InMemory_SingleStep(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.noop", func(ctx *Ctx) error { return nil })
	wf, _ := NewWorkflow("bench-single").Step("bench.noop").Commit()
	engine, _ := New()
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := engine.RunSync(context.Background(), wf, nil)
		if result.Status != Completed {
			b.Fatal("not completed")
		}
	}
}

func BenchmarkRunSync_InMemory_ThreeSteps(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.a", func(ctx *Ctx) error { Set(ctx, "a", 1); return nil })
	Task("bench.b", func(ctx *Ctx) error { Set(ctx, "b", Get[int](ctx, "a")+1); return nil })
	Task("bench.c", func(ctx *Ctx) error { Set(ctx, "c", Get[int](ctx, "b")+1); return nil })
	wf, _ := NewWorkflow("bench-three").Step("bench.a").Step("bench.b").Step("bench.c").Commit()
	engine, _ := New()
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := engine.RunSync(context.Background(), wf, nil)
		if result.Status != Completed {
			b.Fatal("not completed")
		}
	}
}

func BenchmarkRunSync_SQLite_SingleStep(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.sql.noop", func(ctx *Ctx) error { return nil })
	wf, _ := NewWorkflow("bench-sql").Step("bench.sql.noop").Commit()
	dbPath := filepath.Join(b.TempDir(), "bench.db")
	engine, _ := New(WithSQLite(dbPath))
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := engine.RunSync(context.Background(), wf, nil)
		if result.Status != Completed {
			b.Fatal("not completed")
		}
	}
}

func BenchmarkRunSync_SQLite_ThreeSteps(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.sql.a", func(ctx *Ctx) error { Set(ctx, "a", 1); return nil })
	Task("bench.sql.b", func(ctx *Ctx) error { Set(ctx, "b", 2); return nil })
	Task("bench.sql.c", func(ctx *Ctx) error { Set(ctx, "c", 3); return nil })
	wf, _ := NewWorkflow("bench-sql3").Step("bench.sql.a").Step("bench.sql.b").Step("bench.sql.c").Commit()
	dbPath := filepath.Join(b.TempDir(), "bench3.db")
	engine, _ := New(WithSQLite(dbPath))
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := engine.RunSync(context.Background(), wf, nil)
		if result.Status != Completed {
			b.Fatal("not completed")
		}
	}
}

// === State Operations ===

func BenchmarkState_Set(b *testing.B) {
	s := newRunState("bench", nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(fmt.Sprintf("key_%d", i%100), i)
	}
}

func BenchmarkState_Get(b *testing.B) {
	s := newRunState("bench", nil)
	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("key_%d", i), i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(fmt.Sprintf("key_%d", i%100))
	}
}

func BenchmarkState_SetBatch(b *testing.B) {
	s := newRunState("bench", nil)
	batch := make(map[string]any, 10)
	for i := 0; i < 10; i++ {
		batch[fmt.Sprintf("key_%d", i)] = i
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SetBatch(batch)
	}
}

func BenchmarkState_FlushInMemory(b *testing.B) {
	s := newRunState("bench", newMemoryPersistence())
	for i := 0; i < 10; i++ {
		s.Set(fmt.Sprintf("key_%d", i), i)
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Re-dirty the entries for each flush
		for j := 0; j < 10; j++ {
			s.Set(fmt.Sprintf("key_%d", j), i+j)
		}
		s.Flush(ctx)
	}
}

func BenchmarkState_FlushSQLite(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench-flush.db")
	p, _ := newSQLitePersistence(dbPath)
	defer p.Close()
	s := newRunState("bench-run", p)
	ctx := context.Background()

	// Initial save so the run exists
	p.SaveRun(ctx, &RunState{
		RunID:      "bench-run",
		WorkflowID: "bench-wf",
		Status:     Running,
		StepStates: make(map[string]Status),
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			s.Set(fmt.Sprintf("key_%d", j), i+j)
		}
		s.Flush(ctx)
	}
}

// === Parallel Scaling ===

func benchmarkForEachConcurrency(b *testing.B, concurrency int) {
	ResetTaskRegistry()
	Task("bench.fe.item", func(ctx *Ctx) error {
		idx := ItemIndex(ctx)
		Set(ctx, fmt.Sprintf("r_%d", idx), idx)
		return nil
	})
	wf, _ := NewWorkflow("bench-fe").
		ForEach("items", Step("bench.fe.item"), WithConcurrency(concurrency)).
		Commit()
	engine, _ := New()
	defer engine.Close()

	items := make([]int, 20)
	for i := range items {
		items[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := engine.RunSync(context.Background(), wf, Params{"items": items})
		if result.Status != Completed {
			b.Fatal("not completed")
		}
	}
}

func BenchmarkForEach_Sequential(b *testing.B)    { benchmarkForEachConcurrency(b, 1) }
func BenchmarkForEach_Concurrency4(b *testing.B)  { benchmarkForEachConcurrency(b, 4) }
func BenchmarkForEach_Concurrency8(b *testing.B)  { benchmarkForEachConcurrency(b, 8) }
func BenchmarkForEach_Concurrency20(b *testing.B) { benchmarkForEachConcurrency(b, 20) }

// === Middleware Overhead ===

func BenchmarkRunSync_NoMiddleware(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.mw.noop", func(ctx *Ctx) error { return nil })
	wf, _ := NewWorkflow("bench-nomw").Step("bench.mw.noop").Commit()
	engine, _ := New()
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.RunSync(context.Background(), wf, nil)
	}
}

func BenchmarkRunSync_WithStepMiddleware(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.mw.step", func(ctx *Ctx) error { return nil })
	wf, _ := NewWorkflow("bench-stepmw").Step("bench.mw.step").Commit()
	engine, _ := New(
		WithStepMiddleware(func(ctx context.Context, info StepInfo, runID RunID, next func() error) error {
			return next()
		}),
	)
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.RunSync(context.Background(), wf, nil)
	}
}

func BenchmarkRunSync_WithTaskMiddleware(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.mw.task", func(ctx *Ctx) error { return nil })
	wf, _ := NewWorkflow("bench-taskmw").Step("bench.mw.task").Commit()
	engine, _ := New(
		WithTaskMiddleware(func(tctx *Ctx, taskName string, next func() error) error {
			return next()
		}),
	)
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.RunSync(context.Background(), wf, nil)
	}
}

func BenchmarkRunSync_WithBothMiddleware(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.mw.both", func(ctx *Ctx) error { return nil })
	wf, _ := NewWorkflow("bench-bothmw").Step("bench.mw.both").Commit()
	engine, _ := New(
		WithStepMiddleware(func(ctx context.Context, info StepInfo, runID RunID, next func() error) error {
			return next()
		}),
		WithTaskMiddleware(func(tctx *Ctx, taskName string, next func() error) error {
			return next()
		}),
	)
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.RunSync(context.Background(), wf, nil)
	}
}

// === Serialization Round-Trip ===

func BenchmarkWorkflowToDefinition(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.ser.a", func(ctx *Ctx) error { return nil })
	Task("bench.ser.b", func(ctx *Ctx) error { return nil })
	Task("bench.ser.c", func(ctx *Ctx) error { return nil })
	Condition("bench.ser.cond", func(ctx *Ctx) bool { return true })
	MapFn("bench.ser.map", func(ctx *Ctx) error { return nil })

	wf, _ := NewWorkflow("bench-ser").
		Step("bench.ser.a").
		Parallel(Step("bench.ser.b"), Step("bench.ser.c")).
		Branch(WhenNamed("bench.ser.cond").Step("bench.ser.a"), Default().Step("bench.ser.b")).
		MapNamed("bench.ser.map").
		ForEach("items", Step("bench.ser.a")).
		Commit()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := WorkflowToDefinition(wf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalUnmarshalDefinition(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.mu.a", func(ctx *Ctx) error { return nil })
	Task("bench.mu.b", func(ctx *Ctx) error { return nil })
	Condition("bench.mu.cond", func(ctx *Ctx) bool { return true })

	wf, _ := NewWorkflow("bench-mu").
		Step("bench.mu.a").
		Branch(WhenNamed("bench.mu.cond").Step("bench.mu.a"), Default().Step("bench.mu.b")).
		Commit()

	def, _ := WorkflowToDefinition(wf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, _ := MarshalWorkflowDefinition(def)
		_, err := UnmarshalWorkflowDefinition(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewWorkflowFromDef(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.rebuild.a", func(ctx *Ctx) error { return nil })
	Task("bench.rebuild.b", func(ctx *Ctx) error { return nil })
	Condition("bench.rebuild.cond", func(ctx *Ctx) bool { return true })

	wf, _ := NewWorkflow("bench-rebuild").
		Step("bench.rebuild.a").
		Branch(WhenNamed("bench.rebuild.cond").Step("bench.rebuild.a"), Default().Step("bench.rebuild.b")).
		ForEach("items", Step("bench.rebuild.a")).
		Commit()

	def, _ := WorkflowToDefinition(wf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewWorkflowFromDef(def)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// === Registry Lookup ===

func BenchmarkRegistryLookupTask(b *testing.B) {
	reg := NewRegistry()
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("bench.reg.task_%d", i)
		reg.RegisterTask(name, func(ctx *Ctx) error { return nil })
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reg.lookupTask(fmt.Sprintf("bench.reg.task_%d", i%100))
	}
}

func BenchmarkRegistryLookupCondition(b *testing.B) {
	reg := NewRegistry()
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("bench.reg.cond_%d", i)
		reg.RegisterCondition(name, func(ctx *Ctx) bool { return true })
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reg.lookupCondition(fmt.Sprintf("bench.reg.cond_%d", i%100))
	}
}

// === Event Handler Overhead ===

func BenchmarkRunSync_NoEventHandler(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.ev.noop", func(ctx *Ctx) error { return nil })
	wf, _ := NewWorkflow("bench-noev").Step("bench.ev.noop").Commit()
	engine, _ := New()
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.RunSync(context.Background(), wf, nil)
	}
}

func BenchmarkRunSync_WithEventHandler(b *testing.B) {
	ResetTaskRegistry()
	Task("bench.ev.handler", func(ctx *Ctx) error { return nil })
	wf, _ := NewWorkflow("bench-withev").Step("bench.ev.handler").Commit()
	engine, _ := New(WithEventHandler(&noopEventHandler{}))
	defer engine.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.RunSync(context.Background(), wf, nil)
	}
}

type noopEventHandler struct{}

func (h *noopEventHandler) OnEvent(_ EngineEvent) {}
