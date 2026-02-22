package gostage

import (
	"testing"
	"time"
)

func TestWorkflow_SingleStep(t *testing.T) {
	ResetTaskRegistry()
	Task("w.single", func(ctx *Ctx) error { return nil })

	wf := NewWorkflow("test-single").Step("w.single").Commit()
	if wf.ID != "test-single" {
		t.Fatalf("expected ID test-single, got %s", wf.ID)
	}
	if len(wf.steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(wf.steps))
	}
	if wf.steps[0].kind != stepSingle {
		t.Fatalf("expected stepSingle, got %d", wf.steps[0].kind)
	}
}

func TestWorkflow_MultipleSteps(t *testing.T) {
	ResetTaskRegistry()
	Task("w.a", func(ctx *Ctx) error { return nil })
	Task("w.b", func(ctx *Ctx) error { return nil })
	Task("w.c", func(ctx *Ctx) error { return nil })

	wf := NewWorkflow("multi").Step("w.a").Step("w.b").Step("w.c").Commit()
	if len(wf.steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(wf.steps))
	}
}

func TestWorkflow_Parallel(t *testing.T) {
	ResetTaskRegistry()
	Task("w.p1", func(ctx *Ctx) error { return nil })
	Task("w.p2", func(ctx *Ctx) error { return nil })
	Task("w.p3", func(ctx *Ctx) error { return nil })

	wf := NewWorkflow("par").
		Parallel(Step("w.p1"), Step("w.p2"), Step("w.p3")).
		Commit()

	if len(wf.steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(wf.steps))
	}
	if wf.steps[0].kind != stepParallel {
		t.Fatalf("expected stepParallel")
	}
	if len(wf.steps[0].refs) != 3 {
		t.Fatalf("expected 3 refs, got %d", len(wf.steps[0].refs))
	}
}

func TestWorkflow_Branch(t *testing.T) {
	ResetTaskRegistry()
	Task("w.urgent", func(ctx *Ctx) error { return nil })
	Task("w.normal", func(ctx *Ctx) error { return nil })

	wf := NewWorkflow("branch").
		Branch(
			When(func(ctx *Ctx) bool { return true }).Step("w.urgent"),
			Default().Step("w.normal"),
		).
		Commit()

	if wf.steps[0].kind != stepBranch {
		t.Fatalf("expected stepBranch")
	}
	if len(wf.steps[0].cases) != 2 {
		t.Fatalf("expected 2 cases, got %d", len(wf.steps[0].cases))
	}
}

func TestWorkflow_ForEach(t *testing.T) {
	ResetTaskRegistry()
	Task("w.each", func(ctx *Ctx) error { return nil })

	wf := NewWorkflow("foreach").
		ForEach("items", Step("w.each"), WithConcurrency(4)).
		Commit()

	if wf.steps[0].kind != stepForEach {
		t.Fatalf("expected stepForEach")
	}
	if wf.steps[0].collectionKey != "items" {
		t.Fatalf("expected collection key 'items'")
	}
	if wf.steps[0].concurrency != 4 {
		t.Fatalf("expected concurrency 4, got %d", wf.steps[0].concurrency)
	}
}

func TestWorkflow_ForEach_DefaultSequential(t *testing.T) {
	ResetTaskRegistry()
	Task("w.seq", func(ctx *Ctx) error { return nil })

	wf := NewWorkflow("foreach-seq").
		ForEach("items", Step("w.seq")).
		Commit()

	if wf.steps[0].concurrency != 1 {
		t.Fatalf("expected default concurrency 1, got %d", wf.steps[0].concurrency)
	}
}

func TestWorkflow_Map(t *testing.T) {
	ResetTaskRegistry()

	wf := NewWorkflow("map").
		Map(func(ctx *Ctx) {
			Set(ctx, "transformed", true)
		}).
		Commit()

	if wf.steps[0].kind != stepMap {
		t.Fatalf("expected stepMap")
	}
}

func TestWorkflow_DoUntil(t *testing.T) {
	ResetTaskRegistry()
	Task("w.poll", func(ctx *Ctx) error { return nil })

	wf := NewWorkflow("until").
		DoUntil(Step("w.poll"), func(ctx *Ctx) bool { return true }).
		Commit()

	if wf.steps[0].kind != stepDoUntil {
		t.Fatalf("expected stepDoUntil")
	}
}

func TestWorkflow_DoWhile(t *testing.T) {
	ResetTaskRegistry()
	Task("w.fetch", func(ctx *Ctx) error { return nil })

	wf := NewWorkflow("while").
		DoWhile(Step("w.fetch"), func(ctx *Ctx) bool { return false }).
		Commit()

	if wf.steps[0].kind != stepDoWhile {
		t.Fatalf("expected stepDoWhile")
	}
}

func TestWorkflow_SubWorkflow(t *testing.T) {
	ResetTaskRegistry()
	Task("w.inner", func(ctx *Ctx) error { return nil })

	inner := NewWorkflow("inner").Step("w.inner").Commit()
	outer := NewWorkflow("outer").SubWorkflow(inner).Commit()

	if outer.steps[0].kind != stepSub {
		t.Fatalf("expected stepSub")
	}
}

func TestWorkflow_Sleep(t *testing.T) {
	ResetTaskRegistry()

	wf := NewWorkflow("sleep").
		Sleep(5 * time.Minute).
		Commit()

	if wf.steps[0].kind != stepSleep {
		t.Fatalf("expected stepSleep")
	}
	if wf.steps[0].sleepDuration != 5*time.Minute {
		t.Fatalf("expected 5m sleep, got %s", wf.steps[0].sleepDuration)
	}
}

func TestWorkflow_Stage(t *testing.T) {
	ResetTaskRegistry()
	Task("w.val1", func(ctx *Ctx) error { return nil })
	Task("w.val2", func(ctx *Ctx) error { return nil })

	wf := NewWorkflow("staged").
		Stage("validation", Step("w.val1"), Step("w.val2")).
		Commit()

	if wf.steps[0].kind != stepStage {
		t.Fatalf("expected stepStage")
	}
	if wf.steps[0].name != "validation" {
		t.Fatalf("expected stage name 'validation', got %s", wf.steps[0].name)
	}
	if len(wf.steps[0].refs) != 2 {
		t.Fatalf("expected 2 refs, got %d", len(wf.steps[0].refs))
	}
}

func TestWorkflow_WithSpawn(t *testing.T) {
	ResetTaskRegistry()
	Task("w.spawn", func(ctx *Ctx) error { return nil })

	wf := NewWorkflow("spawn").
		ForEach("items", Step("w.spawn"), WithConcurrency(2), WithSpawn()).
		Commit()

	if !wf.steps[0].useSpawn {
		t.Fatal("expected useSpawn to be true")
	}
}

func TestWorkflow_Commit_PanicsOnUnregisteredTask(t *testing.T) {
	ResetTaskRegistry()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for unregistered task")
		}
	}()

	NewWorkflow("bad").Step("nonexistent").Commit()
}

func TestWorkflow_ComplexComposition(t *testing.T) {
	ResetTaskRegistry()
	Task("w.validate", func(ctx *Ctx) error { return nil })
	Task("w.charge", func(ctx *Ctx) error { return nil })
	Task("w.reserve", func(ctx *Ctx) error { return nil })
	Task("w.process", func(ctx *Ctx) error { return nil })
	Task("w.fulfill", func(ctx *Ctx) error { return nil })

	wf := NewWorkflow("order",
		WithDefaultRetry(3, time.Second),
		OnStepComplete(func(step string, ctx *Ctx) {}),
		OnError(func(err error) {}),
	).
		Step("w.validate").
		Parallel(Step("w.charge"), Step("w.reserve")).
		Branch(
			When(func(ctx *Ctx) bool { return true }).Step("w.process"),
			Default().Step("w.fulfill"),
		).
		Step("w.fulfill").
		Commit()

	if len(wf.steps) != 4 {
		t.Fatalf("expected 4 steps, got %d", len(wf.steps))
	}
	if wf.cfg.defaultRetries != 3 {
		t.Fatalf("expected 3 default retries, got %d", wf.cfg.defaultRetries)
	}
}
