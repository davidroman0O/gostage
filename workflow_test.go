package gostage

import (
	"testing"
	"time"
)

func TestWorkflow_SingleStep(t *testing.T) {
	ResetTaskRegistry()
	Task("w.single", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("test-single").Step("w.single").Commit()
	if err != nil {
		t.Fatal(err)
	}
	if wf.ID != "test-single" {
		t.Fatalf("expected ID test-single, got %s", wf.ID)
	}
	if len(wf.steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(wf.steps))
	}
	if wf.steps[0].kind != StepSingle {
		t.Fatalf("expected StepSingle, got %d", wf.steps[0].kind)
	}
}

func TestWorkflow_MultipleSteps(t *testing.T) {
	ResetTaskRegistry()
	Task("w.a", func(ctx *Ctx) error { return nil })
	Task("w.b", func(ctx *Ctx) error { return nil })
	Task("w.c", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("multi").Step("w.a").Step("w.b").Step("w.c").Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(wf.steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(wf.steps))
	}
}

func TestWorkflow_Parallel(t *testing.T) {
	ResetTaskRegistry()
	Task("w.p1", func(ctx *Ctx) error { return nil })
	Task("w.p2", func(ctx *Ctx) error { return nil })
	Task("w.p3", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("par").
		Parallel(Step("w.p1"), Step("w.p2"), Step("w.p3")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if len(wf.steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(wf.steps))
	}
	if wf.steps[0].kind != StepParallel {
		t.Fatalf("expected StepParallel")
	}
	if len(wf.steps[0].refs) != 3 {
		t.Fatalf("expected 3 refs, got %d", len(wf.steps[0].refs))
	}
}

func TestWorkflow_Branch(t *testing.T) {
	ResetTaskRegistry()
	Task("w.urgent", func(ctx *Ctx) error { return nil })
	Task("w.normal", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("branch").
		Branch(
			When(func(ctx *Ctx) bool { return true }).Step("w.urgent"),
			Default().Step("w.normal"),
		).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if wf.steps[0].kind != StepBranch {
		t.Fatalf("expected StepBranch")
	}
	if len(wf.steps[0].cases) != 2 {
		t.Fatalf("expected 2 cases, got %d", len(wf.steps[0].cases))
	}
}

func TestWorkflow_ForEach(t *testing.T) {
	ResetTaskRegistry()
	Task("w.each", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("foreach").
		ForEach("items", Step("w.each"), WithConcurrency(4)).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if wf.steps[0].kind != StepForEach {
		t.Fatalf("expected StepForEach")
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

	wf, err := NewWorkflow("foreach-seq").
		ForEach("items", Step("w.seq")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if wf.steps[0].concurrency != 1 {
		t.Fatalf("expected default concurrency 1, got %d", wf.steps[0].concurrency)
	}
}

func TestWorkflow_Map(t *testing.T) {
	ResetTaskRegistry()

	wf, err := NewWorkflow("map").
		Map(func(ctx *Ctx) error {
			Set(ctx, "transformed", true)
			return nil
		}).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if wf.steps[0].kind != StepMap {
		t.Fatalf("expected StepMap")
	}
}

func TestWorkflow_DoUntil(t *testing.T) {
	ResetTaskRegistry()
	Task("w.poll", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("until").
		DoUntil(Step("w.poll"), func(ctx *Ctx) bool { return true }).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if wf.steps[0].kind != StepDoUntil {
		t.Fatalf("expected StepDoUntil")
	}
}

func TestWorkflow_DoWhile(t *testing.T) {
	ResetTaskRegistry()
	Task("w.fetch", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("while").
		DoWhile(Step("w.fetch"), func(ctx *Ctx) bool { return false }).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if wf.steps[0].kind != StepDoWhile {
		t.Fatalf("expected StepDoWhile")
	}
}

func TestWorkflow_SubWorkflow(t *testing.T) {
	ResetTaskRegistry()
	Task("w.inner", func(ctx *Ctx) error { return nil })

	inner, err := NewWorkflow("inner").Step("w.inner").Commit()
	if err != nil {
		t.Fatal(err)
	}
	outer, err := NewWorkflow("outer").Sub(inner).Commit()
	if err != nil {
		t.Fatal(err)
	}

	if outer.steps[0].kind != StepSub {
		t.Fatalf("expected StepSub")
	}
}

func TestWorkflow_Sleep(t *testing.T) {
	ResetTaskRegistry()

	wf, err := NewWorkflow("sleep").
		Sleep(5 * time.Minute).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if wf.steps[0].kind != StepSleep {
		t.Fatalf("expected StepSleep")
	}
	if wf.steps[0].sleepDuration != 5*time.Minute {
		t.Fatalf("expected 5m sleep, got %s", wf.steps[0].sleepDuration)
	}
}

func TestWorkflow_Stage(t *testing.T) {
	ResetTaskRegistry()
	Task("w.val1", func(ctx *Ctx) error { return nil })
	Task("w.val2", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("staged").
		Stage("validation", Step("w.val1"), Step("w.val2")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if wf.steps[0].kind != StepStage {
		t.Fatalf("expected StepStage")
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

	wf, err := NewWorkflow("spawn").
		ForEach("items", Step("w.spawn"), WithConcurrency(2), WithSpawn()).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	if !wf.steps[0].useSpawn {
		t.Fatal("expected useSpawn to be true")
	}
}

func TestWorkflow_Commit_ReturnsErrorOnUnregisteredTask(t *testing.T) {
	ResetTaskRegistry()

	_, err := NewWorkflow("bad").Step("nonexistent").Commit()
	if err == nil {
		t.Fatal("expected error for unregistered task")
	}
}

func TestWorkflow_ComplexComposition(t *testing.T) {
	ResetTaskRegistry()
	Task("w.validate", func(ctx *Ctx) error { return nil })
	Task("w.charge", func(ctx *Ctx) error { return nil })
	Task("w.reserve", func(ctx *Ctx) error { return nil })
	Task("w.process", func(ctx *Ctx) error { return nil })
	Task("w.fulfill", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("order",
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
	if err != nil {
		t.Fatal(err)
	}

	if len(wf.steps) != 4 {
		t.Fatalf("expected 4 steps, got %d", len(wf.steps))
	}
	if wf.cfg.defaultRetries != 3 {
		t.Fatalf("expected 3 default retries, got %d", wf.cfg.defaultRetries)
	}
}
