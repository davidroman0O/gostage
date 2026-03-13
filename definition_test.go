package gostage

import (
	"context"
	"testing"
	"time"
)

// === Serializable Workflow Definitions ===

func TestWorkflowToDefinition(t *testing.T) {
	ResetTaskRegistry()

	Task("def.validate", func(ctx *Ctx) error { return nil })
	Task("def.charge", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("order-def").
		Step("def.validate").
		Step("def.charge").
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	def, err := WorkflowToDefinition(wf)
	if err != nil {
		t.Fatal(err)
	}

	if def.ID != "order-def" {
		t.Fatalf("expected ID 'order-def', got %q", def.ID)
	}
	if len(def.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(def.Steps))
	}
	if def.Steps[0].TaskName != "def.validate" {
		t.Fatalf("expected first task 'def.validate', got %q", def.Steps[0].TaskName)
	}
}

func TestDefinitionMarshalUnmarshal(t *testing.T) {
	ResetTaskRegistry()

	Task("def.a", func(ctx *Ctx) error { return nil })
	Task("def.b", func(ctx *Ctx) error { return nil })

	wf, err := NewWorkflow("serial-def").Step("def.a").Step("def.b").Commit()
	if err != nil {
		t.Fatal(err)
	}
	def, defErr := WorkflowToDefinition(wf)
	if defErr != nil {
		t.Fatal(defErr)
	}

	data, err := MarshalWorkflowDefinition(def)
	if err != nil {
		t.Fatal(err)
	}

	def2, err := UnmarshalWorkflowDefinition(data)
	if err != nil {
		t.Fatal(err)
	}
	if def2.ID != def.ID {
		t.Fatalf("expected ID %q, got %q", def.ID, def2.ID)
	}
	if len(def2.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(def2.Steps))
	}
}

func TestNewWorkflowFromDef(t *testing.T) {
	ResetTaskRegistry()

	Task("def.step1", func(ctx *Ctx) error {
		Set(ctx, "step1", true)
		return nil
	})
	Task("def.step2", func(ctx *Ctx) error {
		Set(ctx, "step2", true)
		return nil
	})

	wf, err := NewWorkflow("rebuild-def").Step("def.step1").Step("def.step2").Commit()
	if err != nil {
		t.Fatal(err)
	}
	def, defErr := WorkflowToDefinition(wf)
	if defErr != nil {
		t.Fatal(defErr)
	}

	data, _ := MarshalWorkflowDefinition(def)
	def2, _ := UnmarshalWorkflowDefinition(data)

	rebuilt, err := NewWorkflowFromDef(def2)
	if err != nil {
		t.Fatal(err)
	}

	// Execute the rebuilt workflow
	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), rebuilt, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (err: %v)", result.Status, result.Error)
	}
	if result.Store["step1"] != true || result.Store["step2"] != true {
		t.Fatal("both steps should have executed")
	}
}

// === Function Registry ===

func TestFunctionRegistry(t *testing.T) {
	ResetTaskRegistry()

	// Register a condition
	Condition("test-cond", func(ctx *Ctx) bool { return true })
	if fn := lookupCondition("test-cond"); fn == nil {
		t.Fatal("expected to find registered condition")
	}
	if fn := lookupCondition("nonexistent"); fn != nil {
		t.Fatal("expected nil for unregistered condition")
	}

	// Register a map function
	MapFn("test-map", func(ctx *Ctx) error { return nil })
	if fn := lookupMapFn("test-map"); fn == nil {
		t.Fatal("expected to find registered map function")
	}
	if fn := lookupMapFn("nonexistent"); fn != nil {
		t.Fatal("expected nil for unregistered map function")
	}

	// Duplicate condition should panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on duplicate condition")
			}
		}()
		Condition("test-cond", func(ctx *Ctx) bool { return false })
	}()

	// Duplicate map function should panic
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic on duplicate map function")
			}
		}()
		MapFn("test-map", func(ctx *Ctx) error { return nil })
	}()

	// Reset clears everything
	ResetFunctionRegistries()
	if fn := lookupCondition("test-cond"); fn != nil {
		t.Fatal("expected nil after reset")
	}
	if fn := lookupMapFn("test-map"); fn != nil {
		t.Fatal("expected nil after reset")
	}
}

// === Named Builder Methods ===

func TestNamedBuilderMethods(t *testing.T) {
	ResetTaskRegistry()

	Task("nb.task", func(ctx *Ctx) error { return nil })
	Condition("nb.cond", func(ctx *Ctx) bool { return true })
	MapFn("nb.transform", func(ctx *Ctx) error { return nil })

	// WhenNamed — stores name only, no function pointer
	bc := WhenNamed("nb.cond").Step("nb.task")
	if bc.condName != "nb.cond" {
		t.Fatalf("expected condName 'nb.cond', got %q", bc.condName)
	}
	if bc.condition != nil {
		t.Fatal("expected condition function to be nil (lazy resolution)")
	}

	// MapNamed
	wf, err := NewWorkflow("nb-test").
		MapNamed("nb.transform").
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	if wf.steps[0].mapFnName != "nb.transform" {
		t.Fatalf("expected mapFnName 'nb.transform', got %q", wf.steps[0].mapFnName)
	}
	if wf.steps[0].mapFn != nil {
		t.Fatal("expected mapFn to be nil (lazy resolution)")
	}

	// DoUntilNamed
	wf2, err := NewWorkflow("nb-until").
		DoUntilNamed(Step("nb.task"), "nb.cond").
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	if wf2.steps[0].loopCondName != "nb.cond" {
		t.Fatalf("expected loopCondName 'nb.cond', got %q", wf2.steps[0].loopCondName)
	}

	// DoWhileNamed
	wf3, err := NewWorkflow("nb-while").
		DoWhileNamed(Step("nb.task"), "nb.cond").
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	if wf3.steps[0].loopCondName != "nb.cond" {
		t.Fatalf("expected loopCondName 'nb.cond', got %q", wf3.steps[0].loopCondName)
	}

	// WhenNamed with unregistered name succeeds at build time (lazy resolution)
	bc2 := WhenNamed("nonexistent").Step("nb.task")
	if bc2.condName != "nonexistent" {
		t.Fatalf("expected condName 'nonexistent', got %q", bc2.condName)
	}
}

// === Full Definition Serialization (All 10 Step Kinds) ===

func TestDefinitionAllStepKinds(t *testing.T) {
	ResetTaskRegistry()

	Task("all.task1", func(ctx *Ctx) error { return nil })
	Task("all.task2", func(ctx *Ctx) error { return nil })
	Task("all.task3", func(ctx *Ctx) error { return nil })
	Task("all.loop", func(ctx *Ctx) error { return nil })
	Condition("all.is-ready", func(ctx *Ctx) bool { return true })
	Condition("all.has-more", func(ctx *Ctx) bool { return false })
	MapFn("all.transform", func(ctx *Ctx) error { return nil })

	// Build a sub-workflow for StepSub
	subWf, err := NewWorkflow("all-sub").Step("all.task3").Commit()
	if err != nil {
		t.Fatal(err)
	}

	wf, err := NewWorkflow("all-kinds").
		Step("all.task1").                                           // StepSingle
		Stage("validation", Step("all.task1"), Step("all.task2")).   // StepStage
		Parallel(Step("all.task1"), Step("all.task2")).              // StepParallel
		Branch(                                                     // StepBranch
			WhenNamed("all.is-ready").Step("all.task1"),
			Default().Step("all.task2"),
		).
		ForEach("items", Step("all.task1"), WithConcurrency(3)).    // StepForEach
		MapNamed("all.transform").                                  // StepMap
		DoUntilNamed(Step("all.loop"), "all.is-ready").             // StepDoUntil
		DoWhileNamed(Step("all.loop"), "all.has-more").             // StepDoWhile
		Sub(subWf).                                                 // StepSub
		Sleep(5 * time.Second).                                     // StepSleep
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Serialize
	def, err := WorkflowToDefinition(wf)
	if err != nil {
		t.Fatalf("WorkflowToDefinition: %v", err)
	}

	if len(def.Steps) != 10 {
		t.Fatalf("expected 10 steps, got %d", len(def.Steps))
	}

	// Verify kind names
	expectedKinds := []string{"single", "stage", "parallel", "branch", "forEach", "map", "doUntil", "doWhile", "sub", "sleep"}
	for i, expected := range expectedKinds {
		if def.Steps[i].Kind != expected {
			t.Fatalf("step %d: expected kind %q, got %q", i, expected, def.Steps[i].Kind)
		}
	}

	// Verify specific fields
	if def.Steps[0].TaskName != "all.task1" {
		t.Fatalf("single step: expected task 'all.task1', got %q", def.Steps[0].TaskName)
	}
	if len(def.Steps[1].Refs) != 2 {
		t.Fatalf("stage step: expected 2 refs, got %d", len(def.Steps[1].Refs))
	}
	if len(def.Steps[2].Refs) != 2 {
		t.Fatalf("parallel step: expected 2 refs, got %d", len(def.Steps[2].Refs))
	}
	if len(def.Steps[3].Cases) != 2 {
		t.Fatalf("branch step: expected 2 cases, got %d", len(def.Steps[3].Cases))
	}
	if def.Steps[3].Cases[0].ConditionName != "all.is-ready" {
		t.Fatalf("branch case 0: expected condition 'all.is-ready', got %q", def.Steps[3].Cases[0].ConditionName)
	}
	if !def.Steps[3].Cases[1].IsDefault {
		t.Fatal("branch case 1: expected isDefault=true")
	}
	if def.Steps[4].CollectionKey != "items" {
		t.Fatalf("forEach step: expected collection key 'items', got %q", def.Steps[4].CollectionKey)
	}
	if def.Steps[4].Concurrency != 3 {
		t.Fatalf("forEach step: expected concurrency 3, got %d", def.Steps[4].Concurrency)
	}
	if def.Steps[5].MapFnName != "all.transform" {
		t.Fatalf("map step: expected map fn 'all.transform', got %q", def.Steps[5].MapFnName)
	}
	if def.Steps[6].LoopCondName != "all.is-ready" {
		t.Fatalf("doUntil step: expected cond 'all.is-ready', got %q", def.Steps[6].LoopCondName)
	}
	if def.Steps[7].LoopCondName != "all.has-more" {
		t.Fatalf("doWhile step: expected cond 'all.has-more', got %q", def.Steps[7].LoopCondName)
	}
	if def.Steps[8].SubWorkflow == nil {
		t.Fatal("sub step: expected sub-workflow, got nil")
	}
	if def.Steps[8].SubWorkflow.ID != "all-sub" {
		t.Fatalf("sub step: expected sub-workflow ID 'all-sub', got %q", def.Steps[8].SubWorkflow.ID)
	}
	if def.Steps[9].SleepDuration != (5 * time.Second).String() {
		t.Fatalf("sleep step: expected duration '5s', got %q", def.Steps[9].SleepDuration)
	}

	// Marshal -> Unmarshal round-trip
	data, marshalErr := MarshalWorkflowDefinition(def)
	if marshalErr != nil {
		t.Fatalf("marshal: %v", marshalErr)
	}

	def2, unmarshalErr := UnmarshalWorkflowDefinition(data)
	if unmarshalErr != nil {
		t.Fatalf("unmarshal: %v", unmarshalErr)
	}

	if len(def2.Steps) != 10 {
		t.Fatalf("after round-trip: expected 10 steps, got %d", len(def2.Steps))
	}

	// Rebuild workflow from definition
	rebuilt, rebuildErr := NewWorkflowFromDef(def2)
	if rebuildErr != nil {
		t.Fatalf("NewWorkflowFromDef: %v", rebuildErr)
	}

	if len(rebuilt.steps) != 10 {
		t.Fatalf("rebuilt: expected 10 steps, got %d", len(rebuilt.steps))
	}

	// Verify rebuilt step kinds match
	expectedStepKinds := []StepKind{StepSingle, StepStage, StepParallel, StepBranch, StepForEach, StepMap, StepDoUntil, StepDoWhile, StepSub, StepSleep}
	for i, expected := range expectedStepKinds {
		if rebuilt.steps[i].kind != expected {
			t.Fatalf("rebuilt step %d: expected kind %d, got %d", i, expected, rebuilt.steps[i].kind)
		}
	}

	// Verify rebuilt functions are wired
	if rebuilt.steps[3].cases[0].condition == nil {
		t.Fatal("rebuilt branch: condition function should be wired")
	}
	if rebuilt.steps[5].mapFn == nil {
		t.Fatal("rebuilt map: map function should be wired")
	}
	if rebuilt.steps[6].loopCond == nil {
		t.Fatal("rebuilt doUntil: loop condition should be wired")
	}
	if rebuilt.steps[9].sleepDuration != 5*time.Second {
		t.Fatalf("rebuilt sleep: expected 5s, got %v", rebuilt.steps[9].sleepDuration)
	}
}

func TestDefinitionAnonymousError(t *testing.T) {
	ResetTaskRegistry()

	Task("anon.task", func(ctx *Ctx) error { return nil })

	// Anonymous branch condition -> error
	wfBranch, err := NewWorkflow("anon-branch").
		Branch(When(func(ctx *Ctx) bool { return true }).Step("anon.task")).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	_, err = WorkflowToDefinition(wfBranch)
	if err == nil {
		t.Fatal("expected error for anonymous branch condition")
	}
	if !contains(err.Error(), "unnamed condition") {
		t.Fatalf("expected 'unnamed condition' in error, got: %v", err)
	}

	// Anonymous map function -> error
	wfMap, err := NewWorkflow("anon-map").
		Map(func(ctx *Ctx) error { return nil }).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	_, err = WorkflowToDefinition(wfMap)
	if err == nil {
		t.Fatal("expected error for anonymous map function")
	}
	if !contains(err.Error(), "unnamed map function") {
		t.Fatalf("expected 'unnamed map function' in error, got: %v", err)
	}

	// Anonymous loop condition -> error
	wfLoop, err := NewWorkflow("anon-loop").
		DoUntil(Step("anon.task"), func(ctx *Ctx) bool { return true }).
		Commit()
	if err != nil {
		t.Fatal(err)
	}
	_, err = WorkflowToDefinition(wfLoop)
	if err == nil {
		t.Fatal("expected error for anonymous loop condition")
	}
	if !contains(err.Error(), "unnamed loop condition") {
		t.Fatalf("expected 'unnamed loop condition' in error, got: %v", err)
	}
}

func TestDefinitionSubWorkflow(t *testing.T) {
	ResetTaskRegistry()

	Task("sub.inner", func(ctx *Ctx) error {
		Set(ctx, "inner_ran", true)
		return nil
	})
	Task("sub.outer", func(ctx *Ctx) error {
		Set(ctx, "outer_ran", true)
		return nil
	})

	inner, err := NewWorkflow("inner-wf").Step("sub.inner").Commit()
	if err != nil {
		t.Fatal(err)
	}
	outer, err := NewWorkflow("outer-wf").
		Step("sub.outer").
		Sub(inner).
		Commit()
	if err != nil {
		t.Fatal(err)
	}

	// Serialize
	def, err := WorkflowToDefinition(outer)
	if err != nil {
		t.Fatalf("WorkflowToDefinition: %v", err)
	}

	if len(def.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(def.Steps))
	}
	if def.Steps[1].Kind != "sub" {
		t.Fatalf("expected step 1 kind 'sub', got %q", def.Steps[1].Kind)
	}
	if def.Steps[1].SubWorkflow.ID != "inner-wf" {
		t.Fatalf("expected sub-workflow ID 'inner-wf', got %q", def.Steps[1].SubWorkflow.ID)
	}

	// Round-trip
	data, _ := MarshalWorkflowDefinition(def)
	def2, _ := UnmarshalWorkflowDefinition(data)
	rebuilt, err := NewWorkflowFromDef(def2)
	if err != nil {
		t.Fatalf("NewWorkflowFromDef: %v", err)
	}

	// Execute rebuilt workflow
	engine, _ := New()
	defer engine.Close()

	result, _ := engine.RunSync(context.Background(), rebuilt, nil)
	if result.Status != Completed {
		t.Fatalf("expected Completed, got %s (err: %v)", result.Status, result.Error)
	}
	if result.Store["outer_ran"] != true || result.Store["inner_ran"] != true {
		t.Fatalf("both inner and outer should have run: %v", result.Store)
	}
}
