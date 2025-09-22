package runner

import (
	"sync"
	"testing"

	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v2/types"
)

// mockStage implements types.Stage for testing
type mockStage struct {
	id          string
	name        string
	description string
	tags        []string
}

func (m *mockStage) ID() string                           { return m.id }
func (m *mockStage) Name() string                         { return m.name }
func (m *mockStage) Description() string                  { return m.description }
func (m *mockStage) Tags() []string                       { return m.tags }
func (m *mockStage) Actions() types.ActionMutation        { return nil }
func (m *mockStage) InitialStore() *store.KVStore         { return store.NewKVStore() }
func (m *mockStage) Middlewares() []types.StageMiddleware { return nil }

func TestStageMutationAdd(t *testing.T) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	ctx := &actionContext{
		workflow:       workflow,
		dynamicStages:  []types.Stage{},
		disabledStages: make(map[string]bool),
	}

	mutation := newStageMutation(ctx)

	stage1 := &mockStage{id: "stage1", name: "Stage 1", description: "test stage 1", tags: []string{"test"}}
	stage2 := &mockStage{id: "stage2", name: "Stage 2", description: "test stage 2", tags: []string{"test", "second"}}

	// Test adding stages
	mutation.Add(stage1)
	mutation.Add(stage2)

	if len(ctx.dynamicStages) != 2 {
		t.Errorf("Expected 2 dynamic stages, got %d", len(ctx.dynamicStages))
	}

	if ctx.dynamicStages[0].ID() != "stage1" {
		t.Errorf("Expected first stage to be 'stage1', got %s", ctx.dynamicStages[0].ID())
	}
}

func TestStageMutationDisableEnable(t *testing.T) {
	stage1 := &mockStage{id: "stage1", name: "Stage 1", description: "test stage 1", tags: []string{"test"}}
	stage2 := &mockStage{id: "stage2", name: "Stage 2", description: "test stage 2", tags: []string{"test", "second"}}

	workflow := &mockWorkflow{stages: []types.Stage{stage1, stage2}}
	ctx := &actionContext{
		workflow:       workflow,
		dynamicStages:  []types.Stage{},
		disabledStages: make(map[string]bool),
	}

	mutation := newStageMutation(ctx)

	// Test disable
	mutation.Disable("stage1")
	if !ctx.disabledStages["stage1"] {
		t.Error("Expected stage1 to be disabled")
	}

	// Test IsEnabled
	if mutation.IsEnabled("stage1") {
		t.Error("Expected stage1 to be disabled")
	}
	if !mutation.IsEnabled("stage2") {
		t.Error("Expected stage2 to be enabled")
	}

	// Test enable
	mutation.Enable("stage1")
	if ctx.disabledStages["stage1"] {
		t.Error("Expected stage1 to be enabled")
	}
	if !mutation.IsEnabled("stage1") {
		t.Error("Expected stage1 to be enabled after Enable()")
	}
}

func TestStageMutationDisableByTags(t *testing.T) {
	stage1 := &mockStage{id: "stage1", name: "Stage 1", description: "test stage 1", tags: []string{"test", "first"}}
	stage2 := &mockStage{id: "stage2", name: "Stage 2", description: "test stage 2", tags: []string{"test", "second"}}
	stage3 := &mockStage{id: "stage3", name: "Stage 3", description: "test stage 3", tags: []string{"other"}}

	workflow := &mockWorkflow{stages: []types.Stage{stage1, stage2, stage3}}
	ctx := &actionContext{
		workflow:       workflow,
		dynamicStages:  []types.Stage{},
		disabledStages: make(map[string]bool),
	}

	mutation := newStageMutation(ctx)

	// Disable by tag "test"
	count := mutation.DisableByTags([]string{"test"})
	if count != 2 {
		t.Errorf("Expected 2 stages to be disabled, got %d", count)
	}

	if !ctx.disabledStages["stage1"] || !ctx.disabledStages["stage2"] {
		t.Error("Expected stage1 and stage2 to be disabled")
	}
	if ctx.disabledStages["stage3"] {
		t.Error("Expected stage3 to remain enabled")
	}
}

func TestStageMutationEnableByTags(t *testing.T) {
	stage1 := &mockStage{id: "stage1", name: "Stage 1", description: "test stage 1", tags: []string{"test", "first"}}
	stage2 := &mockStage{id: "stage2", name: "Stage 2", description: "test stage 2", tags: []string{"test", "second"}}
	stage3 := &mockStage{id: "stage3", name: "Stage 3", description: "test stage 3", tags: []string{"other"}}

	workflow := &mockWorkflow{stages: []types.Stage{stage1, stage2, stage3}}
	ctx := &actionContext{
		workflow:       workflow,
		dynamicStages:  []types.Stage{},
		disabledStages: map[string]bool{"stage1": true, "stage2": true, "stage3": true},
	}

	mutation := newStageMutation(ctx)

	// Enable by tag "test"
	count := mutation.EnableByTags([]string{"test"})
	if count != 2 {
		t.Errorf("Expected 2 stages to be enabled, got %d", count)
	}

	if ctx.disabledStages["stage1"] || ctx.disabledStages["stage2"] {
		t.Error("Expected stage1 and stage2 to be enabled")
	}
	if !ctx.disabledStages["stage3"] {
		t.Error("Expected stage3 to remain disabled")
	}
}

func TestStageMutationRemove(t *testing.T) {
	stage1 := &mockStage{id: "stage1", name: "Stage 1", description: "test stage 1", tags: []string{"test"}}
	stage2 := &mockStage{id: "stage2", name: "Stage 2", description: "test stage 2", tags: []string{"test"}}
	stage3 := &mockStage{id: "stage3", name: "Stage 3", description: "test stage 3", tags: []string{"other"}}

	workflow := &mockWorkflow{stages: []types.Stage{stage1, stage2, stage3}}
	ctx := &actionContext{
		workflow:       workflow,
		dynamicStages:  []types.Stage{stage1, stage2},
		disabledStages: make(map[string]bool),
	}

	mutation := newStageMutation(ctx)

	// Remove stage2 (should mark as disabled since we can't remove from workflow)
	removed := mutation.Remove("stage2")
	if !removed {
		t.Error("Expected Remove to return true")
	}

	// Verify stage2 is disabled
	if !ctx.disabledStages["stage2"] {
		t.Error("Expected stage2 to be disabled after removal")
	}

	// Remove from dynamic stages
	ctx.dynamicStages = []types.Stage{stage1, stage2}
	removed = mutation.Remove("stage1")
	if !removed {
		t.Error("Expected Remove to return true for dynamic stage")
	}

	// Try to remove non-existent stage
	removed = mutation.Remove("nonexistent")
	if removed {
		t.Error("Expected Remove to return false for non-existent stage")
	}
}

func TestStageMutationRemoveByTags(t *testing.T) {
	stage1 := &mockStage{id: "stage1", name: "Stage 1", description: "test stage 1", tags: []string{"test", "first"}}
	stage2 := &mockStage{id: "stage2", name: "Stage 2", description: "test stage 2", tags: []string{"test", "second"}}
	stage3 := &mockStage{id: "stage3", name: "Stage 3", description: "test stage 3", tags: []string{"other"}}

	workflow := &mockWorkflow{stages: []types.Stage{stage1, stage2, stage3}}
	ctx := &actionContext{
		workflow:       workflow,
		dynamicStages:  []types.Stage{stage1, stage2, stage3},
		disabledStages: make(map[string]bool),
	}

	mutation := newStageMutation(ctx)

	// Remove by tag "test"
	count := mutation.RemoveByTags([]string{"test"})
	if count != 2 {
		t.Errorf("Expected 2 stages to be removed, got %d", count)
	}

	// Verify stages are disabled
	if !ctx.disabledStages["stage1"] || !ctx.disabledStages["stage2"] {
		t.Error("Expected stage1 and stage2 to be disabled")
	}

	if len(ctx.dynamicStages) != 1 {
		t.Errorf("Expected 1 dynamic stage after removal, got %d", len(ctx.dynamicStages))
	}

	// Verify only stage3 remains in dynamic stages
	if ctx.dynamicStages[0].ID() != "stage3" {
		t.Errorf("Expected remaining stage to be 'stage3', got %s", ctx.dynamicStages[0].ID())
	}
}

// Test concurrent access to stage mutations
func TestStageMutationConcurrentAccess(t *testing.T) {
	// Create workflow with initial stages
	stages := []types.Stage{}
	for i := 0; i < 50; i++ {
		stage := &mockStage{
			id:          "stage" + string(rune('0'+i/10)) + string(rune('0'+i%10)),
			name:        "Stage " + string(rune('0'+i)),
			description: "test stage",
			tags:        []string{"test", "concurrent"},
		}
		stages = append(stages, stage)
	}

	workflow := &mockWorkflow{stages: stages}
	ctx := &actionContext{
		workflow:       workflow,
		dynamicStages:  []types.Stage{},
		disabledStages: make(map[string]bool),
	}

	mutation := newStageMutation(ctx)

	var wg sync.WaitGroup
	numGoroutines := 10

	// Concurrent adds
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				stage := &mockStage{
					id:          "concurrent" + string(rune('0'+id)) + string(rune('0'+j)),
					name:        "Concurrent Stage",
					description: "concurrent test",
					tags:        []string{"concurrent"},
				}
				mutation.Add(stage)
			}
		}(i)
	}

	// Concurrent disables
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				stageID := "stage" + string(rune('0'+id%5)) + string(rune('0'+j))
				mutation.Disable(stageID)
			}
		}(i)
	}

	// Concurrent enables
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				stageID := "stage" + string(rune('0'+id%5)) + string(rune('0'+j))
				mutation.Enable(stageID)
			}
		}(i)
	}

	// Concurrent IsEnabled checks
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				stageID := "stage" + string(rune('0'+id%5)) + string(rune('0'+j))
				_ = mutation.IsEnabled(stageID)
			}
		}(i)
	}

	wg.Wait()

	// Verify we have added 100 stages (10 goroutines * 10 stages each)
	if len(ctx.dynamicStages) != 100 {
		t.Errorf("Expected 100 dynamic stages from concurrent adds, got %d", len(ctx.dynamicStages))
	}
}

// Test that the mutex prevents race conditions for stages
func TestStageMutationRaceCondition(t *testing.T) {
	stage := &mockStage{id: "racetest", name: "Race Test", description: "race test", tags: []string{"race"}}
	workflow := &mockWorkflow{stages: []types.Stage{stage}}

	ctx := &actionContext{
		workflow:       workflow,
		dynamicStages:  []types.Stage{},
		disabledStages: make(map[string]bool),
	}

	mutation := newStageMutation(ctx)

	var wg sync.WaitGroup
	iterations := 1000

	// Run concurrent enable/disable operations
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			mutation.Disable("racetest")
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			mutation.Enable("racetest")
		}
	}()

	wg.Wait()

	// The final state should be consistent (either enabled or disabled)
	// This test mainly verifies no panic or data corruption occurs
	finalState := mutation.IsEnabled("racetest")
	t.Logf("Final state after race test: enabled=%v", finalState)
}
