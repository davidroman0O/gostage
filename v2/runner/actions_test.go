package runner

import (
	"sync"
	"testing"

	"github.com/davidroman0O/gostage/v2/types"
)

// mockAction implements types.Action for testing
type mockAction struct {
	name        string
	description string
	tags        []string
}

func (m *mockAction) Name() string                    { return m.name }
func (m *mockAction) Description() string             { return m.description }
func (m *mockAction) Tags() []string                  { return m.tags }
func (m *mockAction) Execute(ctx types.Context) error { return nil }

func TestActionMutationAdd(t *testing.T) {
	ctx := &actionContext{
		allActions:      []types.Action{},
		dynamicActions:  []types.Action{},
		disabledActions: make(map[string]bool),
	}

	mutation := newActionMutation(ctx)

	action1 := &mockAction{name: "action1", description: "test action 1", tags: []string{"test"}}
	action2 := &mockAction{name: "action2", description: "test action 2", tags: []string{"test", "second"}}

	// Test adding actions
	mutation.Add(action1)
	mutation.Add(action2)

	if len(ctx.dynamicActions) != 2 {
		t.Errorf("Expected 2 dynamic actions, got %d", len(ctx.dynamicActions))
	}

	if ctx.dynamicActions[0].Name() != "action1" {
		t.Errorf("Expected first action to be 'action1', got %s", ctx.dynamicActions[0].Name())
	}
}

func TestActionMutationDisableEnable(t *testing.T) {
	action1 := &mockAction{name: "action1", description: "test action 1", tags: []string{"test"}}
	action2 := &mockAction{name: "action2", description: "test action 2", tags: []string{"test", "second"}}

	ctx := &actionContext{
		allActions:      []types.Action{action1, action2},
		dynamicActions:  []types.Action{},
		disabledActions: make(map[string]bool),
	}

	mutation := newActionMutation(ctx)

	// Test disable
	mutation.Disable("action1")
	if !ctx.disabledActions["action1"] {
		t.Error("Expected action1 to be disabled")
	}

	// Test IsEnabled
	if mutation.IsEnabled("action1") {
		t.Error("Expected action1 to be disabled")
	}
	if !mutation.IsEnabled("action2") {
		t.Error("Expected action2 to be enabled")
	}

	// Test enable
	mutation.Enable("action1")
	if ctx.disabledActions["action1"] {
		t.Error("Expected action1 to be enabled")
	}
	if !mutation.IsEnabled("action1") {
		t.Error("Expected action1 to be enabled after Enable()")
	}
}

func TestActionMutationDisableByTags(t *testing.T) {
	action1 := &mockAction{name: "action1", description: "test action 1", tags: []string{"test", "first"}}
	action2 := &mockAction{name: "action2", description: "test action 2", tags: []string{"test", "second"}}
	action3 := &mockAction{name: "action3", description: "test action 3", tags: []string{"other"}}

	ctx := &actionContext{
		allActions:      []types.Action{action1, action2, action3},
		dynamicActions:  []types.Action{},
		disabledActions: make(map[string]bool),
	}

	mutation := newActionMutation(ctx)

	// Disable by tag "test"
	count := mutation.DisableByTags([]string{"test"})
	if count != 2 {
		t.Errorf("Expected 2 actions to be disabled, got %d", count)
	}

	if !ctx.disabledActions["action1"] || !ctx.disabledActions["action2"] {
		t.Error("Expected action1 and action2 to be disabled")
	}
	if ctx.disabledActions["action3"] {
		t.Error("Expected action3 to remain enabled")
	}
}

func TestActionMutationEnableByTags(t *testing.T) {
	action1 := &mockAction{name: "action1", description: "test action 1", tags: []string{"test", "first"}}
	action2 := &mockAction{name: "action2", description: "test action 2", tags: []string{"test", "second"}}
	action3 := &mockAction{name: "action3", description: "test action 3", tags: []string{"other"}}

	ctx := &actionContext{
		allActions:      []types.Action{action1, action2, action3},
		dynamicActions:  []types.Action{},
		disabledActions: map[string]bool{"action1": true, "action2": true, "action3": true},
	}

	mutation := newActionMutation(ctx)

	// Enable by tag "test"
	count := mutation.EnableByTags([]string{"test"})
	if count != 2 {
		t.Errorf("Expected 2 actions to be enabled, got %d", count)
	}

	if ctx.disabledActions["action1"] || ctx.disabledActions["action2"] {
		t.Error("Expected action1 and action2 to be enabled")
	}
	if !ctx.disabledActions["action3"] {
		t.Error("Expected action3 to remain disabled")
	}
}

func TestActionMutationRemove(t *testing.T) {
	action1 := &mockAction{name: "action1", description: "test action 1", tags: []string{"test"}}
	action2 := &mockAction{name: "action2", description: "test action 2", tags: []string{"test"}}
	action3 := &mockAction{name: "action3", description: "test action 3", tags: []string{"other"}}

	ctx := &actionContext{
		allActions:      []types.Action{action1, action2, action3},
		dynamicActions:  []types.Action{action1, action2},
		disabledActions: make(map[string]bool),
	}

	mutation := newActionMutation(ctx)

	// Remove action2
	removed := mutation.Remove("action2")
	if !removed {
		t.Error("Expected Remove to return true")
	}

	if len(ctx.allActions) != 2 {
		t.Errorf("Expected 2 actions after removal, got %d", len(ctx.allActions))
	}

	// Verify action2 is removed
	for _, action := range ctx.allActions {
		if action.Name() == "action2" {
			t.Error("action2 should have been removed from allActions")
		}
	}

	// Try to remove non-existent action
	removed = mutation.Remove("nonexistent")
	if removed {
		t.Error("Expected Remove to return false for non-existent action")
	}
}

func TestActionMutationRemoveByTags(t *testing.T) {
	action1 := &mockAction{name: "action1", description: "test action 1", tags: []string{"test", "first"}}
	action2 := &mockAction{name: "action2", description: "test action 2", tags: []string{"test", "second"}}
	action3 := &mockAction{name: "action3", description: "test action 3", tags: []string{"other"}}

	ctx := &actionContext{
		allActions:      []types.Action{action1, action2, action3},
		dynamicActions:  []types.Action{action1, action2, action3},
		disabledActions: make(map[string]bool),
	}

	mutation := newActionMutation(ctx)

	// Remove by tag "test"
	count := mutation.RemoveByTags([]string{"test"})
	if count != 2 {
		t.Errorf("Expected 2 actions to be removed, got %d", count)
	}

	if len(ctx.allActions) != 1 {
		t.Errorf("Expected 1 action after removal, got %d", len(ctx.allActions))
	}

	if len(ctx.dynamicActions) != 1 {
		t.Errorf("Expected 1 dynamic action after removal, got %d", len(ctx.dynamicActions))
	}

	// Verify only action3 remains
	if ctx.allActions[0].Name() != "action3" {
		t.Errorf("Expected remaining action to be 'action3', got %s", ctx.allActions[0].Name())
	}
}

// Test concurrent access to action mutations
func TestActionMutationConcurrentAccess(t *testing.T) {
	// Create a properly initialized context with mutex
	workflow := &mockWorkflow{stages: []types.Stage{}}
	ctx := newActionContext(workflow)

	// Populate with initial actions using the thread-safe method
	initialActions := []types.Action{}
	for i := 0; i < 100; i++ {
		action := &mockAction{
			name:        string(rune('a'+i%26)) + string(rune('0'+i/26)),
			description: "test action",
			tags:        []string{"test", "concurrent"},
		}
		initialActions = append(initialActions, action)
	}
	ctx.populateActions(initialActions)

	mutation := newActionMutation(ctx)

	var wg sync.WaitGroup
	numGoroutines := 10

	// Concurrent adds
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				action := &mockAction{
					name:        "concurrent" + string(rune('0'+id)) + string(rune('0'+j)),
					description: "concurrent test",
					tags:        []string{"concurrent"},
				}
				mutation.Add(action)
			}
		}(i)
	}

	// Concurrent disables
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				actionName := string(rune('a'+id%26)) + string(rune('0'+j))
				mutation.Disable(actionName)
			}
		}(i)
	}

	// Concurrent enables
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				actionName := string(rune('a'+id%26)) + string(rune('0'+j))
				mutation.Enable(actionName)
			}
		}(i)
	}

	// Concurrent IsEnabled checks
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				actionName := string(rune('a'+id%26)) + string(rune('0'+j))
				_ = mutation.IsEnabled(actionName)
			}
		}(i)
	}

	// Concurrent RemoveByTags
	wg.Add(2)
	go func() {
		defer wg.Done()
		mutation.RemoveByTags([]string{"toremove"})
	}()
	go func() {
		defer wg.Done()
		mutation.DisableByTags([]string{"todisable"})
	}()

	wg.Wait()

	// Verify we have added 100 actions (10 goroutines * 10 actions each)
	// Use mutex to safely access the field
	ctx.mu.RLock()
	dynamicCount := len(ctx.dynamicActions)
	ctx.mu.RUnlock()

	if dynamicCount != 100 {
		t.Errorf("Expected 100 dynamic actions from concurrent adds, got %d", dynamicCount)
	}
}

// Test that the mutex prevents race conditions
func TestActionMutationRaceCondition(t *testing.T) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	ctx := newActionContext(workflow)

	mutation := newActionMutation(ctx)

	var wg sync.WaitGroup
	iterations := 1000

	// Create a shared counter through enable/disable
	testAction := &mockAction{name: "racetest", description: "race test", tags: []string{"race"}}
	ctx.allActions = append(ctx.allActions, testAction)

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

// Test populateActions thread safety
func TestPopulateActionsConcurrent(t *testing.T) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	ctx := newActionContext(workflow)

	var wg sync.WaitGroup
	numGoroutines := 10

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			actions := []types.Action{
				&mockAction{name: "action" + string(rune('0'+id)), description: "test", tags: []string{"test"}},
			}
			ctx.populateActions(actions)
		}(i)
	}

	wg.Wait()

	// After concurrent populateActions, we should have the last set that was written
	if len(ctx.allActions) != 1 {
		t.Errorf("Expected 1 action after concurrent populateActions, got %d", len(ctx.allActions))
	}
}
