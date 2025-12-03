package local

import (
	"testing"

	rt "github.com/davidroman0O/gostage/v3/shared/runtime"
)

func TestContextStoreConvenienceMethods(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflow{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)

	// Test Put
	if err := ctx.Put("key1", "value1"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Test Has
	if !ctx.Has("key1") {
		t.Fatal("Has should return true for existing key")
	}
	if ctx.Has("nonexistent") {
		t.Fatal("Has should return false for non-existent key")
	}

	// Test Export
	exported := ctx.Export()
	if exported["key1"] != "value1" {
		t.Fatalf("Export: expected 'value1', got %v", exported["key1"])
	}

	// Test Delete
	if !ctx.Delete("key1") {
		t.Fatal("Delete should return true for existing key")
	}
	if ctx.Delete("nonexistent") {
		t.Fatal("Delete should return false for non-existent key")
	}
	if ctx.Has("key1") {
		t.Fatal("Has should return false after Delete")
	}
}

func TestQueryFindActionsByTag(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflowWithActions{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	actions := query.FindActionsByTag("important")
	if len(actions) == 0 {
		t.Fatal("expected to find actions with 'important' tag")
	}
	for _, action := range actions {
		found := false
		for _, tag := range action.Tags() {
			if tag == "important" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("action %s should have 'important' tag", action.Name())
		}
	}
}

func TestQueryFindActionsByTags(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflowWithActions{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	actions := query.FindActionsByTags([]string{"important", "critical"})
	if len(actions) == 0 {
		t.Fatal("expected to find actions with tags")
	}
}

func TestQueryFindStagesByTag(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflowWithStages{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	stages := query.FindStagesByTag("stage-tag")
	if len(stages) == 0 {
		t.Fatal("expected to find stages with tag")
	}
}

func TestQueryFilterActions(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflowWithActions{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	actions := query.FilterActions(func(action rt.Action) bool {
		return action.Name() == "test.action1"
	})
	if len(actions) != 1 {
		t.Fatalf("expected 1 action, got %d", len(actions))
	}
	if actions[0].Name() != "test.action1" {
		t.Fatalf("expected 'test.action1', got %s", actions[0].Name())
	}
}

func TestQueryFilterStages(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflowWithStages{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	stages := query.FilterStages(func(stage rt.Stage) bool {
		return stage.ID() == "stage1"
	})
	if len(stages) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(stages))
	}
}

func TestQueryListAllActions(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflowWithActions{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	actions := query.ListAllActions()
	if len(actions) == 0 {
		t.Fatal("expected to find actions")
	}
}

func TestQueryListAllStages(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflowWithStages{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	stages := query.ListAllStages()
	if len(stages) == 0 {
		t.Fatal("expected to find stages")
	}
}

func TestQueryGetString(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflow{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	// Put a string value
	if err := ctx.Put("str-key", "test-value"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, err := query.GetString("str-key")
	if err != nil {
		t.Fatalf("GetString: %v", err)
	}
	if val != "test-value" {
		t.Fatalf("expected 'test-value', got %q", val)
	}

	// Test non-existent key
	_, err = query.GetString("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
}

func TestQueryGetInt(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflow{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	if err := ctx.Put("int-key", 42); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, err := query.GetInt("int-key")
	if err != nil {
		t.Fatalf("GetInt: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestQueryGetBool(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflow{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	if err := ctx.Put("bool-key", true); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, err := query.GetBool("bool-key")
	if err != nil {
		t.Fatalf("GetBool: %v", err)
	}
	if !val {
		t.Fatal("expected true")
	}
}

func TestQueryGetStringOrDefault(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflow{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	// Test existing key
	if err := ctx.Put("str-key", "value"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val := query.GetStringOrDefault("str-key", "default")
	if val != "value" {
		t.Fatalf("expected 'value', got %q", val)
	}

	// Test non-existent key
	val = query.GetStringOrDefault("nonexistent", "default")
	if val != "default" {
		t.Fatalf("expected 'default', got %q", val)
	}
}

func TestQueryGetIntOrDefault(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflow{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	// Test existing key
	if err := ctx.Put("int-key", 100); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val := query.GetIntOrDefault("int-key", 0)
	if val != 100 {
		t.Fatalf("expected 100, got %d", val)
	}

	// Test non-existent key
	val = query.GetIntOrDefault("nonexistent", 42)
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestQueryGetBoolOrDefault(t *testing.T) {
	factory := Factory{}
	wf := &mockWorkflow{}
	raw := factory.New(wf, &testBroker{})
	ctx := raw.(*contextImpl)
	query := ctx.Query()

	// Test existing key
	if err := ctx.Put("bool-key", false); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val := query.GetBoolOrDefault("bool-key", true)
	if val {
		t.Fatal("expected false")
	}

	// Test non-existent key
	val = query.GetBoolOrDefault("nonexistent", true)
	if !val {
		t.Fatal("expected true (default)")
	}
}

// Mock workflows for testing
type mockWorkflowWithActions struct {
	mockWorkflow
}

func (m *mockWorkflowWithActions) Stages() []rt.Stage {
	return []rt.Stage{
		&mockStageWithActions{
			mockStage: mockStage{id: "stage1", tags: []string{"stage-tag"}},
		},
	}
}

type mockWorkflowWithStages struct {
	mockWorkflow
}

func (m *mockWorkflowWithStages) Stages() []rt.Stage {
	return []rt.Stage{
		&mockStage{id: "stage1", tags: []string{"stage-tag"}},
		&mockStage{id: "stage2", tags: []string{"other"}},
	}
}

type mockStageWithActions struct {
	mockStage
}

func (s *mockStageWithActions) ActionList() []rt.Action {
	return []rt.Action{
		&mockAction{name: "test.action1", tags: []string{"important"}},
		&mockAction{name: "test.action2", tags: []string{"critical"}},
	}
}
