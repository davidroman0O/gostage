package local

import (
	"testing"

	"github.com/davidroman0O/gostage/v2/types"
)

type mockAction struct {
	name        string
	description string
	tags        []string
}

func (m *mockAction) Name() string                    { return m.name }
func (m *mockAction) Description() string             { return m.description }
func (m *mockAction) Tags() []string                  { return m.tags }
func (m *mockAction) Execute(ctx types.Context) error { return nil }

func TestActionMutation_Add(t *testing.T) {
	ctx := newActionContext(&mockWorkflow{})
	mutation := newActionMutation(ctx)

	action1 := &mockAction{name: "action1"}
	action2 := &mockAction{name: "action2"}

	mutation.Add(action1)
	mutation.Add(action2)

	if len(ctx.dynamicActions) != 2 {
		t.Fatalf("expected 2 dynamic actions, got %d", len(ctx.dynamicActions))
	}
}

func TestActionMutation_DisableEnable(t *testing.T) {
	action1 := &mockAction{name: "action1", tags: []string{"test"}}
	action2 := &mockAction{name: "action2", tags: []string{"test"}}

	ctx := newActionContext(&mockWorkflow{})
	ctx.populateActions([]types.Action{action1, action2})

	mutation := newActionMutation(ctx)

	mutation.Disable("action1")
	if mutation.IsEnabled("action1") {
		t.Fatalf("expected action1 disabled")
	}

	mutation.Enable("action1")
	if !mutation.IsEnabled("action1") {
		t.Fatalf("expected action1 enabled")
	}
}

func TestActionMutation_DisableByTags(t *testing.T) {
	action1 := &mockAction{name: "action1", tags: []string{"t"}}
	action2 := &mockAction{name: "action2", tags: []string{"t", "x"}}

	ctx := newActionContext(&mockWorkflow{})
	ctx.populateActions([]types.Action{action1, action2})

	mutation := newActionMutation(ctx)
	count := mutation.DisableByTags([]string{"t"})

	if count != 2 {
		t.Fatalf("expected 2 disabled, got %d", count)
	}
	if mutation.IsEnabled("action1") || mutation.IsEnabled("action2") {
		t.Fatalf("expected actions disabled")
	}
}
