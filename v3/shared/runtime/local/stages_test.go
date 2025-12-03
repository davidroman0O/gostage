package local

import (
	"testing"

	rt "github.com/davidroman0O/gostage/v3/shared/runtime"
	store "github.com/davidroman0O/gostage/v3/shared/store"
)

type mockStage struct {
	id   string
	tags []string
}

func (m *mockStage) ID() string                        { return m.id }
func (m *mockStage) Name() string                      { return m.id }
func (m *mockStage) Description() string               { return m.id }
func (m *mockStage) Actions() rt.ActionMutation        { return nil }
func (m *mockStage) ActionList() []rt.Action           { return nil }
func (m *mockStage) Tags() []string                    { return m.tags }
func (m *mockStage) InitialStore() store.Handle        { return store.New() }
func (m *mockStage) Middlewares() []rt.StageMiddleware { return nil }
func (m *mockStage) ActionMiddlewares() []rt.ActionMiddleware {
	return nil
}

func TestStageMutation_AddDynamic(t *testing.T) {
	workflow := &mockWorkflow{stages: []rt.Stage{}}
	ctx := newActionContext(workflow)
	mutation := newStageMutation(ctx)

	mutation.Add(&mockStage{id: "stage1"})

	if len(ctx.dynamicStages) != 1 {
		t.Fatalf("expected 1 dynamic stage, got %d", len(ctx.dynamicStages))
	}
}

func TestStageMutation_DisableEnable(t *testing.T) {
	stage := &mockStage{id: "stage1", tags: []string{"test"}}
	workflow := &mockWorkflow{stages: []rt.Stage{stage}}
	ctx := newActionContext(workflow)
	mutation := newStageMutation(ctx)

	mutation.Disable("stage1")
	if mutation.IsEnabled("stage1") {
		t.Fatalf("expected disabled stage")
	}

	mutation.Enable("stage1")
	if !mutation.IsEnabled("stage1") {
		t.Fatalf("expected enabled stage")
	}
}
