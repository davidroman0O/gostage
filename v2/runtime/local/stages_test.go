package local

import (
	"testing"

	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v2/types"
)

type mockStage struct {
	id   string
	tags []string
}

func (m *mockStage) ID() string                           { return m.id }
func (m *mockStage) Name() string                         { return m.id }
func (m *mockStage) Description() string                  { return m.id }
func (m *mockStage) Actions() types.ActionMutation        { return nil }
func (m *mockStage) ActionList() []types.Action           { return nil }
func (m *mockStage) Tags() []string                       { return m.tags }
func (m *mockStage) InitialStore() *store.KVStore         { return store.NewKVStore() }
func (m *mockStage) Middlewares() []types.StageMiddleware { return nil }

func TestStageMutation_AddDynamic(t *testing.T) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	ctx := newActionContext(workflow)
	mutation := newStageMutation(ctx)

	mutation.Add(&mockStage{id: "stage1"})

	if len(ctx.dynamicStages) != 1 {
		t.Fatalf("expected 1 dynamic stage, got %d", len(ctx.dynamicStages))
	}
}

func TestStageMutation_DisableEnable(t *testing.T) {
	stage := &mockStage{id: "stage1", tags: []string{"test"}}
	workflow := &mockWorkflow{stages: []types.Stage{stage}}
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
