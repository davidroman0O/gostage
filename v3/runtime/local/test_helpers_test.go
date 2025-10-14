package local

import (
	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v3/types"
)

type mockWorkflow struct {
	stages   []types.Stage
	store    *store.KVStore
	metadata map[string]interface{}
}

func (m *mockWorkflow) ID() string                              { return "test-workflow" }
func (m *mockWorkflow) Name() string                            { return "Test Workflow" }
func (m *mockWorkflow) Description() string                     { return "Test workflow" }
func (m *mockWorkflow) Tags() []string                          { return []string{"test"} }
func (m *mockWorkflow) InitialStore() *store.KVStore            { return m.getStore() }
func (m *mockWorkflow) Store() *store.KVStore                   { return m.getStore() }
func (m *mockWorkflow) Stages() []types.Stage                   { return m.stages }
func (m *mockWorkflow) Metadata() map[string]interface{}        { return m.getMetadata() }
func (m *mockWorkflow) Middlewares() []types.WorkflowMiddleware { return nil }

func (m *mockWorkflow) getStore() *store.KVStore {
	if m.store == nil {
		m.store = store.NewKVStore()
	}
	return m.store
}

func (m *mockWorkflow) getMetadata() map[string]interface{} {
	if m.metadata == nil {
		m.metadata = make(map[string]interface{})
	}
	return m.metadata
}
