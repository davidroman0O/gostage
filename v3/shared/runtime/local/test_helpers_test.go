package local

import (
	rt "github.com/davidroman0O/gostage/v3/shared/runtime"
	store "github.com/davidroman0O/gostage/v3/shared/store"
)

type mockWorkflow struct {
	stages   []rt.Stage
	store    store.Handle
	metadata map[string]interface{}
}

func (m *mockWorkflow) ID() string                           { return "test-workflow" }
func (m *mockWorkflow) Name() string                         { return "Test Workflow" }
func (m *mockWorkflow) Description() string                  { return "Test workflow" }
func (m *mockWorkflow) Tags() []string                       { return []string{"test"} }
func (m *mockWorkflow) InitialStore() store.Handle           { return m.getStore() }
func (m *mockWorkflow) Store() store.Handle                  { return m.getStore() }
func (m *mockWorkflow) Stages() []rt.Stage                   { return m.stages }
func (m *mockWorkflow) Metadata() map[string]interface{}     { return m.getMetadata() }
func (m *mockWorkflow) Middlewares() []rt.WorkflowMiddleware { return nil }

func (m *mockWorkflow) getStore() store.Handle {
	if m.store.IsZero() {
		m.store = store.New()
	}
	return m.store
}

func (m *mockWorkflow) getMetadata() map[string]interface{} {
	if m.metadata == nil {
		m.metadata = make(map[string]interface{})
	}
	return m.metadata
}
