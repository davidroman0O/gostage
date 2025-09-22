package runner

import (
	"github.com/davidroman0O/gostage/store"
	"github.com/davidroman0O/gostage/v2/types"
)

// mockWorkflow implements types.Workflow for testing
type mockWorkflow struct {
	stages []types.Stage
}

func (m *mockWorkflow) ID() string                              { return "test-workflow" }
func (m *mockWorkflow) Name() string                            { return "Test Workflow" }
func (m *mockWorkflow) Description() string                     { return "Test workflow for unit tests" }
func (m *mockWorkflow) Tags() []string                          { return []string{"test"} }
func (m *mockWorkflow) InitialStore() *store.KVStore            { return store.NewKVStore() }
func (m *mockWorkflow) Stages() []types.Stage                   { return m.stages }
func (m *mockWorkflow) Metadata() map[string]interface{}        { return nil }
func (m *mockWorkflow) Middlewares() []types.WorkflowMiddleware { return nil }
