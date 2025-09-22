package runner

import (
	"sync"
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v2/types"
)

// mockBroker implements types.BrokerCall for testing
type mockBroker struct{}

func (m *mockBroker) Call(msgType types.MessageType, payload []byte) error { return nil }
func (m *mockBroker) Publish(key string, value []byte, metadata map[string]interface{}) error {
	return nil
}
func (m *mockBroker) Progress(percent int) error                      { return nil }
func (m *mockBroker) ProgressCause(message string, percent int) error { return nil }
func (m *mockBroker) Log(level types.LogLevel, message string, fields ...types.LogField) error {
	return nil
}

func TestContextBasicOperations(t *testing.T) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	broker := &mockBroker{}

	ctx := NewContext(workflow, broker)

	// Test context interface implementation
	if ctx == nil {
		t.Fatal("Expected NewContext to return non-nil context")
	}

	// Test deadline operations
	deadline := time.Now().Add(time.Hour)
	if ctxImpl, ok := ctx.(*contextImpl); ok {
		ctxImpl.SetDeadline(deadline)

		gotDeadline, hasDeadline := ctx.Deadline()
		if !hasDeadline {
			t.Error("Expected context to have deadline after SetDeadline")
		}
		if !gotDeadline.Equal(deadline) {
			t.Errorf("Expected deadline %v, got %v", deadline, gotDeadline)
		}
	} else {
		t.Fatal("Failed to cast context to contextImpl")
	}

	// Test Done channel
	done := ctx.Done()
	if done == nil {
		t.Error("Expected Done() to return non-nil channel")
	}

	// Test initial error state
	if err := ctx.Err(); err != nil {
		t.Errorf("Expected no error initially, got %v", err)
	}

	// Test Broker access
	if broker := ctx.Broker(); broker == nil {
		t.Error("Expected Broker() to return non-nil broker")
	}
}

func TestContextValueOperations(t *testing.T) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	broker := &mockBroker{}

	ctx := NewContext(workflow, broker)
	ctxImpl := ctx.(*contextImpl)

	// Test setting and getting values
	ctxImpl.SetValue("key1", "value1")
	ctxImpl.SetValue("key2", 42)
	ctxImpl.SetValue("key3", []string{"a", "b", "c"})

	// Test retrieval
	if val := ctx.Value("key1"); val != "value1" {
		t.Errorf("Expected 'value1', got %v", val)
	}

	if val := ctx.Value("key2"); val != 42 {
		t.Errorf("Expected 42, got %v", val)
	}

	if val := ctx.Value("key3"); val == nil {
		t.Error("Expected non-nil value for key3")
	} else if arr, ok := val.([]string); !ok || len(arr) != 3 {
		t.Errorf("Expected []string of length 3, got %v", val)
	}

	// Test non-existent key
	if val := ctx.Value("nonexistent"); val != nil {
		t.Errorf("Expected nil for non-existent key, got %v", val)
	}
}

func TestContextCancel(t *testing.T) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	broker := &mockBroker{}

	ctx := NewContext(workflow, broker)
	ctxImpl := ctx.(*contextImpl)

	// Setup a goroutine to wait on Done channel
	doneCalled := make(chan bool, 1)
	go func() {
		<-ctx.Done()
		doneCalled <- true
	}()

	// Cancel the context
	testErr := &customError{msg: "test cancellation"}
	ctxImpl.Cancel(testErr)

	// Wait for Done to be signaled
	select {
	case <-doneCalled:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Error("Done channel was not closed after Cancel")
	}

	// Verify error is set
	if err := ctx.Err(); err != testErr {
		t.Errorf("Expected error %v, got %v", testErr, err)
	}
}

type customError struct {
	msg string
}

func (e *customError) Error() string {
	return e.msg
}

func TestContextMutations(t *testing.T) {
	action1 := &mockAction{name: "action1", description: "test action 1", tags: []string{"test"}}
	stage1 := &mockStage{id: "stage1", name: "Stage 1", description: "test stage 1", tags: []string{"test"}}

	workflow := &mockWorkflow{stages: []types.Stage{stage1}}
	broker := &mockBroker{}

	ctx := NewContext(workflow, broker)

	// Get action mutation
	actionMutation := ctx.Actions()
	if actionMutation == nil {
		t.Fatal("Expected Actions() to return non-nil mutation")
	}

	// Test adding an action through context
	actionMutation.Add(action1)

	// Get stage mutation
	stageMutation := ctx.Stages()
	if stageMutation == nil {
		t.Fatal("Expected Stages() to return non-nil mutation")
	}

	// Test disabling a stage through context
	stageMutation.Disable("stage1")
	if stageMutation.IsEnabled("stage1") {
		t.Error("Expected stage1 to be disabled")
	}
}

// Test concurrent access to context values
func TestContextConcurrentValueAccess(t *testing.T) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	broker := &mockBroker{}

	ctx := NewContext(workflow, broker)
	ctxImpl := ctx.(*contextImpl)

	var wg sync.WaitGroup
	numGoroutines := 20
	iterations := 100

	// Concurrent writes
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := "key" + string(rune('0'+id))
				ctxImpl.SetValue(key, id*1000+j)
			}
		}(i)
	}

	// Concurrent reads
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := "key" + string(rune('0'+id%10))
				_ = ctx.Value(key)
			}
		}(i)
	}

	// Concurrent deadline operations
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			deadline := time.Now().Add(time.Duration(i) * time.Millisecond)
			ctxImpl.SetDeadline(deadline)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_, _ = ctx.Deadline()
		}
	}()

	wg.Wait()

	// Verify no panics occurred and data is consistent
	for i := 0; i < numGoroutines; i++ {
		key := "key" + string(rune('0'+i))
		val := ctx.Value(key)
		if val == nil {
			t.Errorf("Expected value for %s, got nil", key)
		}
	}
}

// Test that context properly isolates mutations
func TestContextMutationIsolation(t *testing.T) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	broker := &mockBroker{}

	ctx1 := NewContext(workflow, broker)
	ctx2 := NewContext(workflow, broker)

	// Each context should have its own action context
	action1 := &mockAction{name: "action1", description: "test action 1", tags: []string{"test"}}
	action2 := &mockAction{name: "action2", description: "test action 2", tags: []string{"test"}}

	// Add different actions to different contexts
	ctx1.Actions().Add(action1)
	ctx2.Actions().Add(action2)

	// Verify isolation
	ctx1Impl := ctx1.(*contextImpl)
	ctx2Impl := ctx2.(*contextImpl)

	if len(ctx1Impl.actionContext.dynamicActions) != 1 {
		t.Errorf("Expected ctx1 to have 1 dynamic action, got %d", len(ctx1Impl.actionContext.dynamicActions))
	}

	if len(ctx2Impl.actionContext.dynamicActions) != 1 {
		t.Errorf("Expected ctx2 to have 1 dynamic action, got %d", len(ctx2Impl.actionContext.dynamicActions))
	}

	// Verify they have different actions
	if ctx1Impl.actionContext.dynamicActions[0].Name() == ctx2Impl.actionContext.dynamicActions[0].Name() {
		t.Error("Expected contexts to have different actions")
	}
}

// Benchmark context value operations
func BenchmarkContextValueSet(b *testing.B) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	broker := &mockBroker{}
	ctx := NewContext(workflow, broker)
	ctxImpl := ctx.(*contextImpl)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctxImpl.SetValue("key", i)
	}
}

func BenchmarkContextValueGet(b *testing.B) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	broker := &mockBroker{}
	ctx := NewContext(workflow, broker)
	ctxImpl := ctx.(*contextImpl)

	ctxImpl.SetValue("key", "value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ctx.Value("key")
	}
}

func BenchmarkConcurrentValueAccess(b *testing.B) {
	workflow := &mockWorkflow{stages: []types.Stage{}}
	broker := &mockBroker{}
	ctx := NewContext(workflow, broker)
	ctxImpl := ctx.(*contextImpl)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				ctxImpl.SetValue("key", i)
			} else {
				_ = ctx.Value("key")
			}
			i++
		}
	})
}
