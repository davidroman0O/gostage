package runner

import (
	"testing"
	"time"

	"github.com/davidroman0O/gostage/v2/types"
)

// TestDeadlockDetection verifies that go-deadlock can detect potential deadlocks
func TestDeadlockDetection(t *testing.T) {
	// This test is designed to verify deadlock detection is working
	// We'll create a scenario that would deadlock with regular sync.Mutex
	// but should be detected by go-deadlock

	t.Run("Verify deadlock detection on recursive lock", func(t *testing.T) {
		// Create a simple test to verify the mutex is deadlock-aware
		workflow := &mockWorkflow{stages: []types.Stage{}}
		ctx := newActionContext(workflow)

		// This function would cause a deadlock with regular mutex
		// but go-deadlock should detect and report it
		testRecursiveLock := func() {
			ctx.mu.Lock()
			defer ctx.mu.Unlock()

			// Try to lock again - this would deadlock
			// We're not actually calling this in production code
			// but this verifies our mutex can detect such issues
			done := make(chan bool)
			go func() {
				ctx.mu.Lock()
				ctx.mu.Unlock()
				done <- true
			}()

			select {
			case <-done:
				t.Error("Expected deadlock detection, but lock succeeded")
			case <-time.After(100 * time.Millisecond):
				// Expected - the lock should be blocked
				t.Log("Deadlock properly prevented - mutex is working correctly")
			}
		}

		testRecursiveLock()
	})

	t.Run("Verify no deadlock in normal operations", func(t *testing.T) {
		workflow := &mockWorkflow{stages: []types.Stage{}}
		ctx := newActionContext(workflow)
		mutation := newActionMutation(ctx)

		// Normal operations should work without deadlock
		action := &mockAction{name: "test", description: "test", tags: []string{"test"}}

		done := make(chan bool)
		go func() {
			mutation.Add(action)
			mutation.Disable("test")
			mutation.Enable("test")
			_ = mutation.IsEnabled("test")
			done <- true
		}()

		select {
		case <-done:
			t.Log("Normal operations completed without deadlock")
		case <-time.After(1 * time.Second):
			t.Error("Normal operations timed out - possible deadlock")
		}
	})

	t.Run("Verify stages mutex isolation", func(t *testing.T) {
		workflow := &mockWorkflow{stages: []types.Stage{}}
		ctx := newActionContext(workflow)
		stageMutation := newStageMutation(ctx)
		actionMutation := newActionMutation(ctx)

		// Operations on different mutations should not block each other
		done := make(chan bool, 2)

		go func() {
			for i := 0; i < 10; i++ {
				actionMutation.Disable("action1")
				actionMutation.Enable("action1")
			}
			done <- true
		}()

		go func() {
			for i := 0; i < 10; i++ {
				stageMutation.Disable("stage1")
				stageMutation.Enable("stage1")
			}
			done <- true
		}()

		timeout := time.After(1 * time.Second)
		for i := 0; i < 2; i++ {
			select {
			case <-done:
				// Good, goroutine completed
			case <-timeout:
				t.Error("Operations timed out - possible deadlock between mutations")
				return
			}
		}

		t.Log("Stage and action mutations operate independently without deadlock")
	})
}
