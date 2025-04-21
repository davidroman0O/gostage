package gostage

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/davidroman0O/gostage/store"
	"github.com/stretchr/testify/assert"
)

// TestDynamicActionEdgeCases tests edge cases related to dynamic action creation
func TestDynamicActionEdgeCases(t *testing.T) {
	// Test adding multiple dynamic actions with the same name
	t.Run("duplicate_dynamic_action_names", func(t *testing.T) {
		wf := NewWorkflow("duplicate-actions", "Duplicate Dynamic Actions", "Testing duplicate dynamic action names")
		stage := NewStage("test-stage", "Test Stage", "A stage for testing")

		// This action creates two dynamic actions with the same name
		createDuplicateActions := NewTestAction("create-duplicates", "Creates duplicate dynamic actions",
			func(ctx *ActionContext) error {
				dynAction1 := NewTestAction("dynamic-action", "First dynamic action", func(c *ActionContext) error {
					c.Logger.Info("First dynamic action executed")
					return nil
				})
				ctx.AddDynamicAction(dynAction1)

				dynAction2 := NewTestAction("dynamic-action", "Second dynamic action with same name", func(c *ActionContext) error {
					c.Logger.Info("Second dynamic action executed")
					return nil
				})
				ctx.AddDynamicAction(dynAction2)

				return nil
			})

		stage.AddAction(createDuplicateActions)
		wf.AddStage(stage)

		// Execute workflow
		logger := &TestLogger{t: t}
		runner := NewRunner()
		err := runner.Execute(context.Background(), wf, logger)

		// Should execute without errors - both actions should execute despite having the same name
		assert.NoError(t, err)

		// The stage should now have 3 actions (original + 2 dynamic)
		assert.Equal(t, 3, len(stage.Actions))
	})

	// Test actions that add their own dynamic actions (cascading dynamic actions)
	t.Run("cascading_dynamic_actions", func(t *testing.T) {
		wf := NewWorkflow("cascading-actions", "Cascading Dynamic Actions", "Testing cascading dynamic action creation")
		stage := NewStage("test-stage", "Test Stage", "A stage for testing")

		executionOrder := []string{}

		// Initial action that adds a dynamic action
		createFirstAction := NewTestAction("level-1", "First level action",
			func(ctx *ActionContext) error {
				executionOrder = append(executionOrder, "level-1")

				// Create a dynamic action that itself creates a dynamic action
				dynAction := NewTestAction("level-2", "Second level action", func(c *ActionContext) error {
					executionOrder = append(executionOrder, "level-2")

					// This dynamic action creates another dynamic action
					childAction := NewTestAction("level-3", "Third level action", func(cc *ActionContext) error {
						executionOrder = append(executionOrder, "level-3")
						return nil
					})

					c.AddDynamicAction(childAction)
					return nil
				})

				ctx.AddDynamicAction(dynAction)
				return nil
			})

		stage.AddAction(createFirstAction)
		wf.AddStage(stage)

		// Execute workflow
		logger := &TestLogger{t: t}
		runner := NewRunner()
		err := runner.Execute(context.Background(), wf, logger)

		// Should execute without errors
		assert.NoError(t, err)

		// Check that all actions executed in the expected order
		assert.Equal(t, []string{"level-1", "level-2", "level-3"}, executionOrder)

		// The stage should now have 3 actions
		assert.Equal(t, 3, len(stage.Actions))
	})

	// Test creating a large number of dynamic actions
	t.Run("large_number_of_dynamic_actions", func(t *testing.T) {
		wf := NewWorkflow("many-actions", "Many Dynamic Actions", "Testing large number of dynamic actions")
		stage := NewStage("test-stage", "Test Stage", "A stage for testing")

		// Configure how many actions to create
		const numActions = 1000
		executedActions := 0

		// This action creates many dynamic actions
		createManyActions := NewTestAction("create-many", "Creates many dynamic actions",
			func(ctx *ActionContext) error {
				executedActions++

				// Create many dynamic actions
				for i := 0; i < numActions; i++ {
					actionName := fmt.Sprintf("dynamic-action-%d", i)
					dynAction := NewTestAction(actionName, fmt.Sprintf("Dynamic action %d", i), func(c *ActionContext) error {
						executedActions++
						return nil
					})
					ctx.AddDynamicAction(dynAction)
				}

				return nil
			})

		stage.AddAction(createManyActions)
		wf.AddStage(stage)

		// Execute workflow
		logger := &TestLogger{t: t}
		runner := NewRunner()
		err := runner.Execute(context.Background(), wf, logger)

		// Should execute without errors
		assert.NoError(t, err)

		// All actions should have executed
		assert.Equal(t, numActions+1, executedActions)

		// The stage should now have all the actions
		assert.Equal(t, numActions+1, len(stage.Actions))
	})
}

// TestNestedErrorWrapping tests error propagation through nested actions
func TestNestedErrorWrapping(t *testing.T) {
	t.Run("error_propagation_in_nested_actions", func(t *testing.T) {
		wf := NewWorkflow("error-propagation", "Error Propagation", "Testing error propagation")
		stage := NewStage("test-stage", "Test Stage", "A stage for testing")

		// Create custom nested errors
		innerError := errors.New("inner error")

		// Create a triple-nested action structure
		innerAction := NewTestAction("inner-action", "Inner action that fails",
			func(ctx *ActionContext) error {
				return innerError
			})

		middleAction := NewTestAction("middle-action", "Middle action with nested action",
			func(ctx *ActionContext) error {
				err := innerAction.Execute(ctx)
				if err != nil {
					return fmt.Errorf("middle error: %w", err)
				}
				return nil
			})

		outerAction := NewTestAction("outer-action", "Outer action with nested action",
			func(ctx *ActionContext) error {
				err := middleAction.Execute(ctx)
				if err != nil {
					return fmt.Errorf("outer error: %w", err)
				}
				return nil
			})

		stage.AddAction(outerAction)
		wf.AddStage(stage)

		// Execute workflow
		logger := &TestLogger{t: t}
		runner := NewRunner()
		err := runner.Execute(context.Background(), wf, logger)

		// Should fail with error
		assert.Error(t, err)

		// Since the actual error might be wrapped in workflow-specific errors,
		// we check that the original error can be found in the error chain
		assert.Contains(t, err.Error(), "inner error")

		// Check the error message contains all parts
		assert.Contains(t, err.Error(), "inner error")
		assert.Contains(t, err.Error(), "middle error")
		assert.Contains(t, err.Error(), "outer error")
	})

	// Test error recovery in a stage
	t.Run("partial_failure_recovery", func(t *testing.T) {
		wf := NewWorkflow("partial-failure", "Partial Failure", "Testing recovery from partial failures")
		stage := NewStage("test-stage", "Test Stage", "A stage for testing")

		executedActions := []string{}

		// First action that will succeed
		action1 := NewTestAction("action1", "First action",
			func(ctx *ActionContext) error {
				executedActions = append(executedActions, "action1")
				return nil
			})

		// Second action that will fail
		action2 := NewTestAction("action2", "Second action that fails",
			func(ctx *ActionContext) error {
				executedActions = append(executedActions, "action2")
				return errors.New("action2 failed")
			})

		// Third action that should never execute due to second action's failure
		action3 := NewTestAction("action3", "Third action",
			func(ctx *ActionContext) error {
				executedActions = append(executedActions, "action3")
				return nil
			})

		stage.AddAction(action1)
		stage.AddAction(action2)
		stage.AddAction(action3)
		wf.AddStage(stage)

		// Execute workflow
		logger := &TestLogger{t: t}
		runner := NewRunner()
		err := runner.Execute(context.Background(), wf, logger)

		// Should fail with error from action2
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "action2 failed")

		// Only first two actions should have executed
		assert.Equal(t, []string{"action1", "action2"}, executedActions)
	})
}

// TestWorkflowRaces tests for race conditions in the workflow
func TestWorkflowRaces(t *testing.T) {
	t.Run("concurrent_store_operations", func(t *testing.T) {
		s := store.NewKVStore()

		var wg sync.WaitGroup
		// Spawn multiple goroutines to read/write to the store concurrently
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				// Do various operations
				key := fmt.Sprintf("key-%d", id)

				// Put
				err := s.Put(key, id)
				assert.NoError(t, err)

				// Get
				val, err := store.Get[int](s, key)
				assert.NoError(t, err)
				assert.Equal(t, id, val)

				// Update
				err = s.Put(key, id*2)
				assert.NoError(t, err)

				// Add metadata
				meta := store.NewMetadata()
				meta.AddTag(fmt.Sprintf("tag-%d", id))
				err = s.SetMetadata(key, meta)
				assert.NoError(t, err)

				// Get tags
				keysWithTag := s.FindKeysByTag(fmt.Sprintf("tag-%d", id))
				assert.Contains(t, keysWithTag, key)

				// Delete
				if id%2 == 0 {
					deleted := s.Delete(key)
					assert.True(t, deleted)
				}
			}(i)
		}
		wg.Wait()

		// Check final state
		count := s.Count()
		assert.Equal(t, 50, count) // Only odd-numbered keys should remain
	})

	t.Run("concurrent_workflow_operations", func(t *testing.T) {
		wf := NewWorkflow("race-test", "Race Test", "Tests for race conditions")
		stage := NewStage("test-stage", "Test Stage", "A stage for testing")

		// Add some initial actions
		for i := 0; i < 10; i++ {
			action := NewTestAction(fmt.Sprintf("action-%d", i), fmt.Sprintf("Action %d", i),
				func(ctx *ActionContext) error {
					return nil
				})
			stage.AddAction(action)
		}

		wf.AddStage(stage)

		// Run operations concurrently
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				actionCtx := &ActionContext{
					GoContext:       context.Background(),
					Workflow:        wf,
					Stage:           stage,
					Store:           wf.Store,
					Logger:          &TestLogger{t: t},
					disabledActions: make(map[string]bool),
					disabledStages:  make(map[string]bool),
				}

				// Enable/disable actions
				for j := 0; j < 10; j++ {
					if (j+id)%2 == 0 {
						actionCtx.DisableAction(fmt.Sprintf("action-%d", j))
					} else {
						actionCtx.EnableAction(fmt.Sprintf("action-%d", j))
					}
				}

				// Check action states
				states := actionCtx.GetActionStates(stage.ID)
				assert.Equal(t, 10, len(states))

				// Filter actions
				filteredActions := actionCtx.FilterActions(func(a Action) bool {
					return a.Name() != fmt.Sprintf("action-%d", id)
				})
				assert.Equal(t, 9, len(filteredActions))
			}(i)
		}
		wg.Wait()
	})
}

// TestMemoryUsage tests for memory leaks in long-running workflows
func TestMemoryUsage(t *testing.T) {
	t.Run("repeated_workflow_execution", func(t *testing.T) {
		// Skip during short tests
		if testing.Short() {
			t.Skip("Skipping memory test in short mode")
		}

		// This test creates and runs many workflows to check for memory growth
		createWorkflow := func() *Workflow {
			wf := NewWorkflow("memory-test", "Memory Test", "Test for memory leaks")

			for i := 0; i < 5; i++ {
				stage := NewStage(fmt.Sprintf("stage-%d", i), fmt.Sprintf("Stage %d", i), "A test stage")

				for j := 0; j < 10; j++ {
					action := NewTestAction(fmt.Sprintf("action-%d-%d", i, j),
						fmt.Sprintf("Action %d in Stage %d", j, i),
						func(ctx *ActionContext) error {
							// Create some data in the store
							ctx.Store.Put(fmt.Sprintf("key-%d-%d", i, j), fmt.Sprintf("value-%d-%d", i, j))
							return nil
						})
					stage.AddAction(action)
				}

				wf.AddStage(stage)
			}

			return wf
		}

		// Get initial memory stats
		var m1, m2 runtime.MemStats
		runtime.ReadMemStats(&m1)

		// Execute workflows in a loop
		for i := 0; i < 100; i++ {
			wf := createWorkflow()
			runner := NewRunner()
			err := runner.Execute(context.Background(), wf, NewDefaultLogger())
			assert.NoError(t, err)

			// Force garbage collection
			runtime.GC()
		}

		// Get final memory stats
		runtime.ReadMemStats(&m2)

		// Check memory growth - some growth is expected, but it should be reasonable
		// This is not a strict test as GC behavior varies
		t.Logf("Initial heap: %d bytes, Final heap: %d bytes, Growth: %d bytes",
			m1.HeapAlloc, m2.HeapAlloc, int64(m2.HeapAlloc)-int64(m1.HeapAlloc))

		// A more reliable test would use a profiler to detect leaks
	})
}

// Helper method to trigger GC and get memory usage
func getMemUsage() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}
