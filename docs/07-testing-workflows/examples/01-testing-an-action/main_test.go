package examples

import (
	"context"
	"fmt"
	"testing"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
	"github.com/stretchr/testify/assert"
)

// The action we want to test.
// It reads two numbers from the store, adds them, and writes the result back.
type SumAction struct {
	gostage.BaseAction
}

func (a *SumAction) Execute(ctx *gostage.ActionContext) error {
	valA, err := store.Get[int](ctx.Store(), "a")
	if err != nil {
		return fmt.Errorf("could not get 'a' from store: %w", err)
	}

	valB, err := store.Get[int](ctx.Store(), "b")
	if err != nil {
		return fmt.Errorf("could not get 'b' from store: %w", err)
	}

	sum := valA + valB
	return ctx.Store().Put("sum", sum)
}

// TestSumAction is a unit test for the SumAction.
func TestSumAction(t *testing.T) {
	// 1. Create a test workflow to hold the store.
	//    We need this to provide a valid store to the action context.
	testWorkflow := gostage.NewWorkflow("test-wf-for-action", "Test Workflow", "")

	// 2. Populate the store with the input data our action needs.
	testWorkflow.Store.Put("a", 10)
	testWorkflow.Store.Put("b", 5)

	// 3. Instantiate the action we want to test.
	action := &SumAction{
		BaseAction: gostage.NewBaseAction("sum-action", "Sums two numbers"),
	}

	// 4. Create a mock ActionContext.
	//    This provides the action with its required environment in a controlled way.
	actionCtx := &gostage.ActionContext{
		GoContext: context.Background(),
		Workflow:  testWorkflow,
		Logger:    nil, // We don't need a real logger for this test.
	}

	// 5. Execute the action's logic directly.
	err := action.Execute(actionCtx)

	// 6. Assert the results.
	//    a. The action should not have returned an error.
	assert.NoError(t, err)

	//    b. The store should now contain the correct result.
	sum, getErr := store.Get[int](testWorkflow.Store, "sum")
	assert.NoError(t, getErr, "Should be able to get the sum from the store")
	assert.Equal(t, 15, sum, "The sum of 10 and 5 should be 15")
}
