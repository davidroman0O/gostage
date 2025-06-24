package examples

import (
	"context"
	"fmt"
	"testing"

	"github.com/davidroman0O/gostage"
	"github.com/davidroman0O/gostage/store"
	"github.com/stretchr/testify/assert"
)

// --- Actions for the Stage ---

// SetInitialValueAction sets a number in the store.
type SetInitialValueAction struct {
	gostage.BaseAction
}

func (a *SetInitialValueAction) Execute(ctx *gostage.ActionContext) error {
	return ctx.Store().Put("number", 10)
}

// DoubleValueAction reads the number, doubles it, and saves it back.
type DoubleValueAction struct {
	gostage.BaseAction
}

func (a *DoubleValueAction) Execute(ctx *gostage.ActionContext) error {
	val, err := store.Get[int](ctx.Store(), "number")
	if err != nil {
		return fmt.Errorf("number not found in store")
	}
	return ctx.Store().Put("number", val*2)
}

// AddTenAction reads the number, adds 10, and saves it back.
type AddTenAction struct {
	gostage.BaseAction
}

func (a *AddTenAction) Execute(ctx *gostage.ActionContext) error {
	val, err := store.Get[int](ctx.Store(), "number")
	if err != nil {
		return fmt.Errorf("number not found in store")
	}
	return ctx.Store().Put("number", val+10)
}

// --- Integration Test for the Stage ---

func TestProcessingStage(t *testing.T) {
	// 1. Create a test workflow. This will be our test harness.
	testWorkflow := gostage.NewWorkflow("stage-test-wf", "", "")

	// 2. Create and configure the stage we want to test.
	//    This stage includes a sequence of actions that depend on each other.
	processingStage := gostage.NewStage("processing-stage", "", "")
	processingStage.AddAction(&SetInitialValueAction{BaseAction: gostage.NewBaseAction("set", "")})
	processingStage.AddAction(&DoubleValueAction{BaseAction: gostage.NewBaseAction("double", "")})
	processingStage.AddAction(&AddTenAction{BaseAction: gostage.NewBaseAction("add", "")})

	// 3. Add the single stage to our test workflow.
	testWorkflow.AddStage(processingStage)

	// 4. Use the standard Runner to execute the workflow.
	//    This ensures the stage is run in a realistic environment.
	runner := gostage.NewRunner()
	err := runner.Execute(context.Background(), testWorkflow, nil)

	// 5. Assert the results.
	//    a. The stage should execute without any errors.
	assert.NoError(t, err)

	//    b. Check the final state of the store to verify the actions interacted correctly.
	//       Initial: 10
	//       After Double: 20
	//       After AddTen: 30
	finalValue, getErr := store.Get[int](testWorkflow.Store, "number")
	assert.NoError(t, getErr)
	assert.Equal(t, 30, finalValue, "The final value should be (10 * 2) + 10 = 30")
}
