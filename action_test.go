package gostate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// CustomError is a simple error type for testing
type CustomError struct {
	Code    string
	Message string
	Cause   error
}

func (e *CustomError) Error() string {
	msg := fmt.Sprintf("[%s] %s", e.Code, e.Message)
	if e.Cause != nil {
		msg += fmt.Sprintf(" (caused by: %s)", e.Cause.Error())
	}
	return msg
}

// CompleteAction is an Action implementation used for testing
type CompleteAction struct {
	BaseAction
}

// Execute implements the Action interface for CompleteAction
func (a CompleteAction) Execute(ctx *ActionContext) error {
	return nil
}

func TestBaseActionCreation(t *testing.T) {
	// Test simple constructor
	baseAction := NewBaseAction("test-action", "Test Action")
	assert.Equal(t, "test-action", baseAction.name)
	assert.Equal(t, "Test Action", baseAction.description)
	assert.Equal(t, 0, len(baseAction.tags))

	// Test constructor with tags
	tags := []string{"tag1", "tag2"}
	taggedAction := NewBaseActionWithTags("tagged-action", "Tagged Action", tags)
	assert.Equal(t, "tagged-action", taggedAction.name)
	assert.Equal(t, "Tagged Action", taggedAction.description)
	assert.Equal(t, 2, len(taggedAction.tags))
	assert.Contains(t, taggedAction.tags, "tag1")
	assert.Contains(t, taggedAction.tags, "tag2")
}

func TestBaseActionGetters(t *testing.T) {
	// Create a base action
	baseAction := NewBaseAction("test-action", "Test Action")

	// Test getters
	assert.Equal(t, "test-action", baseAction.Name())
	assert.Equal(t, "Test Action", baseAction.Description())
	assert.Equal(t, 0, len(baseAction.Tags()))

	// Tagged action
	taggedAction := NewBaseActionWithTags("tagged-action", "Tagged Action", []string{"tag1", "tag2"})
	assert.Equal(t, "tagged-action", taggedAction.Name())
	assert.Equal(t, "Tagged Action", taggedAction.Description())
	assert.Equal(t, 2, len(taggedAction.Tags()))
}

func TestBaseActionImplementation(t *testing.T) {
	// Create an instance of CompleteAction
	action := CompleteAction{
		BaseAction: NewBaseAction("test-action", "Test Action"),
	}

	// Create context and execute
	ctx := &ActionContext{
		Logger: &TestLogger{t: t},
	}

	err := action.Execute(ctx)
	assert.NoError(t, err)
}

func TestActionWithExecutor(t *testing.T) {
	// Test execution through a custom action that embeds BaseAction
	executed := false
	action := &TestAction{
		BaseAction: NewBaseAction("test-action", "Test Action"),
		executeFunc: func(ctx *ActionContext) error {
			executed = true
			return nil
		},
	}

	// Create context and execute
	ctx := &ActionContext{
		Logger: &TestLogger{t: t},
	}

	err := action.Execute(ctx)
	assert.NoError(t, err)
	assert.True(t, executed, "Execute function should have been called")
}

func TestActionWithError(t *testing.T) {
	// Test an action that returns an error
	expectedErr := "execution failed"
	action := &TestAction{
		BaseAction: NewBaseAction("error-action", "Error Action"),
		executeFunc: func(ctx *ActionContext) error {
			return &CustomError{
				Code:    "ERR_EXEC",
				Message: expectedErr,
			}
		},
	}

	// Create context and execute
	ctx := &ActionContext{
		Logger: &TestLogger{t: t},
	}

	err := action.Execute(ctx)
	assert.Error(t, err)

	// Check if we can cast to CustomError and get the details
	if customErr, ok := err.(*CustomError); ok {
		assert.Equal(t, "ERR_EXEC", customErr.Code)
		assert.Equal(t, expectedErr, customErr.Message)
	} else {
		t.Fatalf("Expected a CustomError, got %T", err)
	}
}

func TestActionTagsManagement(t *testing.T) {
	// Create a base action with initial tags
	action := NewBaseActionWithTags("tagged-action", "Tagged Action", []string{"tag1", "tag2"})

	// Check initial tags
	assert.Equal(t, 2, len(action.Tags()))
	assert.Contains(t, action.Tags(), "tag1")
	assert.Contains(t, action.Tags(), "tag2")

	// Create a test action that can manage its tags
	testAction := &TestAction{
		BaseAction:  action,
		customTags:  action.tags,
		executeFunc: nil,
	}

	// Add a new tag
	testAction.AddTag("tag3")
	assert.Equal(t, 3, len(testAction.Tags()))
	assert.Contains(t, testAction.Tags(), "tag3")

	// Test having empty customTags but base tags
	emptyTagsAction := &TestAction{
		BaseAction:  action,
		customTags:  []string{},
		executeFunc: nil,
	}

	// Should fall back to base tags
	assert.Equal(t, 2, len(emptyTagsAction.Tags()))
}

func TestNestedActionExecution(t *testing.T) {
	// Create a nested action structure (action within action)
	innerExecuted := false
	outerExecuted := false

	innerAction := &TestAction{
		BaseAction: NewBaseAction("inner-action", "Inner Action"),
		executeFunc: func(ctx *ActionContext) error {
			innerExecuted = true
			return nil
		},
	}

	outerAction := &TestAction{
		BaseAction: NewBaseAction("outer-action", "Outer Action"),
		executeFunc: func(ctx *ActionContext) error {
			// Execute the inner action from the outer one
			outerExecuted = true
			return innerAction.Execute(ctx)
		},
	}

	// Create context and execute the outer action
	ctx := &ActionContext{
		Logger: &TestLogger{t: t},
	}

	err := outerAction.Execute(ctx)
	assert.NoError(t, err)
	assert.True(t, outerExecuted, "Outer action should have executed")
	assert.True(t, innerExecuted, "Inner action should have executed")
}

func TestActionErrorHandling(t *testing.T) {
	// Create an action with error handling
	innerAction := &TestAction{
		BaseAction: NewBaseAction("error-action", "Error Action"),
		executeFunc: func(ctx *ActionContext) error {
			return &CustomError{
				Code:    "INNER_ERROR",
				Message: "Inner execution failed",
			}
		},
	}

	// Create an action that catches and transforms the error
	handlingAction := &TestAction{
		BaseAction: NewBaseAction("handling-action", "Error Handling Action"),
		executeFunc: func(ctx *ActionContext) error {
			err := innerAction.Execute(ctx)
			if err != nil {
				// Transform the error
				return &CustomError{
					Code:    "TRANSFORMED",
					Message: "Transformed error: " + err.Error(),
					Cause:   err,
				}
			}
			return nil
		},
	}

	// Create context and execute
	ctx := &ActionContext{
		Logger: &TestLogger{t: t},
	}

	err := handlingAction.Execute(ctx)
	assert.Error(t, err)

	// Verify error transformation
	if customErr, ok := err.(*CustomError); ok {
		assert.Equal(t, "TRANSFORMED", customErr.Code)
		assert.Contains(t, customErr.Message, "Transformed error")
		assert.NotNil(t, customErr.Cause, "Should have cause field set")

		// Check the inner error
		if causeErr, ok := customErr.Cause.(*CustomError); ok {
			assert.Equal(t, "INNER_ERROR", causeErr.Code)
		} else {
			t.Fatalf("Expected a CustomError cause, got %T", customErr.Cause)
		}
	} else {
		t.Fatalf("Expected a CustomError, got %T", err)
	}
}

func TestActionExecution(t *testing.T) {
	// Create an action that runs in phases
	var executionPhase string
	phaseAction := &TestAction{
		BaseAction: NewBaseAction("phase-action", "Phase-based Action"),
		executeFunc: func(ctx *ActionContext) error {
			// First set the phase to "running"
			executionPhase = "running"

			// Then set it to "completed"
			executionPhase = "completed"
			return nil
		},
	}

	// Create context and execute
	ctx := &ActionContext{
		Logger: &TestLogger{t: t},
	}

	err := phaseAction.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "completed", executionPhase, "Action should have completed execution")
}

// TestCompositeAction tests an action that executes multiple child actions
func TestCompositeAction(t *testing.T) {
	// Track execution of child actions
	actionExecutions := make(map[string]bool)

	// Create child actions
	action1 := &TestAction{
		BaseAction: NewBaseAction("action1", "Action 1"),
		executeFunc: func(ctx *ActionContext) error {
			actionExecutions["action1"] = true
			return nil
		},
	}

	action2 := &TestAction{
		BaseAction: NewBaseAction("action2", "Action 2"),
		executeFunc: func(ctx *ActionContext) error {
			actionExecutions["action2"] = true
			return nil
		},
	}

	action3 := &TestAction{
		BaseAction: NewBaseAction("action3", "Action 3"),
		executeFunc: func(ctx *ActionContext) error {
			actionExecutions["action3"] = true
			return nil
		},
	}

	// Create a composite action that executes all three
	compositeAction := &TestAction{
		BaseAction: NewBaseAction("composite", "Composite Action"),
		executeFunc: func(ctx *ActionContext) error {
			err := action1.Execute(ctx)
			if err != nil {
				return err
			}

			err = action2.Execute(ctx)
			if err != nil {
				return err
			}

			return action3.Execute(ctx)
		},
	}

	// Create context and execute
	ctx := &ActionContext{
		Logger: &TestLogger{t: t},
	}

	err := compositeAction.Execute(ctx)
	assert.NoError(t, err)

	// Verify all actions executed
	assert.True(t, actionExecutions["action1"], "Action 1 should have executed")
	assert.True(t, actionExecutions["action2"], "Action 2 should have executed")
	assert.True(t, actionExecutions["action3"], "Action 3 should have executed")
}

// Test CustomError methods
func TestCustomError(t *testing.T) {
	// Create a simple error
	err := &CustomError{
		Code:    "TEST_ERR",
		Message: "Test error message",
	}

	// Test Error() method
	assert.Contains(t, err.Error(), "TEST_ERR")
	assert.Contains(t, err.Error(), "Test error message")

	// Test with cause
	cause := &CustomError{
		Code:    "CAUSE_ERR",
		Message: "Cause error message",
	}

	errWithCause := &CustomError{
		Code:    "WRAPPER_ERR",
		Message: "Wrapper error message",
		Cause:   cause,
	}

	// Test Error() with cause
	assert.Contains(t, errWithCause.Error(), "WRAPPER_ERR")
	assert.Contains(t, errWithCause.Error(), "Wrapper error message")
	assert.Contains(t, errWithCause.Error(), "caused by")
	assert.Contains(t, errWithCause.Error(), "CAUSE_ERR")
}

// Test action implements interface
func TestActionInterface(t *testing.T) {
	// Create a custom action that implements the Action interface
	action := &TestAction{
		BaseAction: NewBaseAction("test", "Test"),
		executeFunc: func(ctx *ActionContext) error {
			return nil
		},
	}

	// Verify it implements the interface
	var actionInterface Action = action

	// If this compiles, it means TestAction implements Action
	assert.Equal(t, "test", actionInterface.Name())
	assert.Equal(t, "Test", actionInterface.Description())
	assert.Empty(t, actionInterface.Tags())

	// Execute should work as well
	ctx := &ActionContext{
		Logger: &TestLogger{t: t},
	}

	err := actionInterface.Execute(ctx)
	assert.NoError(t, err)
}
