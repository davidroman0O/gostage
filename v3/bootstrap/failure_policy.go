package bootstrap

import (
	"context"

	"github.com/davidroman0O/gostage/v3/runner"
	"github.com/davidroman0O/gostage/v3/state"
)

// FailureAction instructs the dispatcher how to treat a failed attempt.
type FailureAction int

const (
	FailureActionAck FailureAction = iota
	FailureActionRetry
	FailureActionFinalize
)

// FailureOutcome captures the decision returned by a FailurePolicy.
type FailureOutcome struct {
	Action     FailureAction
	FinalState runner.ExecutionStatus
	Reason     state.TerminationReason
}

// FailureContext supplies details about a failed workflow execution.
type FailureContext struct {
	WorkflowID state.WorkflowID
	Attempt    int
	Err        error
}

// FailurePolicy evaluates a failed workflow run and returns the desired action.
type FailurePolicy interface {
	Decide(ctx context.Context, info FailureContext) FailureOutcome
}

// FailurePolicyFunc adapts a function into a FailurePolicy.
type FailurePolicyFunc func(context.Context, FailureContext) FailureOutcome

// Decide implements FailurePolicy.
func (f FailurePolicyFunc) Decide(ctx context.Context, info FailureContext) FailureOutcome {
	if f == nil {
		return FailureOutcome{Action: FailureActionAck}
	}
	return f(ctx, info)
}

// AckOutcome returns a FailureOutcome that acknowledges the run.
func AckOutcome() FailureOutcome { return FailureOutcome{Action: FailureActionAck} }

// RetryOutcome returns a FailureOutcome that requeues the workflow.
func RetryOutcome() FailureOutcome { return FailureOutcome{Action: FailureActionRetry} }

// CancelOutcome returns a FailureOutcome that finalises the workflow as cancelled using the provided reason.
func CancelOutcome(reason state.TerminationReason) FailureOutcome {
	if reason == "" {
		reason = state.TerminationReasonPolicyCancel
	}
	return FailureOutcome{
		Action:     FailureActionFinalize,
		FinalState: runner.StatusCancelled,
		Reason:     reason,
	}
}
