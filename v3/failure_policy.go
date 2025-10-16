package gostage

import (
	"context"

	"github.com/davidroman0O/gostage/v3/state"
)

// FailureDecision instructs the dispatcher how to proceed after a workflow run.
type FailureDecision int

const (
	// FailureDecisionAck acknowledges the workflow and removes it from the queue.
	FailureDecisionAck FailureDecision = iota
	// FailureDecisionRetry releases the workflow back to the queue for another attempt.
	FailureDecisionRetry
	// FailureDecisionCancel marks the workflow as cancelled (treated as Ack for now).
	FailureDecisionCancel
)

// FailureContext supplies details about a failed workflow execution.
type FailureContext struct {
	WorkflowID state.WorkflowID
	Attempt    int
	Err        error
}

// FailurePolicy evaluates a failed workflow run and returns the desired action.
type FailurePolicy interface {
	Decide(ctx context.Context, info FailureContext) FailureDecision
}

// FailurePolicyFunc adapts a function into a FailurePolicy.
type FailurePolicyFunc func(context.Context, FailureContext) FailureDecision

// Decide implements FailurePolicy.
func (f FailurePolicyFunc) Decide(ctx context.Context, info FailureContext) FailureDecision {
	if f == nil {
		return FailureDecisionAck
	}
	return f(ctx, info)
}
