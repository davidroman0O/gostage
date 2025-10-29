package gostage

import (
	"github.com/davidroman0O/gostage/v3/bootstrap"
)

type (
	FailureAction     = bootstrap.FailureAction
	FailureOutcome    = bootstrap.FailureOutcome
	FailureContext    = bootstrap.FailureContext
	FailurePolicy     = bootstrap.FailurePolicy
	FailurePolicyFunc = bootstrap.FailurePolicyFunc
)

const (
	FailureActionAck      = bootstrap.FailureActionAck
	FailureActionRetry    = bootstrap.FailureActionRetry
	FailureActionFinalize = bootstrap.FailureActionFinalize
)

func AckOutcome() FailureOutcome   { return bootstrap.AckOutcome() }
func RetryOutcome() FailureOutcome { return bootstrap.RetryOutcome() }
func CancelOutcome(reason TerminationReason) FailureOutcome {
	return bootstrap.CancelOutcome(reason)
}
