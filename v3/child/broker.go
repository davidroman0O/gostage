package child

import (
	"context"

	"github.com/davidroman0O/gostage/v3/broker"
	"github.com/davidroman0O/gostage/v3/state"
)

type noopBroker struct{}

func newChildBroker() broker.Broker {
	return noopBroker{}
}

func (noopBroker) WorkflowRegistered(context.Context, state.WorkflowRecord) error { return nil }
func (noopBroker) WorkflowStatus(context.Context, string, state.WorkflowState) error {
	return nil
}
func (noopBroker) StageRegistered(context.Context, string, state.StageRecord) error { return nil }
func (noopBroker) StageStatus(context.Context, string, string, state.WorkflowState) error {
	return nil
}
func (noopBroker) ActionRegistered(context.Context, string, string, state.ActionRecord) error {
	return nil
}
func (noopBroker) ActionStatus(context.Context, string, string, string, state.WorkflowState) error {
	return nil
}
func (noopBroker) ActionProgress(context.Context, string, string, string, int, string) error {
	return nil
}
func (noopBroker) ActionEvent(context.Context, string, string, string, string, string, map[string]any) error {
	return nil
}
func (noopBroker) ActionRemoved(context.Context, string, string, string, string) error { return nil }
func (noopBroker) StageRemoved(context.Context, string, string, string) error       { return nil }
func (noopBroker) ExecutionSummary(context.Context, string, state.ExecutionReport) error {
	return nil
}
