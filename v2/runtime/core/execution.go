package core

import "github.com/davidroman0O/gostage/v2/types"

// ExecutionContext describes the extended context capabilities required by runner backends.
type ExecutionContext interface {
	types.Context

	SetLogger(types.Logger)
	SetStage(types.Stage)
	ClearStage()
	SetAction(types.Action, int, bool)
	ConsumeDynamicActions() []types.Action
	ConsumeDynamicStages() []types.Stage
	SetActionList([]types.Action)
	SetDisabledMaps(map[string]bool, map[string]bool)
	DisabledMaps() (map[string]bool, map[string]bool)
	ConsumeRemovedAction(stageID, actionName string) (bool, string)
	ConsumeRemovedStages() map[string]string
}

// Factory builds an execution context for a given workflow.
type Factory interface {
	New(types.Workflow, types.BrokerCall) ExecutionContext
}
