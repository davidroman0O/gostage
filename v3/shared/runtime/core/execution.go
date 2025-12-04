// Package core provides core execution context interfaces for workflow runtime.
package core

import rt "github.com/davidroman0O/gostage/v3/shared/runtime"

// ExecutionContext describes the extended context capabilities required by runner backends.
type ExecutionContext interface {
	rt.Context

	SetLogger(rt.Logger)
	SetStage(rt.Stage)
	ClearStage()
	SetAction(rt.Action, int, bool)
	ConsumeDynamicActions() []rt.Action
	ConsumeDynamicStages() []rt.Stage
	SetActionList([]rt.Action)
	SetDisabledMaps(map[string]bool, map[string]bool)
	DisabledMaps() (map[string]bool, map[string]bool)
	ConsumeRemovedAction(stageID, actionName string) (bool, string)
	ConsumeRemovedStages() map[string]string
}

// Factory builds an execution context for a given workflow.
type Factory interface {
	New(rt.Workflow, rt.Broker) ExecutionContext
}
