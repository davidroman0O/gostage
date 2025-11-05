package gostagetest

import (
	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/orchestrator"
)

type (
	PoolBinding    = orchestrator.PoolBinding
	RemoteBinding  = orchestrator.RemoteBinding
	SpawnerBinding = orchestrator.SpawnerBinding
)

func NewParentNode() *orchestrator.Node {
	return orchestrator.NewTestNode()
}

func ApplySubmitOption(opt orchestrator.SubmitOption, req *bootstrap.SubmitRequest) {
	orchestrator.ApplySubmitOptionForTest(opt, req)
}

func PoolMetadataToStrings(values map[string]any) map[string]string {
	return orchestrator.PoolMetadataToStrings(values)
}
