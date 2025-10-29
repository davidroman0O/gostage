package gostage

import (
	"github.com/davidroman0O/gostage/v3/bootstrap"
	"github.com/davidroman0O/gostage/v3/node"
	"github.com/davidroman0O/gostage/v3/orchestrator"
	"github.com/davidroman0O/gostage/v3/telemetry"
	"github.com/davidroman0O/gostage/v3/workflow"
)

type poolBinding = orchestrator.PoolBinding
type remoteBinding = orchestrator.RemoteBinding
type spawnerBinding = orchestrator.SpawnerBinding
type remoteCoordinator = orchestrator.RemoteCoordinator
type remotePool = orchestrator.RemotePool
type remoteWorker = orchestrator.RemoteWorker
type remoteJob = orchestrator.RemoteJob
type parentNode = orchestrator.Node

var newRemoteCoordinator = orchestrator.NewRemoteCoordinator

func newParentNodeForTest() *parentNode {
	return orchestrator.NewTestNode()
}

func newRemoteCoordinatorBareForTest(diag node.DiagnosticsWriter, logger telemetry.Logger) *remoteCoordinator {
	return orchestrator.NewRemoteCoordinatorBareForTest(diag, logger)
}

func newRemoteWorkerForTest(busy bool, metadata map[string]string) *remoteWorker {
	return orchestrator.NewRemoteWorkerForTest(busy, metadata)
}

func resolveWorkflowReferenceForTest(ref WorkflowReference) (workflow.Definition, error) {
	return orchestrator.ResolveWorkflowReferenceForTest(ref)
}

func applySubmitOptionForTest(opt SubmitOption, req *bootstrap.SubmitRequest) {
	orchestrator.ApplySubmitOptionForTest(opt, req)
}
