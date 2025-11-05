package gostage

import (
	"github.com/davidroman0O/gostage/v3/orchestrator"
	"github.com/davidroman0O/gostage/v3/workflow"
)

type (
	ActionFunc        = orchestrator.ActionFunc
	ActionFactory     = orchestrator.ActionFactory
	ActionOption      = orchestrator.ActionOption
	WorkflowReference = orchestrator.WorkflowReference
	SubmitOption      = orchestrator.SubmitOption
)

func WithActionDescription(desc string) ActionOption {
	return orchestrator.WithActionDescription(desc)
}

func WithActionTags(tags ...string) ActionOption {
	return orchestrator.WithActionTags(tags...)
}

func WithActionVersion(version string) ActionOption {
	return orchestrator.WithActionVersion(version)
}

func WithActionAuthor(author string) ActionOption {
	return orchestrator.WithActionAuthor(author)
}

func MustRegisterAction(name string, factory ActionFactory, opts ...ActionOption) {
	orchestrator.MustRegisterAction(name, factory, opts...)
}

func RegisterAction(name string, factory ActionFactory, opts ...ActionOption) error {
	return orchestrator.RegisterAction(name, factory, opts...)
}

func MustRegisterWorkflow(def workflow.Definition) (string, workflow.IDAssignment) {
	return orchestrator.MustRegisterWorkflow(def)
}

func RegisterWorkflow(def workflow.Definition) (string, workflow.IDAssignment, error) {
	return orchestrator.RegisterWorkflow(def)
}

func WorkflowRef(id string) WorkflowReference {
	return orchestrator.WorkflowRef(id)
}

func WorkflowDefinition(def workflow.Definition) WorkflowReference {
	return orchestrator.WorkflowDefinition(def)
}

func ResolveWorkflow(id string) (workflow.Definition, bool) {
	return orchestrator.ResolveWorkflow(id)
}

func WorkflowIDs(id string) (workflow.IDAssignment, bool) {
	return orchestrator.WorkflowIDs(id)
}

func WorkflowReferenceIDs(ref WorkflowReference) (workflow.IDAssignment, bool) {
	return orchestrator.WorkflowReferenceIDs(ref)
}

func WithPriority(priority int) SubmitOption {
	return orchestrator.WithPriority(priority)
}

func WithTags(tags ...string) SubmitOption {
	return orchestrator.WithTags(tags...)
}

func WithInitialStore(values map[string]any) SubmitOption {
	return orchestrator.WithInitialStore(values)
}

func WithMetadata(values map[string]any) SubmitOption {
	return orchestrator.WithMetadata(values)
}
