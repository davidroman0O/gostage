package gostage

import "github.com/davidroman0O/gostage/v3/orchestrator"

type SubmitOption = orchestrator.SubmitOption

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
