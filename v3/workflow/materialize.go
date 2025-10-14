package workflow

import (
	"fmt"
	"maps"

	"github.com/davidroman0O/gostage/v3/registry"
	"github.com/davidroman0O/gostage/v3/types"
	"github.com/davidroman0O/gostage/v3/types/defaults"
)

// Materialize builds a runtime workflow instance from a declarative Definition.
// The returned workflow is safe to execute with the runner package.
func Materialize(def Definition, reg registry.Registry) (types.Workflow, error) {
	if reg == nil {
		reg = registry.Default()
	}
	if def.ID == "" {
		return nil, fmt.Errorf("workflow: definition requires id")
	}

	wf := defaults.NewWorkflow(def.ID, def.Name, def.Description)
	if len(def.Tags) > 0 {
		wf.AddTags(def.Tags...)
	}
	if meta := def.Metadata; len(meta) > 0 {
		dst := wf.Metadata()
		for k, v := range meta {
			dst[k] = v
		}
	}
	if def.Type != "" {
		wf.SetType(def.Type)
	}
	if len(def.Payload) > 0 {
		wf.SetPayload(maps.Clone(def.Payload))
	}

	for _, stageDef := range def.Stages {
		stage := defaults.NewStage(stageDef.ID, stageDef.Name, stageDef.Description)
		if len(stageDef.Tags) > 0 {
			stage.AddTags(stageDef.Tags...)
		}
		if storeVals := stageDef.InitialStore; len(storeVals) > 0 {
			initial := stage.InitialStore()
			for key, value := range storeVals {
				_ = initial.Put(key, value)
			}
		}
		for _, actionDef := range stageDef.Actions {
			action, err := resolveAction(actionDef, reg)
			if err != nil {
				return nil, fmt.Errorf("workflow: stage %s action %s: %w", stageDef.ID, actionDef.ID, err)
			}
			stage.AddActions(action)
		}
		wf.AddStage(stage)
	}
	return wf, nil
}

type runtimeAction struct {
	id          string
	ref         string
	description string
	tags        []string
	factory     registry.ActionFactory
}

func resolveAction(def Action, reg registry.Registry) (types.Action, error) {
	if def.Ref == "" {
		return nil, fmt.Errorf("missing registry ref")
	}
	id := def.ID
	if id == "" {
		id = def.Ref
	}
	factory, meta, err := reg.ResolveAction(def.Ref)
	if err != nil {
		return nil, err
	}
	if id != def.Ref {
		aliasMeta := meta
		_ = reg.RegisterAction(id, func(ctx types.Context) error {
			return factory(ctx)
		}, aliasMeta)
	}
	desc := def.Description
	if desc == "" {
		desc = meta.Description
	}
	tags := append([]string(nil), meta.Tags...)
	if len(def.Tags) > 0 {
		existing := make(map[string]struct{}, len(tags))
		for _, t := range tags {
			existing[t] = struct{}{}
		}
		for _, t := range def.Tags {
			if _, ok := existing[t]; !ok {
				tags = append(tags, t)
				existing[t] = struct{}{}
			}
		}
	}
	return &runtimeAction{
		id:          id,
		ref:         def.Ref,
		description: desc,
		tags:        tags,
		factory:     factory,
	}, nil
}

func (a *runtimeAction) Name() string        { return a.id }
func (a *runtimeAction) Description() string { return a.description }
func (a *runtimeAction) Tags() []string      { return append([]string(nil), a.tags...) }

func (a *runtimeAction) Execute(ctx types.Context) error {
	if a.factory == nil {
		return fmt.Errorf("workflow: action %s missing factory", a.id)
	}
	return a.factory(ctx)
}
