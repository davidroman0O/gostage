package workflow

import (
	"fmt"
	"maps"

	"github.com/davidroman0O/gostage/v3/registry"
	rt "github.com/davidroman0O/gostage/v3/runtime"
)

// Materialize builds a runtime workflow instance from a declarative Definition.
// The returned workflow is safe to execute with the runner package.
func Materialize(def Definition, reg registry.Registry) (rt.Workflow, error) {
	if reg == nil {
		reg = registry.Default()
	}
	if def.ID == "" {
		return nil, fmt.Errorf("workflow: definition requires id")
	}

	normalized, _, err := EnsureIDs(def)
	if err != nil {
		return nil, err
	}

	wf := newRuntimeWorkflow(normalized.ID, normalized.Name, normalized.Description)
	wf.tags = append(wf.tags, normalized.Tags...)
	if len(normalized.Metadata) > 0 {
		for k, v := range normalized.Metadata {
			wf.metadata[k] = v
		}
	}
	if normalized.Type != "" {
		wf.workflowType = normalized.Type
	}
	if len(normalized.Payload) > 0 {
		wf.payload = maps.Clone(normalized.Payload)
	}
	for _, mwID := range normalized.Middleware {
		mw, err := resolveWorkflowMiddleware(mwID)
		if err != nil {
			return nil, fmt.Errorf("workflow: middleware %s: %w", mwID, err)
		}
		wf.Use(mw)
	}
	for _, stageDef := range normalized.Stages {
		stage, err := materializeStage(stageDef, reg)
		if err != nil {
			return nil, fmt.Errorf("workflow: stage %s: %w", stageDef.ID, err)
		}
		wf.AddStage(stage)
	}
	return wf, nil
}

// MaterializeStage converts a declarative Stage into a runtime stage compatible with the runner.
func MaterializeStage(def Stage, reg registry.Registry) (rt.Stage, error) {
	stage, err := materializeStage(def, reg)
	if err != nil {
		return nil, err
	}
	return stage, nil
}

func materializeStage(def Stage, reg registry.Registry) (*RuntimeStage, error) {
	if reg == nil {
		reg = registry.Default()
	}
	if def.ID == "" {
		normalized, _, err := EnsureIDs(Definition{Stages: []Stage{def}})
		if err != nil {
			return nil, err
		}
		if len(normalized.Stages) == 0 {
			return nil, fmt.Errorf("workflow: stage definition requires id")
		}
		def = normalized.Stages[0]
	}
	stage := newRuntimeStage(def.ID, def.Name, def.Description)
	if len(def.Tags) > 0 {
		stage.AddTags(def.Tags...)
	}
	if len(def.InitialStore) > 0 {
		initial := stage.InitialStore()
		for key, value := range def.InitialStore {
			_ = initial.Put(key, value)
		}
	}
	for _, actionDef := range def.Actions {
		action, err := resolveAction(actionDef, reg)
		if err != nil {
			return nil, fmt.Errorf("stage %s action %s: %w", def.ID, actionDef.ID, err)
		}
		stage.AddActions(action)
	}
	for _, mwID := range def.Middleware {
		mw, err := resolveStageMiddleware(mwID)
		if err != nil {
			return nil, fmt.Errorf("stage %s: middleware %s: %w", def.ID, mwID, err)
		}
		stage.WithMiddleware(mw)
	}
	return stage, nil
}

type runtimeAction struct {
	id          string
	ref         string
	description string
	tags        []string
	factory     registry.ActionFactory
	middleware  []rt.ActionMiddleware
}

func resolveAction(def Action, reg registry.Registry) (rt.Action, error) {
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
		_ = reg.RegisterAction(id, func(ctx rt.Context) error {
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
	var middleware []rt.ActionMiddleware
	for _, mwID := range def.Middleware {
		mw, err := resolveActionMiddleware(mwID)
		if err != nil {
			return nil, fmt.Errorf("workflow: action %s middleware %s: %w", id, mwID, err)
		}
		middleware = append(middleware, mw)
	}
	return &runtimeAction{
		id:          id,
		ref:         def.Ref,
		description: desc,
		tags:        tags,
		factory:     factory,
		middleware:  middleware,
	}, nil
}

func (a *runtimeAction) Name() string        { return a.id }
func (a *runtimeAction) Description() string { return a.description }
func (a *runtimeAction) Tags() []string      { return append([]string(nil), a.tags...) }

func (a *runtimeAction) Execute(ctx rt.Context) error {
	if a.factory == nil {
		return fmt.Errorf("workflow: action %s missing factory", a.id)
	}
	return a.factory(ctx)
}

func (a *runtimeAction) MiddlewareChain() []rt.ActionMiddleware {
	if len(a.middleware) == 0 {
		return nil
	}
	out := make([]rt.ActionMiddleware, len(a.middleware))
	copy(out, a.middleware)
	return out
}
