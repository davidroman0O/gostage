package workflow

import (
	"fmt"
	"slices"
)

// Builder constructs workflow definitions with validation.
type Builder struct {
	def Definition
	err error
}

// New returns a workflow builder with the provided ID.
func New(id string) *Builder {
	return &Builder{
		def: Definition{
			ID:         id,
			Metadata:   make(map[string]any),
			Middleware: make([]string, 0),
		},
	}
}

func (b *Builder) Named(name string) *Builder {
	b.def.Name = name
	return b
}

func (b *Builder) Describe(desc string) *Builder {
	b.def.Description = desc
	return b
}

func (b *Builder) WithTags(tags ...string) *Builder {
	b.def.Tags = appendUnique(b.def.Tags, tags...)
	return b
}

func (b *Builder) Metadata(key string, value any) *Builder {
	if b.def.Metadata == nil {
		b.def.Metadata = make(map[string]any)
	}
	b.def.Metadata[key] = value
	return b
}

func (b *Builder) Type(t string) *Builder {
	b.def.Type = t
	return b
}

func (b *Builder) Payload(payload map[string]any) *Builder {
	if payload == nil {
		b.def.Payload = nil
		return b
	}
	if b.def.Payload == nil {
		b.def.Payload = make(map[string]any, len(payload))
	}
	for k, v := range payload {
		b.def.Payload[k] = v
	}
	return b
}

func (b *Builder) Use(ids ...string) *Builder {
	b.def.Middleware = appendUnique(b.def.Middleware, ids...)
	return b
}

func (b *Builder) Stage(stage *StageBuilder) *Builder {
	if stage == nil {
		return b
	}
	st, err := stage.Build()
	if err != nil {
		b.err = err
		return b
	}
	b.def.Stages = append(b.def.Stages, st)
	return b
}

func (b *Builder) Build() (Definition, error) {
	if b.err != nil {
		return Definition{}, b.err
	}
	if b.def.ID == "" {
		return Definition{}, fmt.Errorf("workflow: id is required")
	}
	seenStage := make(map[string]struct{})
	for _, stage := range b.def.Stages {
		if stage.ID != "" {
			if _, ok := seenStage[stage.ID]; ok {
				return Definition{}, fmt.Errorf("workflow: duplicate stage id %q", stage.ID)
			}
			seenStage[stage.ID] = struct{}{}
		}
		if err := validateStage(stage); err != nil {
			return Definition{}, err
		}
	}
	return b.def.Clone(), nil
}

// StageBuilder assists with stage construction.
type StageBuilder struct {
	stage Stage
}

func StageDef(id string) *StageBuilder {
	return &StageBuilder{
		stage: Stage{
			ID:         id,
			Actions:    make([]Action, 0),
			Middleware: make([]string, 0),
		},
	}
}

func (s *StageBuilder) Named(name string) *StageBuilder {
	s.stage.Name = name
	return s
}

func (s *StageBuilder) Describe(desc string) *StageBuilder {
	s.stage.Description = desc
	return s
}

func (s *StageBuilder) AddTags(tags ...string) *StageBuilder {
	s.stage.Tags = appendUnique(s.stage.Tags, tags...)
	return s
}

func (s *StageBuilder) WithInitialStore(values map[string]any) *StageBuilder {
	if values == nil {
		s.stage.InitialStore = nil
		return s
	}
	if s.stage.InitialStore == nil {
		s.stage.InitialStore = make(map[string]any, len(values))
	}
	for k, v := range values {
		s.stage.InitialStore[k] = v
	}
	return s
}

func (s *StageBuilder) Use(id string) *StageBuilder {
	if id != "" {
		s.stage.Middleware = appendUnique(s.stage.Middleware, id)
	}
	return s
}

func (s *StageBuilder) Action(action *ActionBuilder) *StageBuilder {
	if action == nil {
		return s
	}
	act, err := action.Build()
	if err != nil {
		return s
	}
	s.stage.Actions = append(s.stage.Actions, act)
	return s
}

func (s *StageBuilder) Build() (Stage, error) {
	actionIDs := make(map[string]struct{})
	for _, act := range s.stage.Actions {
		if act.ID != "" {
			if _, ok := actionIDs[act.ID]; ok {
				return Stage{}, fmt.Errorf("workflow: duplicate action id %s in stage %s", act.ID, s.stage.ID)
			}
			actionIDs[act.ID] = struct{}{}
		}
		if act.Ref == "" {
			return Stage{}, fmt.Errorf("workflow: action %s requires registry ref", act.ID)
		}
	}
	return s.stage.Clone(), nil
}

// ActionBuilder assists with action construction.
type ActionBuilder struct {
	action Action
	err    error
}

func ActionDef(ref string) *ActionBuilder {
	return &ActionBuilder{
		action: Action{
			Ref:        ref,
			Middleware: make([]string, 0),
		},
	}
}

func (a *ActionBuilder) ID(id string) *ActionBuilder {
	a.action.ID = id
	return a
}

func (a *ActionBuilder) Describe(desc string) *ActionBuilder {
	a.action.Description = desc
	return a
}

func (a *ActionBuilder) AddTags(tags ...string) *ActionBuilder {
	a.action.Tags = appendUnique(a.action.Tags, tags...)
	return a
}

func (a *ActionBuilder) Use(id string) *ActionBuilder {
	if id != "" {
		a.action.Middleware = appendUnique(a.action.Middleware, id)
	}
	return a
}

func (a *ActionBuilder) Build() (Action, error) {
	if a.err != nil {
		return Action{}, a.err
	}
	if a.action.Ref == "" {
		return Action{}, fmt.Errorf("workflow: action ref required")
	}
	return a.action.Clone(), nil
}

func validateStage(stage Stage) error {
	actionIDs := make(map[string]struct{})
	for _, action := range stage.Actions {
		if action.ID != "" {
			if _, ok := actionIDs[action.ID]; ok {
				return fmt.Errorf("workflow: duplicate action id %s in stage %s", action.ID, stage.ID)
			}
			actionIDs[action.ID] = struct{}{}
		}
		if action.Ref == "" {
			return fmt.Errorf("workflow: action %s missing ref", action.ID)
		}
	}
	return nil
}

func appendUnique(dst []string, values ...string) []string {
	for _, v := range values {
		if v == "" {
			continue
		}
		if !slices.Contains(dst, v) {
			dst = append(dst, v)
		}
	}
	return dst
}
