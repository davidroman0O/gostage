package workflow

import (
	"slices"

	gostageerrors "github.com/davidroman0O/gostage/v3/internal/errors"
	"github.com/davidroman0O/gostage/v3/metadata"
)

var errHelper = gostageerrors.NewHelper("workflow")

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
			Metadata:   metadata.New(),
			Middleware: make([]string, 0),
		},
	}
}

// Named sets the workflow name.
func (b *Builder) Named(name string) *Builder {
	b.def.Name = name
	return b
}

// Describe sets the workflow description.
func (b *Builder) Describe(desc string) *Builder {
	b.def.Description = desc
	return b
}

// WithTags adds tags to the workflow, avoiding duplicates.
func (b *Builder) WithTags(tags ...string) *Builder {
	b.def.Tags = appendUnique(b.def.Tags, tags...)
	return b
}

// Metadata sets a metadata key-value pair.
func (b *Builder) Metadata(key string, value any) *Builder {
	// Metadata is always initialized in New(), so no need to check
	b.def.Metadata.Set(key, value)
	return b
}

// Type sets the workflow type.
func (b *Builder) Type(t string) *Builder {
	b.def.Type = t
	return b
}

// Payload sets the workflow payload.
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

// Use adds middleware IDs to the workflow.
func (b *Builder) Use(ids ...string) *Builder {
	b.def.Middleware = appendUnique(b.def.Middleware, ids...)
	return b
}

// Stage adds a stage to the workflow.
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

// Build constructs the final workflow definition with validation.
func (b *Builder) Build() (Definition, error) {
	if b.err != nil {
		return Definition{}, b.err
	}
	if b.def.ID == "" {
		return Definition{}, errHelper.Newf(gostageerrors.ErrCodeInvalidConfiguration, "id is required")
	}
	seenStage := make(map[string]struct{})
	for _, stage := range b.def.Stages {
		if stage.ID != "" {
			if _, ok := seenStage[stage.ID]; ok {
				return Definition{}, errHelper.WithContext(errHelper.Newf(gostageerrors.ErrCodeInvalidConfiguration, "duplicate stage id %q", stage.ID), map[string]any{
					"stage_id": stage.ID,
				})
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

// StageDef creates a new stage builder with the given ID.
func StageDef(id string) *StageBuilder {
	return &StageBuilder{
		stage: Stage{
			ID:         id,
			Actions:    make([]Action, 0),
			Middleware: make([]string, 0),
		},
	}
}

// Named sets the stage name.
func (s *StageBuilder) Named(name string) *StageBuilder {
	s.stage.Name = name
	return s
}

// Describe sets the stage description.
func (s *StageBuilder) Describe(desc string) *StageBuilder {
	s.stage.Description = desc
	return s
}

// AddTags adds tags to the stage, avoiding duplicates.
func (s *StageBuilder) AddTags(tags ...string) *StageBuilder {
	s.stage.Tags = appendUnique(s.stage.Tags, tags...)
	return s
}

// WithInitialStore sets the initial store values for the stage.
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

// Use adds a middleware ID to the stage.
func (s *StageBuilder) Use(id string) *StageBuilder {
	if id != "" {
		s.stage.Middleware = appendUnique(s.stage.Middleware, id)
	}
	return s
}

// Action adds an action to the stage.
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

// Build constructs the final stage definition with validation.
func (s *StageBuilder) Build() (Stage, error) {
	actionIDs := make(map[string]struct{})
	for _, act := range s.stage.Actions {
		if act.ID != "" {
			if _, ok := actionIDs[act.ID]; ok {
				return Stage{}, errHelper.WithContext(errHelper.Newf(gostageerrors.ErrCodeInvalidConfiguration, "duplicate action id %s in stage %s", act.ID, s.stage.ID), map[string]any{
					"action_id": act.ID,
					"stage_id":  s.stage.ID,
				})
			}
			actionIDs[act.ID] = struct{}{}
		}
		if act.Ref == "" {
			return Stage{}, errHelper.WithContext(errHelper.Newf(gostageerrors.ErrCodeInvalidConfiguration, "action %s requires registry ref", act.ID), map[string]any{
				"action_id": act.ID,
				"stage_id":  s.stage.ID,
			})
		}
	}
	return s.stage.Clone(), nil
}

// ActionBuilder assists with action construction.
type ActionBuilder struct {
	action Action
	err    error
}

// ActionDef creates a new action builder with the given reference.
func ActionDef(ref string) *ActionBuilder {
	return &ActionBuilder{
		action: Action{
			Ref:        ref,
			Middleware: make([]string, 0),
		},
	}
}

// ID sets the action ID.
func (a *ActionBuilder) ID(id string) *ActionBuilder {
	a.action.ID = id
	return a
}

// Describe sets the action description.
func (a *ActionBuilder) Describe(desc string) *ActionBuilder {
	a.action.Description = desc
	return a
}

// AddTags adds tags to the action, avoiding duplicates.
func (a *ActionBuilder) AddTags(tags ...string) *ActionBuilder {
	a.action.Tags = appendUnique(a.action.Tags, tags...)
	return a
}

// Use adds a middleware ID to the action.
func (a *ActionBuilder) Use(id string) *ActionBuilder {
	if id != "" {
		a.action.Middleware = appendUnique(a.action.Middleware, id)
	}
	return a
}

// Build constructs the final action definition with validation.
func (a *ActionBuilder) Build() (Action, error) {
	if a.err != nil {
		return Action{}, a.err
	}
	if a.action.Ref == "" {
		return Action{}, errHelper.Newf(gostageerrors.ErrCodeInvalidConfiguration, "action ref required")
	}
	return a.action.Clone(), nil
}

func validateStage(stage Stage) error {
	actionIDs := make(map[string]struct{})
	for _, action := range stage.Actions {
		if action.ID != "" {
			if _, ok := actionIDs[action.ID]; ok {
				return errHelper.WithContext(errHelper.Newf(gostageerrors.ErrCodeInvalidConfiguration, "duplicate action id %s in stage %s", action.ID, stage.ID), map[string]any{
					"action_id": action.ID,
					"stage_id":  stage.ID,
				})
			}
			actionIDs[action.ID] = struct{}{}
		}
		if action.Ref == "" {
			return errHelper.WithContext(errHelper.Newf(gostageerrors.ErrCodeInvalidConfiguration, "action %s missing ref", action.ID), map[string]any{
				"action_id": action.ID,
				"stage_id":  stage.ID,
			})
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
