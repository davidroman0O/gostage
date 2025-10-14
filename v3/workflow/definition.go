package workflow

import "maps"

// Definition describes a workflow definition as referenced by the runtime.
type Definition struct {
	ID          string
	Name        string
	Description string
	Tags        []string
	Metadata    map[string]any
	Stages      []Stage
	Type        string
	Payload     map[string]any
	Middleware  []WorkflowMiddleware
}

// Clone returns a deep copy of the definition so runtime mutations do not
// affect shared registrations.
func (d Definition) Clone() Definition {
	out := d
	out.Tags = append([]string(nil), d.Tags...)
	out.Metadata = maps.Clone(d.Metadata)
	out.Payload = maps.Clone(d.Payload)
	out.Stages = cloneStages(d.Stages)
	out.Middleware = append([]WorkflowMiddleware(nil), d.Middleware...)
	return out
}

// Stage represents a sequential set of actions executed within a workflow.
type Stage struct {
	ID           string
	Name         string
	Description  string
	Tags         []string
	Actions      []Action
	Middleware   []StageMiddleware
	ActionMW     []ActionMiddleware
	InitialStore map[string]any
	Dynamic      bool
	CreatedBy    string
}

func (s Stage) Clone() Stage {
	out := s
	out.Tags = append([]string(nil), s.Tags...)
	out.Actions = cloneActions(s.Actions)
	out.Middleware = append([]StageMiddleware(nil), s.Middleware...)
	out.ActionMW = append([]ActionMiddleware(nil), s.ActionMW...)
	if s.InitialStore != nil {
		out.InitialStore = maps.Clone(s.InitialStore)
	}
	return out
}

// Action represents a single unit of execution resolved through the action registry.
type Action struct {
	ID          string
	Ref         string
	Description string
	Tags        []string
	Middleware  []ActionMiddleware
	Dynamic     bool
	CreatedBy   string
}

func (a Action) Clone() Action {
	out := a
	out.Tags = append([]string(nil), a.Tags...)
	out.Middleware = append([]ActionMiddleware(nil), a.Middleware...)
	return out
}

func cloneStages(stages []Stage) []Stage {
	if len(stages) == 0 {
		return nil
	}
	out := make([]Stage, len(stages))
	for i, st := range stages {
		out[i] = st.Clone()
	}
	return out
}

func cloneActions(actions []Action) []Action {
	if len(actions) == 0 {
		return nil
	}
	out := make([]Action, len(actions))
	for i, act := range actions {
		out[i] = act.Clone()
	}
	return out
}
