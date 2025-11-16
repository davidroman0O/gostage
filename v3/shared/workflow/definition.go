package workflow

import (
	"maps"

	"github.com/davidroman0O/gostage/v3/shared/metadata"
)

// Definition describes a workflow definition as referenced by the runtime.
type Definition struct {
	ID          string            `json:"id"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Metadata    metadata.Metadata `json:"metadata,omitempty"`
	Stages      []Stage           `json:"stages,omitempty"`
	Type        string            `json:"type,omitempty"`
	Payload     map[string]any    `json:"payload,omitempty"`
	Middleware  []string          `json:"middleware,omitempty"`
}

// Clone returns a deep copy of the definition so runtime mutations do not
// affect shared registrations.
func (d Definition) Clone() Definition {
	out := d
	out.Tags = append([]string(nil), d.Tags...)
	out.Metadata = d.Metadata.Clone()
	out.Payload = maps.Clone(d.Payload)
	out.Stages = cloneStages(d.Stages)
	out.Middleware = append([]string(nil), d.Middleware...)
	return out
}

// GetMetadataString is a convenience method for accessing metadata strings.
func (d Definition) GetMetadataString(key string) (string, bool) {
	return d.Metadata.GetString(key)
}

// Stage represents a sequential set of actions executed within a workflow.
type Stage struct {
	ID           string         `json:"id"`
	Name         string         `json:"name,omitempty"`
	Description  string         `json:"description,omitempty"`
	Tags         []string       `json:"tags,omitempty"`
	Actions      []Action       `json:"actions,omitempty"`
	Middleware   []string       `json:"middleware,omitempty"`
	InitialStore map[string]any `json:"initialStore,omitempty"`
}

// Clone creates a deep copy of the stage.
func (s Stage) Clone() Stage {
	out := s
	out.Tags = append([]string(nil), s.Tags...)
	out.Actions = cloneActions(s.Actions)
	out.Middleware = append([]string(nil), s.Middleware...)
	if s.InitialStore != nil {
		out.InitialStore = maps.Clone(s.InitialStore)
	}
	return out
}

// Action represents a single unit of execution resolved through the action registry.
type Action struct {
	ID          string   `json:"id"`
	Ref         string   `json:"ref"`
	Description string   `json:"description,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Middleware  []string `json:"middleware,omitempty"`
}

// Clone creates a deep copy of the action.
func (a Action) Clone() Action {
	out := a
	out.Tags = append([]string(nil), a.Tags...)
	out.Middleware = append([]string(nil), a.Middleware...)
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
