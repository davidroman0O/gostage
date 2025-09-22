package defaults

import "github.com/davidroman0O/gostage/v2/types"

// Action is a convenience implementation of types.Action.
type Action struct {
	name        string
	description string
	tags        []string
	run         func(types.Context) error
}

// NewAction creates a new action using the provided runner function.
func NewAction(name, description string, run func(types.Context) error) *Action {
	return &Action{name: name, description: description, run: run, tags: make([]string, 0)}
}

// SetTags replaces the action tags.
func (a *Action) SetTags(tags ...string) {
	a.tags = append([]string(nil), tags...)
}

// AddTags appends tags, avoiding duplicates.
func (a *Action) AddTags(tags ...string) {
	for _, tag := range tags {
		if !contains(a.tags, tag) {
			a.tags = append(a.tags, tag)
		}
	}
}

func (a *Action) Name() string        { return a.name }
func (a *Action) Description() string { return a.description }
func (a *Action) Tags() []string      { return append([]string(nil), a.tags...) }

func (a *Action) Execute(ctx types.Context) error {
	if a.run == nil {
		return nil
	}
	return a.run(ctx)
}
