package workflow

// StageMutation allows actions to add/disable/remove stages at runtime.
type StageMutation interface {
	Add(stage Stage) string
	Remove(id string) bool
	RemoveByTags(tags ...string) int
	Disable(id string) bool
	DisableByTags(tags ...string) int
	Enable(id string) bool
	EnableByTags(tags ...string) int
	IsEnabled(id string) bool
}

// ActionMutation allows actions to add/disable/remove actions dynamically.
type ActionMutation interface {
	Add(action Action) string
	Remove(id string) bool
	RemoveByTags(tags ...string) int
	Disable(id string) bool
	DisableByTags(tags ...string) int
	Enable(id string) bool
	EnableByTags(tags ...string) int
	IsEnabled(id string) bool
}
