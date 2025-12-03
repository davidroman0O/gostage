package local

import (
	rt "github.com/davidroman0O/gostage/v3/shared/runtime"
	store "github.com/davidroman0O/gostage/v3/shared/store"
)

// queryImpl implements the Query interface for workflow discovery and type-safe store access.
type queryImpl struct {
	ctx *contextImpl
}

func newQuery(ctx *contextImpl) rt.Query {
	return &queryImpl{ctx: ctx}
}

// Query workflow structure methods
func (q *queryImpl) FindActionsByTag(tag string) []rt.Action {
	return q.FilterActions(func(action rt.Action) bool {
		for _, t := range action.Tags() {
			if t == tag {
				return true
			}
		}
		return false
	})
}

func (q *queryImpl) FindActionsByTags(tags []string) []rt.Action {
	tagSet := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		tagSet[tag] = struct{}{}
	}
	return q.FilterActions(func(action rt.Action) bool {
		for _, t := range action.Tags() {
			if _, ok := tagSet[t]; ok {
				return true
			}
		}
		return false
	})
}

func (q *queryImpl) FindStagesByTag(tag string) []rt.Stage {
	return q.FilterStages(func(stage rt.Stage) bool {
		for _, t := range stage.Tags() {
			if t == tag {
				return true
			}
		}
		return false
	})
}

func (q *queryImpl) FindStagesByTags(tags []string) []rt.Stage {
	tagSet := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		tagSet[tag] = struct{}{}
	}
	return q.FilterStages(func(stage rt.Stage) bool {
		for _, t := range stage.Tags() {
			if _, ok := tagSet[t]; ok {
				return true
			}
		}
		return false
	})
}

func (q *queryImpl) FilterActions(filter func(rt.Action) bool) []rt.Action {
	if filter == nil {
		return nil
	}
	workflow := q.ctx.Workflow()
	if workflow == nil {
		return nil
	}
	var result []rt.Action
	for _, stage := range workflow.Stages() {
		for _, action := range stage.ActionList() {
			if filter(action) {
				result = append(result, action)
			}
		}
	}
	return result
}

func (q *queryImpl) FilterStages(filter func(rt.Stage) bool) []rt.Stage {
	if filter == nil {
		return nil
	}
	workflow := q.ctx.Workflow()
	if workflow == nil {
		return nil
	}
	var result []rt.Stage
	for _, stage := range workflow.Stages() {
		if filter(stage) {
			result = append(result, stage)
		}
	}
	return result
}

func (q *queryImpl) ListAllActions() []rt.Action {
	workflow := q.ctx.Workflow()
	if workflow == nil {
		return nil
	}
	var result []rt.Action
	for _, stage := range workflow.Stages() {
		result = append(result, stage.ActionList()...)
	}
	return result
}

func (q *queryImpl) ListAllStages() []rt.Stage {
	workflow := q.ctx.Workflow()
	if workflow == nil {
		return nil
	}
	return workflow.Stages()
}

// Type-safe store accessors
func (q *queryImpl) GetString(key string) (string, error) {
	return store.Get[string](q.ctx, key)
}

func (q *queryImpl) GetInt(key string) (int, error) {
	return store.Get[int](q.ctx, key)
}

func (q *queryImpl) GetBool(key string) (bool, error) {
	return store.Get[bool](q.ctx, key)
}

func (q *queryImpl) GetStringOrDefault(key, defaultValue string) string {
	val, err := store.Get[string](q.ctx, key)
	if err != nil {
		return defaultValue
	}
	return val
}

func (q *queryImpl) GetIntOrDefault(key string, defaultValue int) int {
	val, err := store.Get[int](q.ctx, key)
	if err != nil {
		return defaultValue
	}
	return val
}

func (q *queryImpl) GetBoolOrDefault(key string, defaultValue bool) bool {
	val, err := store.Get[bool](q.ctx, key)
	if err != nil {
		return defaultValue
	}
	return val
}

// Note: For generic type access, use store.Get[T](ctx, key) directly.
// The Query interface provides convenience methods for common types (string, int, bool).
