package gostage

import (
	"fmt"
	"sync"
)

// ConditionFunc is a named condition function for serializable branch and loop steps.
// Register with Condition() and reference with WhenNamed(), DoUntilNamed(), DoWhileNamed().
//
//	gostage.Condition("is-high-priority", func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[string](ctx, "priority") == "high"
//	})
type ConditionFunc func(ctx *Ctx) bool

// MapFunc is a named transform function for serializable map steps.
// Register with MapFn() and reference with MapNamed().
//
//	gostage.MapFn("parse-csv", func(ctx *gostage.Ctx) {
//	    raw := gostage.Get[[]byte](ctx, "raw")
//	    gostage.Set(ctx, "records", parseCSV(raw))
//	})
type MapFunc func(ctx *Ctx)

var (
	condRegistryMu sync.RWMutex
	condRegistry   = make(map[string]ConditionFunc)

	mapFnRegistryMu sync.RWMutex
	mapFnRegistry   = make(map[string]MapFunc)
)

// Condition registers a named condition function in the global registry.
// Panics if a condition with the same name is already registered.
//
//	gostage.Condition("order-is-paid", func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[string](ctx, "payment_status") == "paid"
//	})
func Condition(name string, fn ConditionFunc) {
	condRegistryMu.Lock()
	defer condRegistryMu.Unlock()

	if _, exists := condRegistry[name]; exists {
		panic(fmt.Sprintf("gostage: condition %q already registered", name))
	}
	condRegistry[name] = fn
}

// lookupCondition returns the condition function for the given name, or nil if not found.
func lookupCondition(name string) ConditionFunc {
	condRegistryMu.RLock()
	defer condRegistryMu.RUnlock()
	return condRegistry[name]
}

// MapFn registers a named map/transform function in the global registry.
// Panics if a map function with the same name is already registered.
//
//	gostage.MapFn("normalize-data", func(ctx *gostage.Ctx) {
//	    raw := gostage.Get[string](ctx, "input")
//	    gostage.Set(ctx, "output", strings.ToLower(raw))
//	})
func MapFn(name string, fn MapFunc) {
	mapFnRegistryMu.Lock()
	defer mapFnRegistryMu.Unlock()

	if _, exists := mapFnRegistry[name]; exists {
		panic(fmt.Sprintf("gostage: map function %q already registered", name))
	}
	mapFnRegistry[name] = fn
}

// lookupMapFn returns the map function for the given name, or nil if not found.
func lookupMapFn(name string) MapFunc {
	mapFnRegistryMu.RLock()
	defer mapFnRegistryMu.RUnlock()
	return mapFnRegistry[name]
}

// ResetFunctionRegistries clears all registered conditions and map functions. Used in tests.
func ResetFunctionRegistries() {
	condRegistryMu.Lock()
	condRegistry = make(map[string]ConditionFunc)
	condRegistryMu.Unlock()

	mapFnRegistryMu.Lock()
	mapFnRegistry = make(map[string]MapFunc)
	mapFnRegistryMu.Unlock()
}
