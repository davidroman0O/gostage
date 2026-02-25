package gostage

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
//	gostage.MapFn("parse-csv", func(ctx *gostage.Ctx) error {
//	    raw := gostage.Get[[]byte](ctx, "raw")
//	    gostage.Set(ctx, "records", parseCSV(raw))
//	    return nil
//	})
type MapFunc func(ctx *Ctx) error

// Condition registers a named condition function in the default registry.
// Panics if a condition with the same name is already registered.
//
//	gostage.Condition("order-is-paid", func(ctx *gostage.Ctx) bool {
//	    return gostage.Get[string](ctx, "payment_status") == "paid"
//	})
func Condition(name string, fn ConditionFunc) {
	defaultRegistry.RegisterCondition(name, fn)
}

// lookupCondition returns the condition function from the default registry.
func lookupCondition(name string) ConditionFunc {
	return defaultRegistry.lookupCondition(name)
}

// MapFn registers a named map/transform function in the default registry.
// Panics if a map function with the same name is already registered.
//
//	gostage.MapFn("normalize-data", func(ctx *gostage.Ctx) error {
//	    raw := gostage.Get[string](ctx, "input")
//	    gostage.Set(ctx, "output", strings.ToLower(raw))
//	    return nil
//	})
func MapFn(name string, fn MapFunc) {
	defaultRegistry.RegisterMapFn(name, fn)
}

// lookupMapFn returns the map function from the default registry.
func lookupMapFn(name string) MapFunc {
	return defaultRegistry.lookupMapFn(name)
}

// ResetFunctionRegistries clears all registered conditions and map functions. Used in tests.
func ResetFunctionRegistries() {
	// Reset is now handled by Registry.Reset() which clears all three registries.
	// This function exists for backward compatibility — it only clears conditions and map functions.
	// Since ResetTaskRegistry calls defaultRegistry.Reset() which clears everything,
	// and this is typically called from ResetTaskRegistry, we only need to clear if called standalone.
	r := defaultRegistry
	r.condMu.Lock()
	r.conditions = make(map[string]ConditionFunc)
	r.condMu.Unlock()

	r.mapFnMu.Lock()
	r.mapFns = make(map[string]MapFunc)
	r.mapFnMu.Unlock()
}
