package gostage

// findStepByID returns the index of the step matching the given identifier
// (checked against both id and name), or -1 if not found.
func findStepByID(steps []step, id string) int {
	for i, s := range steps {
		if s.id == id || s.name == id {
			return i
		}
	}
	return -1
}

// setStepDisabledByIDOrName sets the disabled flag on all steps matching the
// given identifier (by id or name). This handles the case where the caller
// wants to disable/enable by name and multiple steps might share a name.
func setStepDisabledByIDOrName(steps []step, id string, disabled bool) {
	for i := range steps {
		if steps[i].id == id || steps[i].name == id {
			steps[i].disabled = disabled
		}
	}
}
