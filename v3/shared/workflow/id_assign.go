package workflow

import "fmt"

// IDAssignment captures the IDs associated with stages and actions after
// normalisation. It is returned so callers can reference generated identifiers
// when needed (for example, to map builder positions to persisted IDs).
type IDAssignment struct {
	Stages []StageIDAssignment
}

// StageIDAssignment describes the identifier chosen for a specific stage.
type StageIDAssignment struct {
	Index     int
	ID        string
	Generated bool
	Actions   []ActionIDAssignment
}

// ActionIDAssignment captures the identifier recorded for an action within a
// stage.
type ActionIDAssignment struct {
	Index     int
	ID        string
	Generated bool
}

// EnsureIDs returns a cloned definition with stable stage/action identifiers.
//
// When the caller omits IDs, deterministic identifiers are generated based on
// positional order. Existing identifiers are preserved and validated for
// uniqueness. The accompanying IDAssignment can be used to understand which
// values were generated versus supplied by the caller.
func EnsureIDs(def Definition) (Definition, IDAssignment, error) {
	clone := def.Clone()

	assignment := IDAssignment{
		Stages: make([]StageIDAssignment, len(clone.Stages)),
	}

	seenStages := make(map[string]int)

	for i := range clone.Stages {
		stage := &clone.Stages[i]
		stageAssign := StageIDAssignment{Index: i}

		if stage.ID != "" {
			if prev, ok := seenStages[stage.ID]; ok {
				return Definition{}, IDAssignment{}, fmt.Errorf("workflow: duplicate stage id %q (indexes %d, %d)", stage.ID, prev, i)
			}
			stageAssign.ID = stage.ID
		} else {
			generated := generateStageID(i, seenStages)
			stage.ID = generated
			stageAssign.ID = generated
			stageAssign.Generated = true
		}

		seenStages[stage.ID] = i

		stageAssign.Actions = make([]ActionIDAssignment, len(stage.Actions))
		seenActions := make(map[string]int)

		for j := range stage.Actions {
			action := &stage.Actions[j]
			actionAssign := ActionIDAssignment{Index: j}

			if action.ID != "" {
				if prev, ok := seenActions[action.ID]; ok {
					return Definition{}, IDAssignment{}, fmt.Errorf("workflow: duplicate action id %q in stage %s (indexes %d, %d)", action.ID, stage.ID, prev, j)
				}
				actionAssign.ID = action.ID
			} else {
				generated := generateActionID(stage.ID, j, seenActions)
				action.ID = generated
				actionAssign.ID = generated
				actionAssign.Generated = true
			}

			seenActions[action.ID] = j
			stageAssign.Actions[j] = actionAssign
		}

		assignment.Stages[i] = stageAssign
	}

	return clone, assignment, nil
}

func generateStageID(index int, taken map[string]int) string {
	base := fmt.Sprintf("stage-%d", index+1)
	return ensureUnique(base, taken)
}

func generateActionID(stageID string, index int, taken map[string]int) string {
	base := fmt.Sprintf("%s-action-%d", stageID, index+1)
	return ensureUnique(base, taken)
}

func ensureUnique(base string, taken map[string]int) string {
	candidate := base
	suffix := 1
	for {
		if _, exists := taken[candidate]; !exists {
			return candidate
		}
		candidate = fmt.Sprintf("%s-%d", base, suffix)
		suffix++
	}
}
