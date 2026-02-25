package gostage

import (
	"encoding/json"
	"fmt"
	"time"
)

// --- Step kind name mapping ---

var StepKindNames = map[StepKind]string{
	StepSingle:   "single",
	StepParallel: "parallel",
	StepBranch:   "branch",
	StepForEach:  "forEach",
	StepMap:      "map",
	StepDoUntil:  "doUntil",
	StepDoWhile:  "doWhile",
	StepSub:      "sub",
	StepSleep:    "sleep",
	StepStage:    "stage",
}

var StepKindFromName = map[string]StepKind{
	"single":   StepSingle,
	"parallel": StepParallel,
	"branch":   StepBranch,
	"forEach":  StepForEach,
	"map":      StepMap,
	"doUntil":  StepDoUntil,
	"doWhile":  StepDoWhile,
	"sub":      StepSub,
	"sleep":    StepSleep,
	"stage":    StepStage,
}

// --- Serializable definition types ---

// RefDef is a JSON-serializable step reference.
type RefDef struct {
	TaskName    string       `json:"task_name,omitempty"`
	SubWorkflow *WorkflowDef `json:"sub_workflow,omitempty"`
}

// CaseDef is a JSON-serializable branch case.
type CaseDef struct {
	ConditionName string `json:"condition_name,omitempty"`
	Ref           RefDef `json:"ref"`
	IsDefault     bool   `json:"is_default,omitempty"`
}

// StepDef is a JSON-serializable step definition supporting all 10 step kinds.
type StepDef struct {
	Kind string   `json:"kind"`
	Name string   `json:"name"`
	Tags []string `json:"tags,omitempty"`

	// StepSingle
	TaskName string `json:"task_name,omitempty"`

	// StepParallel / StepStage
	Refs []RefDef `json:"refs,omitempty"`

	// StepBranch
	Cases []CaseDef `json:"cases,omitempty"`

	// StepForEach
	CollectionKey string  `json:"collection_key,omitempty"`
	ForEachRef    *RefDef `json:"foreach_ref,omitempty"`
	Concurrency   int     `json:"concurrency,omitempty"`
	UseSpawn      bool    `json:"use_spawn,omitempty"`

	// StepMap
	MapFnName string `json:"map_fn_name,omitempty"`

	// StepDoUntil / StepDoWhile
	LoopRef      *RefDef `json:"loop_ref,omitempty"`
	LoopCondName string  `json:"loop_cond_name,omitempty"`

	// StepSub
	SubWorkflow *WorkflowDef `json:"sub_workflow,omitempty"`

	// StepSleep
	SleepDuration string `json:"sleep_duration,omitempty"`
}

// WorkflowDef is a JSON-serializable workflow definition supporting all 10 step kinds.
type WorkflowDef struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Steps        []StepDef      `json:"steps"`
	InitialStore map[string]any `json:"initial_store,omitempty"`
}

// SubWorkflowDef is kept as a type alias for backward compatibility with spawn.go and child.go.
type SubWorkflowDef = WorkflowDef

// --- Serialization: Workflow → Definition ---

// WorkflowToDefinition converts a compiled Workflow to a serializable WorkflowDef.
// All 10 step kinds are supported. Steps using unnamed closures (anonymous When(),
// Map(), DoUntil(), DoWhile()) return an error — use the *Named variants instead.
func WorkflowToDefinition(wf *Workflow) (*WorkflowDef, error) {
	def := &WorkflowDef{
		ID:   wf.ID,
		Name: wf.Name,
	}

	for _, s := range wf.steps {
		kindName, ok := StepKindNames[s.kind]
		if !ok {
			return nil, fmt.Errorf("step %q: unknown kind %d", s.name, s.kind)
		}

		sd := StepDef{
			Kind: kindName,
			Name: s.name,
			Tags: s.tags,
		}

		switch s.kind {
		case StepSingle:
			sd.TaskName = s.taskName

		case StepParallel, StepStage:
			for _, ref := range s.refs {
				rd, err := refToRefDef(ref)
				if err != nil {
					return nil, fmt.Errorf("step %q ref: %w", s.name, err)
				}
				sd.Refs = append(sd.Refs, rd)
			}

		case StepBranch:
			for _, c := range s.cases {
				cd := CaseDef{
					IsDefault: c.isDefault,
				}
				if !c.isDefault {
					if c.condName == "" {
						return nil, fmt.Errorf("step %q: cannot serialize unnamed condition; use WhenNamed()", s.name)
					}
					cd.ConditionName = c.condName
				}
				rd, err := refToRefDef(c.ref)
				if err != nil {
					return nil, fmt.Errorf("step %q case ref: %w", s.name, err)
				}
				cd.Ref = rd
				sd.Cases = append(sd.Cases, cd)
			}

		case StepForEach:
			sd.CollectionKey = s.collectionKey
			sd.Concurrency = s.concurrency
			sd.UseSpawn = s.useSpawn
			rd, err := refToRefDef(s.forEachRef)
			if err != nil {
				return nil, fmt.Errorf("step %q forEach ref: %w", s.name, err)
			}
			sd.ForEachRef = &rd

		case StepMap:
			if s.mapFnName == "" {
				return nil, fmt.Errorf("step %q: cannot serialize unnamed map function; use MapNamed()", s.name)
			}
			sd.MapFnName = s.mapFnName

		case StepDoUntil, StepDoWhile:
			if s.loopCondName == "" {
				return nil, fmt.Errorf("step %q: cannot serialize unnamed loop condition; use DoUntilNamed()/DoWhileNamed()", s.name)
			}
			sd.LoopCondName = s.loopCondName
			rd, err := refToRefDef(s.loopRef)
			if err != nil {
				return nil, fmt.Errorf("step %q loop ref: %w", s.name, err)
			}
			sd.LoopRef = &rd

		case StepSub:
			if s.subWorkflow == nil {
				return nil, fmt.Errorf("step %q: sub-workflow is nil", s.name)
			}
			subDef, err := WorkflowToDefinition(s.subWorkflow)
			if err != nil {
				return nil, fmt.Errorf("step %q sub-workflow: %w", s.name, err)
			}
			sd.SubWorkflow = subDef

		case StepSleep:
			sd.SleepDuration = s.sleepDuration.String()
		}

		def.Steps = append(def.Steps, sd)
	}

	return def, nil
}

// refToRefDef converts a StepRef to a serializable RefDef.
func refToRefDef(ref StepRef) (RefDef, error) {
	if ref.subWorkflow != nil {
		subDef, err := WorkflowToDefinition(ref.subWorkflow)
		if err != nil {
			return RefDef{}, err
		}
		return RefDef{SubWorkflow: subDef}, nil
	}
	return RefDef{TaskName: ref.taskName}, nil
}

// --- Deserialization: Definition → Workflow ---

// maxWorkflowDepth limits recursion depth when deserializing nested workflow definitions.
const maxWorkflowDepth = 64

// NewWorkflowFromDef rebuilds a Workflow from a serialized definition.
// Tasks are resolved via the task registry, conditions via the condition registry,
// and map functions via the map function registry. Returns an error if any
// required function is not registered.
func NewWorkflowFromDef(def *WorkflowDef) (*Workflow, error) {
	return newWorkflowFromDef(def, maxWorkflowDepth)
}

func newWorkflowFromDef(def *WorkflowDef, depth int) (*Workflow, error) {
	if depth <= 0 {
		return nil, fmt.Errorf("workflow nesting exceeds maximum depth (%d)", maxWorkflowDepth)
	}
	wf := &Workflow{
		ID:    def.ID,
		Name:  def.Name,
		state: newRunState("", nil),
		steps: make([]step, 0, len(def.Steps)),
	}

	for k, v := range def.InitialStore {
		wf.state.Set(k, v)
	}

	for i, sd := range def.Steps {
		kind, ok := StepKindFromName[sd.Kind]
		if !ok {
			return nil, fmt.Errorf("step %q: unknown kind %q", sd.Name, sd.Kind)
		}

		s := step{
			id:   fmt.Sprintf("%s:%d", def.ID, i),
			kind: kind,
			name: sd.Name,
			tags: sd.Tags,
		}

		switch kind {
		case StepSingle:
			if lookupTask(sd.TaskName) == nil {
				return nil, fmt.Errorf("task %q not registered", sd.TaskName)
			}
			s.taskName = sd.TaskName

		case StepParallel, StepStage:
			refs, err := refDefsToRefs(sd.Refs, depth)
			if err != nil {
				return nil, fmt.Errorf("step %q: %w", sd.Name, err)
			}
			s.refs = refs

		case StepBranch:
			for _, cd := range sd.Cases {
				bc := BranchCase{
					isDefault: cd.IsDefault,
				}
				if !cd.IsDefault {
					fn := lookupCondition(cd.ConditionName)
					if fn == nil {
						return nil, fmt.Errorf("condition %q not registered", cd.ConditionName)
					}
					bc.condition = fn
					bc.condName = cd.ConditionName
				}
				ref, err := refDefToRef(cd.Ref, depth)
				if err != nil {
					return nil, fmt.Errorf("step %q case: %w", sd.Name, err)
				}
				bc.ref = ref
				s.cases = append(s.cases, bc)
			}

		case StepForEach:
			s.collectionKey = sd.CollectionKey
			s.concurrency = sd.Concurrency
			s.useSpawn = sd.UseSpawn
			if sd.ForEachRef != nil {
				ref, err := refDefToRef(*sd.ForEachRef, depth)
				if err != nil {
					return nil, fmt.Errorf("step %q forEach ref: %w", sd.Name, err)
				}
				s.forEachRef = ref
			}

		case StepMap:
			fn := lookupMapFn(sd.MapFnName)
			if fn == nil {
				return nil, fmt.Errorf("map function %q not registered", sd.MapFnName)
			}
			s.mapFn = fn
			s.mapFnName = sd.MapFnName

		case StepDoUntil, StepDoWhile:
			fn := lookupCondition(sd.LoopCondName)
			if fn == nil {
				return nil, fmt.Errorf("condition %q not registered", sd.LoopCondName)
			}
			s.loopCond = fn
			s.loopCondName = sd.LoopCondName
			if sd.LoopRef != nil {
				ref, err := refDefToRef(*sd.LoopRef, depth)
				if err != nil {
					return nil, fmt.Errorf("step %q loop ref: %w", sd.Name, err)
				}
				s.loopRef = ref
			}

		case StepSub:
			if sd.SubWorkflow != nil {
				subWf, err := newWorkflowFromDef(sd.SubWorkflow, depth-1)
				if err != nil {
					return nil, fmt.Errorf("step %q sub: %w", sd.Name, err)
				}
				s.subWorkflow = subWf
			}

		case StepSleep:
			d, err := time.ParseDuration(sd.SleepDuration)
			if err != nil {
				return nil, fmt.Errorf("step %q: parse sleep duration %q: %w", sd.Name, sd.SleepDuration, err)
			}
			s.sleepDuration = d
		}

		wf.steps = append(wf.steps, s)
	}

	return wf, nil
}

// refDefToRef converts a RefDef to a StepRef by looking up registered tasks.
func refDefToRef(rd RefDef, depth int) (StepRef, error) {
	if rd.SubWorkflow != nil {
		subWf, err := newWorkflowFromDef(rd.SubWorkflow, depth-1)
		if err != nil {
			return StepRef{}, err
		}
		return StepRef{subWorkflow: subWf}, nil
	}
	if lookupTask(rd.TaskName) == nil {
		return StepRef{}, fmt.Errorf("task %q not registered", rd.TaskName)
	}
	return StepRef{taskName: rd.TaskName}, nil
}

// refDefsToRefs converts a slice of RefDef to StepRefs.
func refDefsToRefs(rds []RefDef, depth int) ([]StepRef, error) {
	refs := make([]StepRef, 0, len(rds))
	for _, rd := range rds {
		ref, err := refDefToRef(rd, depth)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// --- Marshal/Unmarshal helpers ---

// MarshalWorkflowDefinition serializes a WorkflowDef to JSON bytes.
func MarshalWorkflowDefinition(def *WorkflowDef) ([]byte, error) {
	return json.Marshal(def)
}

// UnmarshalWorkflowDefinition deserializes JSON bytes to a WorkflowDef.
func UnmarshalWorkflowDefinition(data []byte) (*WorkflowDef, error) {
	var def WorkflowDef
	if err := json.Unmarshal(data, &def); err != nil {
		return nil, fmt.Errorf("unmarshal workflow definition: %w", err)
	}
	return &def, nil
}
