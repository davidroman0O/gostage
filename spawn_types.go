package gostage

// SpawnJob represents one ForEach item to execute in a child process.
// The exported fields are accessible to ChildMiddleware implementations
// and the spawn sub-package.
type SpawnJob struct {
	ID             string
	RunID          RunID             // the run this job belongs to
	TaskName       string
	StoreData      map[string][]byte // serialized store snapshot including item/index
	DefinitionJSON []byte            // serialized SubWorkflowDef for multi-stage children
	resultCh       chan *SpawnResult  // child sends result here (buffered, size 1)
	token          string            // per-job auth token
}

// NewSpawnJob creates a SpawnJob with the given parameters.
// Used by the spawn sub-package.
func NewSpawnJob(id string, runID RunID, taskName string, storeData map[string][]byte, token string) *SpawnJob {
	return &SpawnJob{
		ID:        id,
		RunID:     runID,
		TaskName:  taskName,
		StoreData: storeData,
		resultCh:  make(chan *SpawnResult, 1),
		token:     token,
	}
}

// Token returns the per-job auth token.
func (j *SpawnJob) Token() string { return j.token }

// ResultCh returns the channel for receiving the child process result.
func (j *SpawnJob) ResultCh() chan *SpawnResult { return j.resultCh }

// SpawnResult holds the outcome of a child process execution.
type SpawnResult struct {
	StoreData  map[string][]byte // final store from child
	Err        string            // error message, empty on success
	BailReason string            // bail reason if the child bailed, empty otherwise
}
