package types

// Direct flags:
//
// `--process-id`: `pid` of the sub-process
// `--process-type`: worker type which helps the spawning router
// `--parent-port`: (optional) default
// `--parent-host`: (optional) default
//
//	Other properties will be fetched later on.
//
//	`ProcessInfo` is meant to be instantiated once as a pointer which will have a ready boolean
type ProcessInfo struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	ParentPort int    `json:"parentPort"`
	ParentHost int    `json:"parentHost"`

	Meta       map[string]interface{} `json:"meta"`
	Tags       []string               `json:"tags"`
	WorkflowID *int                   `json:"workflowID,omitempty"` // ephemeral worker

	Ready bool
}
