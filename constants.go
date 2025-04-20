package gostate

// Store key prefixes for organizing different entities in the store
const (
	// PrefixWorkflow is used for workflow metadata
	PrefixWorkflow = "workflow:"

	// PrefixStage is used for stage metadata
	PrefixStage = "stage:"

	// PrefixAction is used for action metadata
	PrefixAction = "action:"

	// PrefixConfig is used for workflow configuration items
	PrefixConfig = "config:"

	// PrefixData is used for user data in the workflow
	PrefixData = "data:"

	// PrefixTemp is used for temporary data that shouldn't persist between executions
	PrefixTemp = "temp:"
)

// Common tags used across the workflow system
const (
	// TagSystem identifies system-managed entities
	TagSystem = "system"

	// TagCore identifies core/required components
	TagCore = "core"

	// TagDynamic identifies dynamically generated components
	TagDynamic = "dynamic"

	// TagDisabled identifies disabled components
	TagDisabled = "disabled"

	// TagTemporary identifies temporary components
	TagTemporary = "temporary"
)

// Common property keys used in metadata
const (
	// PropCreatedBy tracks who/what created an entity
	PropCreatedBy = "createdBy"

	// PropDependencies lists dependencies of an entity
	PropDependencies = "dependencies"

	// PropOrder tracks execution order for components
	PropOrder = "order"

	// PropStatus tracks the current status
	PropStatus = "status"

	// PropType indicates the type of an entity
	PropType = "type"
)

// Status values for workflow components
const (
	// StatusPending means not yet started
	StatusPending = "pending"

	// StatusRunning means currently in progress
	StatusRunning = "running"

	// StatusCompleted means successfully finished
	StatusCompleted = "completed"

	// StatusFailed means execution failed
	StatusFailed = "failed"

	// StatusSkipped means execution was skipped
	StatusSkipped = "skipped"
)
