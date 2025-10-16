package workflow

import rt "github.com/davidroman0O/gostage/v3/runtime"

// WorkflowMiddleware mirrors the runtime contract so callers can keep importing
// middleware helpers from the workflow package.
type WorkflowMiddleware = rt.WorkflowMiddleware

// StageMiddleware mirrors the runtime stage middleware signature.
type StageMiddleware = rt.StageMiddleware

// ActionMiddleware mirrors the runtime action middleware signature.
type ActionMiddleware = rt.ActionMiddleware
