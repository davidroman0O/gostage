// Package gostage provides a workflow orchestration and state management system.
//
// gostage enables building multi-stage stateful workflows with runtime modification
// capabilities. It provides a framework for organizing complex processes into
// manageable stages and actions with rich metadata support.
//
// Core components include:
//   - Workflows: The top-level container representing an entire process
//   - Stages: Sequential phases within a workflow, each containing multiple actions
//   - Actions: Individual units of work that implement specific tasks
//   - State Store: A type-safe key-value store for workflow data
//
// Key features include sequential execution, dynamic modification, tag-based organization,
// type-safe state storage, conditional execution, rich metadata, and serializable state.
package gostage
