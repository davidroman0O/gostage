// Package store provides a type-safe key-value store with advanced features.
//
// The store package implements a thread-safe, generic key-value store with
// additional capabilities such as metadata support, TTL, and type validation.
// It allows storing and retrieving Go values with their concrete types preserved.
//
// Core features include:
//   - Type-safe operations using generics
//   - Metadata for entries including tags and properties
//   - Time-to-live (TTL) expiration for entries
//   - JSON Schema support for type validation
//   - Thread-safe operations with concurrency support
package store
