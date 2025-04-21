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
//   - Deep cloning and copying between stores
//
// Store Cloning and Copying:
//
// The store provides several ways to duplicate or transfer data:
//
//   - Clone(): Creates a new store with deep copies of all entries from the original
//   - CloneFrom(): Static helper that creates a new store from an existing one
//   - CopyFrom(): Copies entries from a source store to an existing destination store,
//     preserving existing entries in the destination
//   - CopyFromWithOverwrite(): Similar to CopyFrom() but overwrites any existing
//     entries in the destination that have the same keys as in the source
//
// All of these functions perform deep copying to ensure that no references are
// shared between the original and new stores, maintaining proper isolation.
package store
