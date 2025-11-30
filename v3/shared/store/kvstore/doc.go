// Package kvstore hosts the lightweight in-memory key/value cache used by the v3
// workflow runtime. It intentionally exposes only the behaviour required by the
// execution engine: typed Put/Get/Delete operations, safe concurrent access and
// utilities for cloning data between stores. Legacy concepts such as TTL,
// metadata bags and JSON schema generation have been dropped to keep the
// surface minimal and idiomatic.
package kvstore
