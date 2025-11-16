// Package store provides a key-value store interface for workflow execution state.
package store

import (
	"errors"

	internal "github.com/davidroman0O/gostage/v3/layers/foundation/store"
)

// Handle wraps the internal KV store used during workflow execution.
type Handle struct {
	kv *internal.KVStore
}

// Provider exposes a Store() Handle method (satisfied by runtime.Context).
type Provider interface {
	Store() Handle
}

// ErrNilHandle is returned when the caller attempts to use an uninitialised handle.
var ErrNilHandle = errors.New("store: handle is nil")

// New returns a fresh Handle backed by an empty KV store.
func New() Handle {
	return Handle{kv: internal.NewKVStore()}
}

// FromInternal wraps an existing internal KV store.
func FromInternal(kv *internal.KVStore) Handle {
	return Handle{kv: kv}
}

// IsZero reports whether the handle references a backing store.
func (h Handle) IsZero() bool { return h.kv == nil }

// Store allows Handle itself to satisfy the Provider interface.
func (h Handle) Store() Handle { return h }

// Put stores a value under key. Accepts either a Handle or a Provider.
func Put(target Provider, key string, value any) error {
	h := target.Store()
	if h.kv == nil {
		return ErrNilHandle
	}
	return h.kv.Put(key, value)
}

// Delete removes a key from the store.
func Delete(target Provider, key string) bool {
	h := target.Store()
	if h.kv == nil {
		return false
	}
	return h.kv.Delete(key)
}

// Get retrieves a typed value from the store.
func Get[T any](target Provider, key string) (T, error) {
	var zero T
	h := target.Store()
	if h.kv == nil {
		return zero, ErrNilHandle
	}
	return internal.Get[T](h.kv, key)
}

// MustGet retrieves the value and panics on error (useful for tests).
func MustGet[T any](target Provider, key string) T {
	val, err := Get[T](target, key)
	if err != nil {
		panic(err)
	}
	return val
}

// ExportAll returns a deep copy of the current store contents.
func ExportAll(target Provider) map[string]any {
	h := target.Store()
	if h.kv == nil {
		return nil
	}
	return h.kv.ExportAll()
}

// CopyFromWithOverwrite copies entries from src into dst, overwriting existing keys.
func CopyFromWithOverwrite(dst Provider, src Provider) (copied int, overwritten int, err error) {
	dstHandle := dst.Store()
	srcHandle := src.Store()
	if dstHandle.kv == nil {
		return 0, 0, ErrNilHandle
	}
	if srcHandle.kv == nil {
		return 0, 0, nil
	}
	return dstHandle.kv.CopyFromWithOverwrite(srcHandle.kv)
}

// Clone returns a deep copy of the underlying store.
func Clone(target Provider) Handle {
	h := target.Store()
	if h.kv == nil {
		return Handle{}
	}
	clone := internal.NewKVStore()
	_, _, _ = clone.CopyFromWithOverwrite(h.kv)
	return Handle{kv: clone}
}
