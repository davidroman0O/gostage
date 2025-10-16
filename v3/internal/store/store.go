package store

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/sasha-s/go-deadlock"
)

var (
	// ErrNotFound indicates the requested key is absent.
	ErrNotFound = errors.New("store: key not found")
	// ErrTypeMismatch indicates the stored value cannot be represented as the requested type.
	ErrTypeMismatch = errors.New("store: type mismatch")
	// ErrEmptyKey is returned when callers attempt to use an empty key.
	ErrEmptyKey = errors.New("store: key cannot be empty")
)

type entry struct {
	typ   reflect.Type
	value any
}

// KVStore is a threadsafe in-memory key/value store used by the runtime.
type KVStore struct {
	mu   deadlock.RWMutex
	data map[string]entry
}

// NewKVStore constructs an empty store.
func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string]entry)}
}

// Put stores a value under the provided key.
func (s *KVStore) Put(key string, value any) error {
	if key == "" {
		return ErrEmptyKey
	}

	s.mu.Lock()
	if s.data == nil {
		s.data = make(map[string]entry)
	}
	s.data[key] = entry{typ: reflect.TypeOf(value), value: value}
	s.mu.Unlock()
	return nil
}

// Delete removes the provided key if it exists and returns true when a value was removed.
func (s *KVStore) Delete(key string) bool {
	if key == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.data == nil {
		return false
	}

	if _, ok := s.data[key]; ok {
		delete(s.data, key)
		return true
	}
	return false
}

// Clear removes all keys from the store.
func (s *KVStore) Clear() {
	s.mu.Lock()
	s.data = make(map[string]entry)
	s.mu.Unlock()
}

// Count reports how many keys are currently stored.
func (s *KVStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// ListKeys returns a best-effort snapshot of the stored keys.
func (s *KVStore) ListKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.data) == 0 {
		return nil
	}

	keys := make([]string, 0, len(s.data))
	for key := range s.data {
		keys = append(keys, key)
	}
	return keys
}

// Get retrieves a typed value from the store.
func Get[T any](s *KVStore, key string) (T, error) {
	var zero T
	if s == nil {
		return zero, ErrNotFound
	}
	if key == "" {
		return zero, ErrEmptyKey
	}

	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return zero, ErrNotFound
	}

	want := reflect.TypeOf((*T)(nil)).Elem()

	if e.value == nil {
		if !isNilable(want) {
			return zero, fmt.Errorf("%w: stored <nil>, requested %v", ErrTypeMismatch, want)
		}
		return zero, nil
	}

	if e.typ != nil && !typesCompatible(e.typ, want) {
		return zero, fmt.Errorf("%w: stored %v, requested %v", ErrTypeMismatch, e.typ, want)
	}

	val, ok := e.value.(T)
	if !ok {
		// As a fallback, attempt to convert when the request is for an interface type.
		if want.Kind() == reflect.Interface {
			if reflect.TypeOf(e.value).Implements(want) {
				converted := reflect.ValueOf(e.value).Convert(want).Interface()
				if out, ok := converted.(T); ok {
					return out, nil
				}
			}
		}
		return zero, fmt.Errorf("%w: stored %T, requested %v", ErrTypeMismatch, e.value, want)
	}
	return val, nil
}

// ExportAll returns a clone of the underlying map so callers cannot mutate runtime state.
func (s *KVStore) ExportAll() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.data) == 0 {
		return nil
	}

	out := make(map[string]any, len(s.data))
	for key, entry := range s.data {
		out[key] = cloneValue(entry.value)
	}
	return out
}

// CopyFromWithOverwrite copies all entries from source into the store.
// Destination values are deep cloned to avoid sharing references with source.
func (s *KVStore) CopyFromWithOverwrite(source *KVStore) (copied int, overwritten int, err error) {
	if source == nil {
		return 0, 0, nil
	}

	source.mu.RLock()
	defer source.mu.RUnlock()

	s.mu.Lock()
	if s.data == nil {
		s.data = make(map[string]entry, len(source.data))
	}

	for key, src := range source.data {
		_, exists := s.data[key]
		s.data[key] = entry{typ: src.typ, value: cloneValue(src.value)}
		if exists {
			overwritten++
		} else {
			copied++
		}
	}
	s.mu.Unlock()

	return copied, overwritten, nil
}

// typesCompatible reports whether the stored type can be represented as the requested type.
func typesCompatible(stored, want reflect.Type) bool {
	if want == nil || stored == nil {
		return true
	}
	if stored == want {
		return true
	}
	if want.Kind() == reflect.Interface {
		if want.NumMethod() == 0 {
			return true
		}
		return stored.Implements(want)
	}
	return false
}

// isNilable reports whether values of the provided type can be nil.
func isNilable(t reflect.Type) bool {
	if t == nil {
		return true
	}
	switch t.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}

func cloneValue(value any) any {
	if value == nil {
		return nil
	}
	v := reflect.ValueOf(value)
	cloned := cloneRecursive(v)
	if !cloned.IsValid() {
		return nil
	}
	return cloned.Interface()
}

func cloneRecursive(v reflect.Value) reflect.Value {
	if !v.IsValid() {
		return v
	}

	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		elem := cloneRecursive(v.Elem())
		dst := reflect.New(v.Type().Elem())
		assignValue(dst.Elem(), elem)
		return dst
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		elem := cloneRecursive(v.Elem())
		if elem.Type().AssignableTo(v.Type()) {
			return elem
		}
		if elem.Type().ConvertibleTo(v.Type()) {
			return elem.Convert(v.Type())
		}
		return elem
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		dst := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			key := iter.Key()
			dst.SetMapIndex(key, cloneRecursive(iter.Value()))
		}
		return dst
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		dst := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			dst.Index(i).Set(cloneRecursive(v.Index(i)))
		}
		return dst
	case reflect.Array:
		dst := reflect.New(v.Type()).Elem()
		for i := 0; i < v.Len(); i++ {
			dst.Index(i).Set(cloneRecursive(v.Index(i)))
		}
		return dst
	case reflect.Struct:
		dst := reflect.New(v.Type()).Elem()
		dst.Set(v)
		return dst
	default:
		return v
	}
}

func assignValue(dst, src reflect.Value) {
	if !src.IsValid() {
		dst.Set(reflect.Zero(dst.Type()))
		return
	}
	if src.Type() == dst.Type() || src.Type().AssignableTo(dst.Type()) {
		dst.Set(src)
		return
	}
	if src.Type().ConvertibleTo(dst.Type()) {
		dst.Set(src.Convert(dst.Type()))
		return
	}
	dst.Set(reflect.Zero(dst.Type()))
}
