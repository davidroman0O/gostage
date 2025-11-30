package kvstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPutGet(t *testing.T) {
	s := NewKVStore()
	require.NoError(t, s.Put("greeting", "hello"))

	val, err := Get[string](s, "greeting")
	require.NoError(t, err)
	require.Equal(t, "hello", val)
}

func TestGetTypeMismatch(t *testing.T) {
	s := NewKVStore()
	require.NoError(t, s.Put("answer", 42))

	_, err := Get[string](s, "answer")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTypeMismatch)
}

func TestGetNilPointer(t *testing.T) {
	s := NewKVStore()
	var ptr *int
	require.NoError(t, s.Put("ptr", ptr))

	got, err := Get[*int](s, "ptr")
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestDeleteAndClear(t *testing.T) {
	s := NewKVStore()
	require.NoError(t, s.Put("foo", "bar"))
	require.True(t, s.Delete("foo"))
	require.False(t, s.Delete("foo"))

	require.NoError(t, s.Put("foo", "bar"))
	s.Clear()
	_, err := Get[string](s, "foo")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestCopyFromWithOverwrite(t *testing.T) {
	src := NewKVStore()
	require.NoError(t, src.Put("count", 1))
	require.NoError(t, src.Put("meta", map[string]any{"k": "v"}))

	dst := NewKVStore()
	require.NoError(t, dst.Put("count", 0))

	copied, overwritten, err := dst.CopyFromWithOverwrite(src)
	require.NoError(t, err)
	require.Equal(t, 1, copied)
	require.Equal(t, 1, overwritten)

	// Ensure stored map is deep copied.
	exported := dst.ExportAll()
	require.NotNil(t, exported)
	meta := exported["meta"].(map[string]any)
	meta["k"] = "mutated"

	// Original value remains unchanged inside the store.
	again, err := Get[map[string]any](dst, "meta")
	require.NoError(t, err)
	require.Equal(t, "v", again["k"])
}

func TestExportAllReturnsCopy(t *testing.T) {
	s := NewKVStore()
	require.NoError(t, s.Put("numbers", []int{1, 2, 3}))

	exported := s.ExportAll()
	require.NotNil(t, exported)

	slice := exported["numbers"].([]int)
	slice[0] = 99

	stored, err := Get[[]int](s, "numbers")
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, stored)
}
