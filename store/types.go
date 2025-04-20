package store

import (
	"errors"
	"reflect"
	"time"
)

// Metadata holds additional information about a stored entry
type Metadata struct {
	Tags        []string               // Tags for categorizing and filtering entries
	Properties  map[string]interface{} // Custom properties for the entry
	Description string                 // Human-readable description
	CreatedAt   time.Time              // When the entry was created
	UpdatedAt   time.Time              // When the entry was last updated
}

// NewMetadata creates a new metadata object with default values
func NewMetadata() *Metadata {
	now := time.Now()
	return &Metadata{
		Tags:       []string{},
		Properties: make(map[string]interface{}),
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// AddTag adds a tag to the metadata
func (m *Metadata) AddTag(tag string) {
	// Check if tag already exists
	for _, t := range m.Tags {
		if t == tag {
			return
		}
	}
	m.Tags = append(m.Tags, tag)
	m.UpdatedAt = time.Now()
}

// RemoveTag removes a tag from the metadata
func (m *Metadata) RemoveTag(tag string) bool {
	for i, t := range m.Tags {
		if t == tag {
			m.Tags = append(m.Tags[:i], m.Tags[i+1:]...)
			m.UpdatedAt = time.Now()
			return true
		}
	}
	return false
}

// HasTag checks if the metadata has a specific tag
func (m *Metadata) HasTag(tag string) bool {
	for _, t := range m.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// HasAllTags checks if the metadata has all the specified tags
func (m *Metadata) HasAllTags(tags []string) bool {
	for _, requiredTag := range tags {
		found := false
		for _, entryTag := range m.Tags {
			if entryTag == requiredTag {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// HasAnyTag checks if the metadata has any of the specified tags
func (m *Metadata) HasAnyTag(tags []string) bool {
	for _, entryTag := range m.Tags {
		for _, searchTag := range tags {
			if entryTag == searchTag {
				return true
			}
		}
	}
	return false
}

// SetProperty sets a property in the metadata
func (m *Metadata) SetProperty(key string, value interface{}) {
	m.Properties[key] = value
	m.UpdatedAt = time.Now()
}

// GetProperty gets a property from the metadata
func (m *Metadata) GetProperty(key string) (interface{}, bool) {
	val, ok := m.Properties[key]
	return val, ok
}

// RemoveProperty removes a property from the metadata
func (m *Metadata) RemoveProperty(key string) bool {
	_, exists := m.Properties[key]
	if exists {
		delete(m.Properties, key)
		m.UpdatedAt = time.Now()
		return true
	}
	return false
}

// Entry holds the serialized value plus its concrete Go type.
type entry struct {
	typ       reflect.Type
	blob      []byte
	expiresAt *time.Time // nil means no expiration
	metadata  *Metadata  // nil means no metadata
}

// MergeStrategy determines how key collisions are handled during a merge.
type MergeStrategy int

const (
	// Skip keeps the original value in case of collision
	Skip MergeStrategy = iota
	// Overwrite replaces the original value with the new one in case of collision
	Overwrite
	// Error fails the merge if a collision is detected
	Error
)

// Common errors returned by the store
var (
	ErrNotFound     = errors.New("key not found")
	ErrTypeMismatch = errors.New("type mismatch on Get")
	ErrExpired      = errors.New("key has expired")
)
