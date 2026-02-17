package storage

import "errors"

// ErrNotFound is returned by storage implementations when the requested record is not found in the database.
var ErrNotFound = errors.New("record not found")

// ErrVersionMismatch is returned by storage implementations when the expected entity version does not match the current version of the object.
// This is used to implement an optimistic locking mechanism, allowing multiple clients to update the same entity concurrently
// and either retry or implement idempotent operations.
var ErrVersionMismatch = errors.New("version mismatch")

// StoreFactory is an interface that defines methods for creating different stores..
// Each store is responsible for performing atomic storage operations for a specific entity type.
type StoreFactory interface {
	// GetRequestStore creates a new RequestStore instance.
	GetRequestStore() RequestStore
}
