package storage

import "errors"
import "fmt"

// ErrNotFound is returned by storage implementations when the requested record is not found in the database.
var ErrNotFound = errors.New("record not found")

// IsNotFound returns true if any error in the error chain is a ErrNotFound.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// WrapNotFound wraps ErrNotFound with the original error from the storage implementation.
func WrapNotFound(err error) error {
	return fmt.Errorf("%w: %w", ErrNotFound, err)
}

// ErrVersionMismatch is returned by storage implementations when the expected entity version does not match the current version of the object.
// This is used to implement an optimistic locking mechanism, allowing multiple clients to update the same entity concurrently
// and either retry or implement idempotent operations.
var ErrVersionMismatch = errors.New("version mismatch")

// StoreFactory is an interface that defines methods for creating different stores..
// Each store is responsible for performing atomic storage operations for a specific entity type.
type StoreFactory interface {
	// GetRequestStore creates a new RequestStore instance.
	GetRequestStore() RequestStore

	// Close closes the store factory and all underlying connections. Should only be called once at the end of the program.
	Close() error
}
