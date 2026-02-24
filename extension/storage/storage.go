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

// ErrAlreadyExists is returned by storage implementations when attempting to create a record with an ID that already exists.
var ErrAlreadyExists = errors.New("record already exists")

// ErrVersionMismatch is returned by storage implementations when the expected entity version does not match the current version of the object.
// This is used to implement an optimistic locking mechanism, allowing multiple clients to update the same entity concurrently
// and either retry or implement idempotent operations.
var ErrVersionMismatch = errors.New("version mismatch")

// Storage is a factory interface that aggregates all entity stores into a single injectable dependency.
type Storage interface {
	// GetRequestStore returns the RequestStore instance.
	GetRequestStore() RequestStore

	// GetChangeProviderStore returns the ChangeProviderStore instance.
	GetChangeProviderStore() ChangeProviderStore

	// Close closes the storage and all underlying connections. Should only be called once at the end of the program.
	Close() error
}
