// Copyright (c) 2025 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

//go:generate mockgen -source=storage.go -destination=mock/storage_mock.go -package=mock

import (
	"errors"
	"fmt"
)

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

	// GetBatchStore returns the BatchStore instance.
	GetBatchStore() BatchStore

	// GetBatchDependentStore returns the BatchDependentStore instance.
	GetBatchDependentStore() BatchDependentStore

	// GetBuildStore returns the BuildStore instance.
	GetBuildStore() BuildStore

	// GetSpeculationTreeStore returns the SpeculationTreeStore instance.
	GetSpeculationTreeStore() SpeculationTreeStore

	// GetRequestLogStore returns the RequestLogStore instance.
	GetRequestLogStore() RequestLogStore

	// Close closes the storage and all underlying connections. Should only be called once at the end of the program.
	Close() error
}

// Factory returns the Storage backing a named queue. It exists so a single
// queue can be migrated to a different backend without affecting others; the
// default implementation (NewStaticFactory) returns the same Storage for
// every queue. Callers resolve the Storage per message from the queue name
// carried in the message envelope, before any entity lookup.
type Factory interface {
	// For returns the Storage for the named queue. An empty name selects the
	// default backend. It returns an error if no backend is configured for the
	// queue.
	For(name string) (Storage, error)
}
