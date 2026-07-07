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

// ErrAlreadyExists is returned by storage implementations when attempting to create a record that already exists.
var ErrAlreadyExists = errors.New("record already exists")

// ErrVersionMismatch is returned by storage implementations when a conditional (CAS) update finds that
// the stored version does not match the expected version. It backs optimistic locking, letting callers
// retry or converge instead of overwriting a concurrent change.
var ErrVersionMismatch = errors.New("version mismatch")

// Storage is a factory interface that aggregates all entity stores into a single injectable dependency.
type Storage interface {
	// GetRequestStore returns the RequestStore instance.
	GetRequestStore() RequestStore

	// GetRequestURIStore returns the RequestURIStore instance.
	GetRequestURIStore() RequestURIStore

	// Close closes the storage and all underlying connections. Should only be called once at the end of the program.
	Close() error
}
