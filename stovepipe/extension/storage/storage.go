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

// Package storage defines the storage extension interfaces for the Stovepipe domain.
// The gateway owns the CommitStore (commit-status store and event log).
// The orchestrator owns the BatchStore (in-flight batches and pipeline working state).
// The two services share no storage; they communicate only through the messaging queue.
package storage

import (
	"errors"
	"fmt"
)

// ErrNotFound is returned by storage implementations when the requested record is not found.
var ErrNotFound = errors.New("record not found")

// IsNotFound returns true if any error in the chain is ErrNotFound.
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// WrapNotFound wraps ErrNotFound with the underlying implementation error.
func WrapNotFound(err error) error {
	return fmt.Errorf("%w: %w", ErrNotFound, err)
}

// ErrAlreadyExists is returned when attempting to create a record whose identity already exists.
var ErrAlreadyExists = errors.New("record already exists")

// ErrVersionMismatch is returned when an optimistic-locking CAS write finds the persisted
// version does not match the expected version. Callers should retry from a fresh read.
var ErrVersionMismatch = errors.New("version mismatch")
