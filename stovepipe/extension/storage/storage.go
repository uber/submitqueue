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

	"github.com/uber/submitqueue/platform/errs"
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
// retry or converge instead of overwriting a concurrent change. It is intrinsically a retryable infrastructure error.
var ErrVersionMismatch = errs.NewRetryableError(errors.New("version mismatch"))

// Config identifies the queue a Storage instance is resolved for. Like every
// other extension config, it carries only the queue name — everything an
// implementation needs beyond that is injected at construction by the
// integrator.
type Config struct {
	// QueueName is the name of the queue whose data the resolved Storage is
	// scoped to.
	QueueName string
}

// Factory resolves the queue-scoped Storage aggregate for a queue. Mirrors the
// extension contract: the host wiring decides which backend serves which
// queue; implementations bind the queue over their backend so a resolved
// instance can only read and write that queue's data. Stovepipe has no
// cross-queue read paths, so every store lives inside the aggregate.
type Factory interface {
	// For returns the Storage aggregate bound to the queue named in config.
	For(config Config) (Storage, error)
}

// Storage aggregates the queue-scoped entity stores into a single injectable
// dependency. An instance is resolved per queue through Factory and is bound
// to that queue: entity arguments whose queue disagrees with the binding are
// rejected, and reads never surface another queue's records.
type Storage interface {
	// GetRequestStore returns the RequestStore instance.
	GetRequestStore() RequestStore

	// GetRequestURIStore returns the RequestURIStore instance.
	GetRequestURIStore() RequestURIStore

	// GetRequestLogStore returns the RequestLogStore instance.
	GetRequestLogStore() RequestLogStore

	// GetQueueStore returns the QueueStore instance.
	GetQueueStore() QueueStore

	// GetBuildStore returns the BuildStore instance.
	GetBuildStore() BuildStore

	// GetValidationFactStore returns the ValidationFactStore instance.
	GetValidationFactStore() ValidationFactStore
}
