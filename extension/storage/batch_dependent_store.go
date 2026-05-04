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

//go:generate mockgen -source=batch_dependent_store.go -destination=mock/batch_dependent_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// BatchDependentStore is an interface that defines methods for managing batch dependent information in the database.
type BatchDependentStore interface {
	// Get retrieves the batch dependent by batch ID.
	// Returns ErrNotFound if the batch dependent is not found.
	Get(ctx context.Context, batchID string) (entity.BatchDependent, error)

	// Create creates a new batch dependent.
	// Returns ErrAlreadyExists if the entry already exists.
	Create(ctx context.Context, batchDependent entity.BatchDependent) error

	// UpdateDependents updates the dependents of a batch dependent and the version to newVersion
	// if the current persisted version matches oldVersion. If versions do not match, returns ErrVersionMismatch.
	// Version arithmetic is owned by the caller; the store performs a pure conditional write.
	UpdateDependents(ctx context.Context, batchID string, oldVersion, newVersion int32, dependents []string) error
}
