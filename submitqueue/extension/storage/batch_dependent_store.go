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

	"github.com/uber/submitqueue/submitqueue/entity"
)

// BatchDependentStore is an interface that defines methods for managing batch dependent information in the database.
//
// A BatchDependent is a reverse index ("batches that depend on me") paired one-to-one with a Batch.
// The batch-creation flow creates this row while the Batch is Creating and before making the Batch eligible for pipeline processing.
// A Creating Batch can briefly exist without its row; every Batch that reaches Created is guaranteed to have one.
type BatchDependentStore interface {
	// Get retrieves the batch dependent by batch ID.
	// If the batch contains no dependents, the returned BatchDependent will have an empty Dependents list.
	// Returns ErrNotFound if no reverse-index row exists for the batch.
	Get(ctx context.Context, batchID string) (entity.BatchDependent, error)

	// Create creates a new batch dependent.
	// Returns ErrAlreadyExists if the entry already exists for the given batch ID.
	Create(ctx context.Context, batchDependent entity.BatchDependent) error

	// Update replaces the non-key fields of a batch dependent and persists newVersion
	// if the current persisted version matches oldVersion. If versions do not match, returns ErrVersionMismatch.
	// Version arithmetic is owned by the caller; the store performs a pure conditional write.
	Update(ctx context.Context, batchDependent entity.BatchDependent, oldVersion, newVersion int32) error
}
