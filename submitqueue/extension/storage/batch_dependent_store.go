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
// The batch-creation flow always calls Create here before creating the Batch itself, so every active
// Batch is guaranteed to have a corresponding BatchDependent row. Lookups via Get are only performed
// for batch IDs returned from the active-batch set, meaning a missing row indicates data corruption or
// out-of-band manipulation rather than a normal "not found" outcome. errs.ErrNotFound is therefore part of
// the contract for completeness but is not expected to be returned in steady-state operation.
type BatchDependentStore interface {
	// Get retrieves the batch dependent by batch ID.
	// If the batch contains no dependents, the returned BatchDependent will have an empty Dependents list.
	// Returns errs.ErrNotFound if the batch itself is not found, which should never happen in steady-state system and
	// therefore does not need a special handling.
	Get(ctx context.Context, batchID string) (entity.BatchDependent, error)

	// Create creates a new batch dependent.
	// Returns ErrAlreadyExists if the entry already exists for the given batch ID.
	Create(ctx context.Context, batchDependent entity.BatchDependent) error

	// UpdateDependents updates the dependents of a batch dependent and the version to newVersion
	// if the current persisted version matches oldVersion. If versions do not match, returns errs.ErrVersionMismatch.
	// Version arithmetic is owned by the caller; the store performs a pure conditional write.
	UpdateDependents(ctx context.Context, batchID string, oldVersion, newVersion int32, dependents []string) error
}
