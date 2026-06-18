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

//go:generate mockgen -source=batch_store.go -destination=mock/batch_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// BatchStore is an interface that defines methods for managing batches in the database.
type BatchStore interface {
	// Get retrieves a batch by ID. Returns ErrNotFound if the batch is not found.
	Get(ctx context.Context, id string) (entity.Batch, error)

	// Create creates a new batch. The batch must have a unique ID already assigned.
	// Returns ErrAlreadyExists if a batch with the same ID already exists.
	Create(ctx context.Context, batch entity.Batch) error

	// UpdateState updates the state of a batch to newState and the version to newVersion
	// if the current persisted version matches oldVersion. If versions do not match, returns ErrVersionMismatch.
	// Version arithmetic is owned by the caller; the store performs a pure conditional write.
	UpdateState(ctx context.Context, id string, oldVersion, newVersion int32, newState entity.BatchState) error

	// UpdateScoreAndState atomically updates the score and state of a batch and the version to newVersion
	// if the current persisted version matches oldVersion. If versions do not match, returns ErrVersionMismatch.
	// Version arithmetic is owned by the caller; the store performs a pure conditional write.
	UpdateScoreAndState(ctx context.Context, id string, oldVersion, newVersion int32, score float64, newState entity.BatchState) error
}
