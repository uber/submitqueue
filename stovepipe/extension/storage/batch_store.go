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

	"github.com/uber/submitqueue/stovepipe/entity"
)

// BatchStore is the orchestrator-owned store for in-flight validation batches.
// A batch represents a contiguous range of trunk commits under validation.
// Bisection reuses the same type — sub-range probes are ordinary batches
// driven through the same speculate→build→buildsignal→bisect loop.
type BatchStore interface {
	// Get retrieves a batch by ID. Returns ErrNotFound if no record exists.
	Get(ctx context.Context, id string) (entity.Batch, error)

	// Create records a new batch with status BatchStatusPending.
	// Returns ErrAlreadyExists if a batch with the same ID already exists.
	Create(ctx context.Context, batch entity.Batch) error

	// UpdateStatus updates the batch's status and advances the version from
	// oldVersion to newVersion. Returns ErrVersionMismatch if the current
	// persisted version does not match oldVersion; the caller must re-read and retry.
	// Version arithmetic is owned by the caller (controller); the store performs
	// a pure conditional write.
	UpdateStatus(ctx context.Context, id string, oldVersion, newVersion int32, status entity.BatchStatus) error
}
