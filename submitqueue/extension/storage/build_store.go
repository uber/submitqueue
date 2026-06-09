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

//go:generate mockgen -source=build_store.go -destination=mock/build_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// BuildStore is an interface that defines methods for managing builds in the database.
type BuildStore interface {
	// Get retrieves a build by ID. Returns ErrNotFound if the build is not found.
	Get(ctx context.Context, id string) (entity.Build, error)

	// GetByBatchID retrieves the single build scheduled for the given batch
	// (build.ID is the runner-assigned ID, so callers that only hold a batch ID
	// cannot use Get).
	// Returns ErrNotFound if no build exists for the batch.
	GetByBatchID(ctx context.Context, batchID string) (entity.Build, error)

	// Create creates a new build. The build must have a unique ID and batch ID.
	// Returns ErrAlreadyExists if either uniqueness constraint is violated.
	Create(ctx context.Context, build entity.Build) error

	// UpdateStatus updates the status of a build.
	UpdateStatus(ctx context.Context, id string, newStatus entity.BuildStatus) error
}
