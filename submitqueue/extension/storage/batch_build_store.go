// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

//go:generate mockgen -source=batch_build_store.go -destination=mock/batch_build_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// BatchBuildStore manages immutable batch-to-build mappings.
type BatchBuildStore interface {
	// Create stores a batch-to-build mapping.
	// Returns ErrAlreadyExists if a mapping for the batch already exists.
	Create(ctx context.Context, batchBuild entity.BatchBuild) error

	// Get retrieves the mapping for a batch.
	// Returns ErrNotFound if the batch has no mapping.
	Get(ctx context.Context, batchID string) (entity.BatchBuild, error)
}
