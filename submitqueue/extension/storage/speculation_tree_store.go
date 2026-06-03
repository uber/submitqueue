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

//go:generate mockgen -source=speculation_tree_store.go -destination=mock/speculation_tree_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// SpeculationTreeStore is an interface that defines methods for managing speculation trees in the database.
type SpeculationTreeStore interface {
	// Get retrieves the speculation tree by batch ID.
	// Returns ErrNotFound if the speculation tree is not found.
	Get(ctx context.Context, batchID string) (entity.SpeculationTree, error)

	// Create creates a new speculation tree.
	// Returns ErrAlreadyExists if the entry already exists.
	Create(ctx context.Context, speculationTree entity.SpeculationTree) error

	// UpdateSpeculations updates the speculations of a speculation tree.
	// Returns ErrNotFound if the speculation tree is not found.
	UpdateSpeculations(ctx context.Context, batchID string, speculations []entity.SpeculationInfo) error
}
