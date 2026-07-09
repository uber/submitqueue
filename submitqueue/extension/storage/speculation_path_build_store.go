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

//go:generate mockgen -source=speculation_path_build_store.go -destination=mock/speculation_path_build_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// SpeculationPathBuildStore is an interface that defines methods for managing
// the path->build mapping in the database. A row is written only by the build
// controller, when it triggers a build for a speculation path, and is
// immutable once written: a path maps to at most one build. If the mapping
// for a path is lost (e.g. redelivery races), the build controller
// re-triggers a build and the new mapping wins — so Create on an existing
// PathID returns ErrAlreadyExists, and the caller treats the already-persisted
// row as truth rather than overwriting it.
type SpeculationPathBuildStore interface {
	// Create creates a new path->build mapping. Returns ErrAlreadyExists if a
	// mapping for the given PathID already exists.
	Create(ctx context.Context, pathBuild entity.SpeculationPathBuild) error

	// Get retrieves the path->build mapping for the given path ID. Returns
	// ErrNotFound if no mapping exists for pathID.
	Get(ctx context.Context, pathID string) (entity.SpeculationPathBuild, error)
}
