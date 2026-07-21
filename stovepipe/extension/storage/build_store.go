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

	"github.com/uber/submitqueue/stovepipe/entity"
)

// BuildStore persists builds, keyed by build ID (the runner-assigned id minted at Trigger).
// build is the sole creator of a row; buildsignal is the sole writer of Status/Version
// afterward. No reverse index from Request to its builds is needed — buildsignal and record
// reach a build by the id carried in their messages.
type BuildStore interface {
	// Create persists a new build. The build must have a unique ID already assigned.
	// Returns ErrAlreadyExists if a build with the same ID already exists.
	Create(ctx context.Context, build entity.Build) error

	// Get retrieves a build by ID. Returns errs.ErrNotFound if the build is not found.
	Get(ctx context.Context, id string) (entity.Build, error)

	// Update persists the mutable fields of build if the currently stored version matches
	// oldVersion, writing newVersion as the new version. Returns errs.ErrVersionMismatch if the
	// stored version does not match (including when the build does not exist).
	//
	// Version arithmetic is owned by the caller: it computes newVersion (typically oldVersion+1)
	// and only assigns build.Version = newVersion after this call succeeds. The store performs
	// a pure conditional write and does not read build.Version.
	Update(ctx context.Context, build entity.Build, oldVersion, newVersion int32) error
}
