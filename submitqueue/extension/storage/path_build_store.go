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

//go:generate mockgen -source=path_build_store.go -destination=mock/path_build_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// PathBuildStore resolves one attempt of one speculation path to the build
// started for it, keyed by (path ID, attempt).
//
// It exists because the runner chooses the build ID: a caller holding a path
// and an attempt cannot derive it, and there is no lookup by attribute in this
// contract. Promoting the relationship to a keyed record is the mechanism for a
// reverse lookup here, not a workaround.
//
// A record is write-once and complete from the start: it is created only once
// the runner has named the build, and never updated afterwards. It answers two
// questions — absent means no build is recorded for the attempt, present names
// the attempt's build permanently. A retried path is a new attempt under a
// different key.
//
// Because creation is the only write, a duplicate Create is how concurrent
// dispatches for the same attempt are decided: the first insert wins, and
// ErrAlreadyExists tells the loser the attempt's build is someone else's.
type PathBuildStore interface {
	// Get resolves an attempt to its build.
	// Returns ErrNotFound if no build has been recorded for the attempt.
	Get(ctx context.Context, pathID string, attempt int) (entity.PathBuild, error)

	// Create records the build for an attempt, permanently.
	// Returns ErrAlreadyExists if the attempt already has a build, which means
	// a concurrent dispatch recorded its own first.
	Create(ctx context.Context, pathBuild entity.PathBuild) error
}
