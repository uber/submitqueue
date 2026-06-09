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

//go:generate mockgen -source=commit_store.go -destination=mock/commit_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// CommitStore is the gateway-owned store for trunk commit health state.
// It is the authoritative record of each commit's validation status and is the
// only storage the gateway's GetStatus RPC reads from.
//
// The (Repository, Branch, SHA) triple is the identity key and dedup handle:
// a commit announced by both a webhook and a poll backfill resolves to the same
// row and is processed once.
type CommitStore interface {
	// Get retrieves a commit by its (repository, branch, sha) identity key.
	// Returns ErrNotFound if no record exists.
	Get(ctx context.Context, repository, branch, sha string) (entity.Commit, error)

	// Create records a new commit. The status must be CommitStatusUnknown.
	// Returns ErrAlreadyExists if a commit with the same identity already exists;
	// callers treat this as a successful dedup, not a failure.
	Create(ctx context.Context, commit entity.Commit) error

	// UpdateStatus updates the commit's status and advances the version from
	// oldVersion to newVersion. Returns ErrVersionMismatch if the current
	// persisted version does not match oldVersion; the caller must re-read and retry.
	// Version arithmetic is owned by the caller (controller); the store performs
	// a pure conditional write.
	UpdateStatus(ctx context.Context, repository, branch, sha string, oldVersion, newVersion int32, status entity.CommitStatus) error
}
