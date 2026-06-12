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

// Package changeset resolves batch identity into the changes a batch contains.
// It is the single place the orchestrator walks batch -> requests -> changes,
// consolidating what the build, merge, and score controllers each did privately.
// Decision/action extensions (scorer, buildrunner, pusher, and future
// detail-aware conflict analyzers) take thin identity entities and resolve their
// granular content through an injected Resolver instead of being handed
// pre-resolved data by a controller.
package changeset

//go:generate mockgen -source=changeset.go -destination=mock/changeset_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity/change"
	"github.com/uber/submitqueue/submitqueue/entity"
)

// Resolver turns batch identity into the changes the batch contains. Both methods
// operate on a single batch — callers with several batches (a build's base, a
// merge train) loop and keep the per-batch boundary by holding a slice per batch.
// The two methods differ only in fidelity: ChangesForBatch is the cheap URI-only
// view; DetailedForBatch reads the change store for provider details.
type Resolver interface {
	// ChangesForBatch resolves a batch's contained requests into their raw
	// changes (URIs only; no change-store read), in batch.Contains order. A batch
	// with no requests yields an empty slice. Used by the build (base/head) and
	// merge stages.
	ChangesForBatch(ctx context.Context, batch entity.Batch) ([]change.Change, error)

	// DetailedForBatch resolves a batch into its normalized, batch-level view:
	// one entity.ChangeInfo per claimed URI (URI plus the provider details read
	// from the change store), aggregated across every request in the batch. For
	// each URI it selects the record owned by the request, since the change store
	// returns rows for all requests that ever claimed the URI. Used by the score
	// stage and detail-aware analyzers.
	DetailedForBatch(ctx context.Context, batch entity.Batch) (entity.BatchChanges, error)
}
