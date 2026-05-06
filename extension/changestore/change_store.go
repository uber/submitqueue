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

package changestore

//go:generate mockgen -source=change_store.go -destination=mock/change_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/entity"
)

// ChangeStore manages per-URI claim records for in-flight land requests.
// Each row records that a specific URI was claimed by a specific request, scoped to a queue.
// The (URI, RequestID) pair is the immutable identity of a record. Metadata may evolve over time.
//
// The store is the source of truth for "which URIs are or were associated with which requests".
// Liveness of an owning request is NOT tracked here — callers must consult RequestStore separately
// to determine whether an owner is in a terminal state.
type ChangeStore interface {
	// Create persists a batch of ChangeRecords. Conflicts on the (URI, RequestID) primary key
	// are silently ignored, making the call idempotent under queue redeliveries of the same request.
	// Records belonging to different requests do not conflict — overlap between requests is detected
	// by FindOverlapping, not by Create.
	Create(ctx context.Context, records []entity.ChangeRecord) error

	// FindOverlapping returns ChangeRecords whose URI is in the given set, scoped to queue,
	// excluding any records belonging to excludeRequestID (so callers can skip self when checking
	// for duplicates of a freshly-claimed request). Returns an empty slice when there is no overlap.
	//
	// Liveness of the returned records' owning requests is NOT filtered here — the caller is
	// responsible for consulting RequestStore to skip terminal owners.
	FindOverlapping(
		ctx context.Context,
		queue string,
		uris []string,
		excludeRequestID string,
	) ([]entity.ChangeRecord, error)
}
