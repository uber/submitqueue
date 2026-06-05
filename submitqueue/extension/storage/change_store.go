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

//go:generate mockgen -source=change_store.go -destination=mock/change_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// ChangeStore manages per-URI claim records for in-flight land requests.
// Each row records that a specific URI was claimed by a specific request, scoped to a queue.
// The (Queue, URI, RequestID) triple is the immutable identity of a record. Metadata may
// evolve over time.
//
// The interface is intentionally per-record / per-URI so that any backend (SQL, DynamoDB,
// Bigtable, …) can implement it without needing batch-atomicity or multi-key query support.
// Callers loop when they have multiple URIs to claim or check; the typical request has a
// small number of URIs (a single PR or a short stack), so the loop overhead is negligible.
type ChangeStore interface {
	// Create persists a single ChangeRecord. A primary-key conflict on
	// (Queue, URI, RequestID) is silently ignored, which makes the call
	// idempotent under queue redeliveries of the same request. Records belonging
	// to different requests do not conflict on the PK — cross-request overlap
	// is detected by GetByURI, not by Create.
	Create(ctx context.Context, record entity.ChangeRecord) error

	// GetByURI returns every ChangeRecord for the given (queue, uri). Multiple
	// requests can have claimed the same URI over time, so the slice may have
	// any number of entries; an empty slice means no claim has ever been
	// recorded for this URI in this queue.
	//
	// The store does not filter by request_id or by the owning request's
	// state — callers that want to skip self filter by RequestID, and callers
	// that want only live owners consult RequestStore for liveness.
	GetByURI(ctx context.Context, queue string, uri string) ([]entity.ChangeRecord, error)
}
