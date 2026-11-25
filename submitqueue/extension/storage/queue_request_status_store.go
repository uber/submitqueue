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

//go:generate mockgen -source=queue_request_status_store.go -destination=mock/queue_request_status_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// QueueRequestStatusStore manages the per-queue membership records that file each in-queue
// request under its public status, so "the requests of queue Q at status S" is a single read
// keyed by (queue, status) — a primary-key prefix on any backend, with no secondary index on
// the request summary.
//
// Records are advisory: the authoritative status is on the RequestSummary, and a record may
// file a request under a status it has already left. Readers must therefore treat a listing
// as a set of candidate request IDs — load each summary by key, classify the request by the
// summary's own Status rather than the bucket the record was found in, and delete records
// whose summary is finished or at a different status. Writers do not clean up after every
// interruption, so reader cleanup is part of the contract, not an optimization.
//
// The interface is per-status and per-record so that any backend can implement it without
// multi-key queries or batch atomicity. A request moves buckets via Put of the new record
// followed by Delete of the old one, which keeps at least one record visible throughout. All
// writes are idempotent so log redeliveries can safely repeat them.
type QueueRequestStatusStore interface {
	// List returns every record filed under the bound queue's given status. An empty slice
	// means the bucket is empty. Order is unspecified.
	List(ctx context.Context, status entity.RequestStatus) ([]entity.QueueRequestStatus, error)

	// Put persists a record. The record's Queue must match the instance's bound queue.
	// Writing a record whose (queue, status, requestID) already exists is a no-op success
	// that keeps the stored EnteredAtMs, so the call is idempotent under redeliveries.
	Put(ctx context.Context, record entity.QueueRequestStatus) error

	// Delete removes the bound queue's record identified by (status, requestID). Deleting an
	// absent record is a no-op success, so the call is idempotent under redeliveries.
	Delete(ctx context.Context, status entity.RequestStatus, requestID string) error
}
