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

package entity

// ChangeRecord represents a single URI's claim by a request, persisted in the change store.
// The whole record is immutable: the (Queue, URI, RequestID) triple is its identity and the
// Details (author, changed files, line counts) are captured once at claim time from the
// change provider. There is no update path.
type ChangeRecord struct {
	// URI identifies the change (RFC 3986). Same scheme/format as change.Change.URIs.
	// Example: "github://github.example.com/uber/submitqueue/pull/123/c3a4d5e6f7890123456789abcdef0123456789ab".
	URI string `json:"uri"`

	// RequestID is the owning land request that claimed this URI.
	// Format matches entity.Request.ID: "<queue>/<counter_value>".
	//
	// RequestID participates in the change-store primary key so that concurrent claims
	// by different requests on the same URI coexist as distinct rows. Same-request
	// retries collide on the PK and are absorbed idempotently; different-request
	// collisions surface as additional rows that callers detect via GetByURI.
	RequestID string `json:"request_id"`

	// Queue is the queue the owning request belongs to. It is the leading column of
	// the change-store primary key, so queue-scoped duplicate checks become PK-prefix
	// scans and the table is shardable by queue.
	Queue string `json:"queue"`

	// Details holds the provider-supplied facts about the change (author, changed
	// files, line counts). It is captured at claim time (the validate controller, after
	// fetching from the change provider) and written once with the record — records are
	// immutable, so Details is never updated after Create.
	Details ChangeDetails `json:"details"`

	// CreatedAt is the Unix milliseconds timestamp when this record was created.
	CreatedAt int64 `json:"created_at"`

	// UpdatedAt is the Unix milliseconds timestamp when this record was created. Records
	// are immutable, so it always equals CreatedAt; retained for schema symmetry.
	UpdatedAt int64 `json:"updated_at"`

	// Version is the record version. Records are immutable, so it is always 1; retained
	// for schema symmetry with the other stores.
	Version int32 `json:"version"`
}
