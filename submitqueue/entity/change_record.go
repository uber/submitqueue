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
// The (Queue, URI, RequestID) triple is the identity and is immutable; Metadata may be
// updated over time as additional information about the change (e.g., PR title, author,
// mergeability) becomes available.
type ChangeRecord struct {
	// URI identifies the change (RFC 3986). Same scheme/format as entity.Change.URIs.
	// Example: "github://uber/submitqueue/pull/123/c3a4d5e6f7890123456789abcdef0123456789ab".
	URI string `json:"uri"`

	// RequestID is the owning land request that claimed this URI.
	// Format matches entity.Request.ID: "<queue>/<counter_value>".
	//
	// RequestID participates in the change-store primary key so that concurrent claims
	// by different requests on the same URI coexist as distinct rows. Same-request
	// retries collide on the PK and are absorbed idempotently; different-request
	// collisions surface as additional rows that callers detect via FindOverlapping.
	RequestID string `json:"request_id"`

	// Queue is the queue the owning request belongs to. It is the leading column of
	// the change-store primary key, so queue-scoped duplicate checks become PK-prefix
	// scans and the table is shardable by queue.
	Queue string `json:"queue"`

	// Metadata is a JSON-encoded blob of provider-specific information about the change
	// (e.g., PR title, author, mergeable state). Stored as `'{}'` when no metadata has
	// been populated yet; updated by downstream enrichment.
	Metadata string `json:"metadata,omitempty"`

	// CreatedAt is the Unix milliseconds timestamp when this record was first created.
	CreatedAt int64 `json:"created_at"`

	// UpdatedAt is the Unix milliseconds timestamp when this record's Metadata was last updated.
	// Equal to CreatedAt when the record has never been updated.
	UpdatedAt int64 `json:"updated_at"`

	// Version is the optimistic-locking counter for mutable fields (Metadata).
	// Starts at 1 on Create and is incremented by callers on every update.
	// Mirrors the request-store convention: callers compute newVersion = oldVersion + 1
	// and pass both to the update method; the store performs a pure conditional write.
	Version int32 `json:"version"`
}
