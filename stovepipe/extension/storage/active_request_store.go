// Copyright (c) 2026 Uber Technologies, Inc.
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

//go:generate mockgen -source=active_request_store.go -destination=mock/active_request_store_mock.go -package=mock

import "context"

// ActiveRequestStore enumerates queue-local request identities awaiting terminal observation.
// Entries are hints: readers must reload the request before interpreting or removing one.
type ActiveRequestStore interface {
	// Create retains an identity, returning ErrAlreadyExists if present.
	Create(ctx context.Context, requestID string) error
	// List returns at most limit identities in ascending bytewise primary-key order.
	// An empty after starts the range; otherwise the bound is exclusive.
	List(ctx context.Context, after string, limit int) ([]string, error)
	// Delete removes an identity idempotently, only after its request is durably terminal.
	Delete(ctx context.Context, requestID string) error
}
