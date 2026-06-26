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

//go:generate mockgen -source=request_uri_store.go -destination=mock/request_uri_store_mock.go -package=mock

import (
	"context"
)

// RequestURIStore is the reverse index from a validated commit to the request that owns it,
// keyed by (queue, commit URI). It is a separate store from RequestStore because it is a separate
// table — the two are written independently (no cross-table transaction), keeping the contract
// satisfiable by key/value backends. The caller orchestrates "create request, then map URI".
type RequestURIStore interface {
	// Create records that request id validates the commit uri for queue. Returns ErrAlreadyExists
	// if (queue, uri) is already mapped — the signal a caller uses to detect that the commit is
	// already being validated by another request.
	Create(ctx context.Context, queue, uri, id string) error

	// GetIDByURI returns the id of the request validating (queue, uri).
	// Returns ErrNotFound if no request is mapped to that commit.
	GetIDByURI(ctx context.Context, queue, uri string) (string, error)
}
