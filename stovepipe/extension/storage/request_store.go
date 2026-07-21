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

//go:generate mockgen -source=request_store.go -destination=mock/request_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// RequestStore persists validation requests, keyed by request ID. The reverse index from a
// validated commit to the request that owns it lives in a separate RequestURIStore (one store per
// table), so the two concerns can evolve and be backed independently.
type RequestStore interface {
	// Create persists a new request. The request must have a unique ID already assigned.
	// Returns ErrAlreadyExists if a request with the same ID already exists.
	Create(ctx context.Context, request entity.Request) error

	// Get retrieves a request by ID. Returns errs.ErrNotFound if the request is not found.
	Get(ctx context.Context, id string) (entity.Request, error)

	// Update persists the mutable fields of request if the currently stored version matches
	// oldVersion, writing newVersion as the new version. Returns errs.ErrVersionMismatch if the
	// stored version does not match (including when the request does not exist).
	//
	// Version arithmetic is owned by the caller: it computes newVersion (typically oldVersion+1)
	// and only assigns request.Version = newVersion after this call succeeds. The store performs
	// a pure conditional write and does not read request.Version.
	Update(ctx context.Context, request entity.Request, oldVersion, newVersion int32) error
}
