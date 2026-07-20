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

//go:generate mockgen -source=request_batch_store.go -destination=mock/request_batch_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// RequestBatchStore manages immutable request-to-batch assignments.
type RequestBatchStore interface {
	// Get retrieves the assignment keyed by request ID.
	// Returns ErrNotFound if the request has not been assigned.
	Get(ctx context.Context, requestID string) (entity.RequestBatch, error)

	// Create reserves the batch assignment for a request.
	// Returns ErrAlreadyExists if the request already has an assignment.
	Create(ctx context.Context, requestBatch entity.RequestBatch) error
}
