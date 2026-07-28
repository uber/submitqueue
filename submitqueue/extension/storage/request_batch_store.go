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

//go:generate mockgen -source=request_batch_store.go -destination=mock/request_batch_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// RequestBatchStore persists the immutable assignment from a request ID to its owning batch ID.
type RequestBatchStore interface {
	// Get retrieves the assignment by request ID. Returns ErrNotFound if no assignment exists.
	Get(ctx context.Context, requestID string) (entity.RequestBatch, error)

	// Create inserts an immutable assignment. Returns ErrAlreadyExists if the request is already assigned.
	Create(ctx context.Context, assignment entity.RequestBatch) error
}
