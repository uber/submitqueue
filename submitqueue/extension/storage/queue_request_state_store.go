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

//go:generate mockgen -source=queue_request_state_store.go -destination=mock/queue_request_state_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// QueueRequestStateStore reads the per-queue membership records that file each in-queue
// request under its current state, so "the requests of queue Q in state S" is a single
// read keyed by (queue, state): a primary-key prefix on any backend, with no secondary
// index or server-side filtering.
//
// The store is read-only. A backend keeps the records in step with the requests it
// stores, as part of the same request write, so a listing reflects every committed
// request write.
type QueueRequestStateStore interface {
	// List returns every record filed under the bound queue's given state. An empty
	// slice means no request is in that state. Order is unspecified.
	List(ctx context.Context, state entity.RequestState) ([]entity.QueueRequestState, error)
}
