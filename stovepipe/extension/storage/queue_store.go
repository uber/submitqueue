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

//go:generate mockgen -source=queue_store.go -destination=mock/queue_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// QueueStore persists per-queue coordination rows, keyed by queue name.
type QueueStore interface {
	// Create persists a new queue row. queue.Name must be set. Returns ErrAlreadyExists
	// if a row with the same name already exists.
	Create(ctx context.Context, queue entity.Queue) error

	// Get retrieves a queue by name. Returns errs.ErrNotFound if the queue is not found.
	Get(ctx context.Context, name string) (entity.Queue, error)

	// Update persists the mutable fields of queue if the stored version matches
	// oldVersion, writing newVersion. Returns errs.ErrVersionMismatch if the stored
	// version does not match (including when the queue does not exist).
	//
	// Version arithmetic is owned by the caller: it computes newVersion (typically
	// oldVersion+1) and only assigns queue.Version = newVersion after this call
	// succeeds. The store performs a pure conditional write.
	Update(ctx context.Context, queue entity.Queue, oldVersion, newVersion int32) error
}
