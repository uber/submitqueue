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

//go:generate mockgen -source=queue_log_store.go -destination=mock/queue_log_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// QueueLogStore retains immutable state occurrences for its bound queue.
type QueueLogStore interface {
	// Get returns an occurrence by log ID or ErrNotFound.
	Get(ctx context.Context, logID string) (entity.QueueLog, error)
	// Create retains an occurrence, returning ErrAlreadyExists when its identity exists.
	Create(context.Context, entity.QueueLog) error
}
