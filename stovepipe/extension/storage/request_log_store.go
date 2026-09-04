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

//go:generate mockgen -source=request_log_store.go -destination=mock/request_log_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// RequestLogStore retains immutable occurrences for requests in its bound queue.
type RequestLogStore interface {
	// Create persists log and returns ErrAlreadyExists when its stable identity exists.
	Create(ctx context.Context, log entity.RequestLog) error

	// Get returns one record identified by requestID and logID, or ErrNotFound when absent.
	Get(ctx context.Context, requestID, logID string) (entity.RequestLog, error)

	// List returns all records for one request ordered by timestamp and log ID ascending, or ErrNotFound when none are retained.
	List(ctx context.Context, requestID string) ([]entity.RequestLog, error)
}
