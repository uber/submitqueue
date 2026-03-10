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

	"github.com/uber/submitqueue/entity"
)

// RequestStore is an interface that defines methods for managing land requests in the database.
type RequestStore interface {
	// Get retrieves a land request by ID. Returns ErrNotFound if the request is not found.
	Get(ctx context.Context, id string) (entity.Request, error)

	// Create creates a new land request. The request must have a unique ID already assigned.
	// Returns ErrAlreadyExists if a request with the same ID already exists.
	Create(ctx context.Context, request entity.Request) error

	// UpdateState updates the state of a land request if the current version matches the expected version. If versions do not match, returns ErrVersionMismatch.
	// The implementation should increment the version by 1 atomically with the state update.
	UpdateState(ctx context.Context, id string, version int32, newState entity.RequestState) error
}
