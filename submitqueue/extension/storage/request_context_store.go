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

//go:generate mockgen -source=request_context_store.go -destination=mock/request_context_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// RequestContextStore persists immutable gateway-owned request admission data.
type RequestContextStore interface {
	// Create persists requestContext. Returns ErrAlreadyExists when any context already exists for the request ID; callers own retry idempotency and conflict detection.
	Create(ctx context.Context, requestContext entity.RequestContext) error

	// Get returns the context for requestID. Returns ErrNotFound when it does not exist.
	Get(ctx context.Context, requestID string) (entity.RequestContext, error)
}
