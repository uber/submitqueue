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

	"github.com/uber/submitqueue/submitqueue/entity"
)

// RequestURIStore persists the immutable change-URI reverse mapping.
type RequestURIStore interface {
	// Create inserts mapping and returns ErrAlreadyExists when its full primary key already exists.
	Create(ctx context.Context, mapping entity.RequestURI) error

	// ListByURI returns at most limit mappings ordered by received_at_ms descending, then request_id descending.
	ListByURI(ctx context.Context, changeURI string, limit int) ([]entity.RequestURI, error)
}
