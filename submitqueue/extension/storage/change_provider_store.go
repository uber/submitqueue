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

//go:generate mockgen -source=change_provider_store.go -destination=mock/change_provider_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// ChangeProviderStore is an interface that defines methods for managing change provider information in the database.
type ChangeProviderStore interface {
	// Get retrieves information about a change by ID.
	// Returns ErrNotFound if the change provider is not found.
	//
	// Note: The order of ChangeProvider entities here is not guaranteed to
	// be the same as the request to which it belongs. The caller is repsonsible
	// for inspecting and mapping the result of this function to the
	// order of changes within the original request.
	//
	Get(ctx context.Context, requestID string) ([]entity.ChangeProvider, error)

	// Create creates a new change provider.
	Create(ctx context.Context, changeProvider entity.ChangeProvider) error

	// There is no update function since once created, data is only ever read from this
	// store.
}
