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

//go:generate mockgen -source=fix_store.go -destination=mock/fix_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// FixStore persists queue-scoped versioned logical repairs by ID.
// Create/Get use ErrAlreadyExists/ErrNotFound; Update uses ErrVersionMismatch.
// Callers supply both the guard version and replacement version.
type FixStore interface {
	Create(context.Context, entity.Fix) error
	Get(context.Context, string) (entity.Fix, error)
	Update(context.Context, entity.Fix, int32, int32) error
}
