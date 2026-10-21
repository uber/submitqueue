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

//go:generate mockgen -source=project_fact_by_degree_store.go -destination=mock/project_fact_by_degree_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// ProjectFactByDegreeStore holds immutable ordering mappings in its bound queue.
type ProjectFactByDegreeStore interface {
	// Create puts one mapping by (URI, degree, project); duplicates return ErrAlreadyExists.
	Create(ctx context.Context, entry entity.ProjectFactByDegree) error

	// List scans a primary-key range by descending degree, then ascending bytewise project.
	// after is exclusive; its zero value starts the range. limit must be positive.
	List(ctx context.Context, uri string, after entity.ProjectFactByDegree, limit int) ([]entity.ProjectFactByDegree, error)
}
