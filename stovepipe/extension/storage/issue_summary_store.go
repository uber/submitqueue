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

//go:generate mockgen -source=issue_summary_store.go -destination=mock/issue_summary_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// IssueSummaryStore retains queue-scoped issue read models, including closed issues.
// Source ordering is the caller's responsibility; Version guards concurrent replacements.
type IssueSummaryStore interface {
	// Create returns ErrAlreadyExists if the task/unit key exists, even when closed.
	Create(ctx context.Context, summary entity.IssueSummary) error

	// Get returns ErrNotFound if the task/unit key has no summary.
	Get(ctx context.Context, taskID, unitID string) (entity.IssueSummary, error)

	// Update replaces summary only at oldVersion, writing the supplied newVersion.
	// A missing key or stale version returns ErrVersionMismatch.
	Update(ctx context.Context, summary entity.IssueSummary, oldVersion, newVersion int32) error
}
