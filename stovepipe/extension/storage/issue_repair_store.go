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

//go:generate mockgen -source=issue_repair_store.go -destination=mock/issue_repair_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// IssueRepairStore retains versioned submissions keyed by opening and executor.
// The controller owns FixID and SkipReason; executors own Metadata. Callers must
// preserve other fields and reload on version conflicts before reapplying changes.
type IssueRepairStore interface {
	// Get returns ErrNotFound if no repair row exists.
	Get(context.Context, entity.OpenIssue, string) (entity.IssueRepair, error)
	// Create returns ErrAlreadyExists without overwriting existing state.
	Create(context.Context, entity.IssueRepair) error
	// Update replaces the record only at oldVersion; callers supply newVersion.
	Update(ctx context.Context, repair entity.IssueRepair, oldVersion, newVersion int32) error
}
