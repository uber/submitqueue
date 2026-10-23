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

//go:generate mockgen -source=issue_tracking_store.go -destination=mock/issue_tracking_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// IssueTrackingStore retains one operational reconciliation state for the bound queue.
// It remains present when no issues are open so older results cannot recreate resolved issues.
type IssueTrackingStore interface {
	// Get returns ErrNotFound before the queue's first reconciliation is accepted.
	Get(context.Context) (entity.IssueTrackingState, error)
	// Create returns ErrAlreadyExists when the queue already has a state.
	Create(context.Context, entity.IssueTrackingState) error
	// Update replaces the state only at oldVersion; callers supply newVersion without store arithmetic.
	Update(ctx context.Context, state entity.IssueTrackingState, oldVersion, newVersion int32) error
}
