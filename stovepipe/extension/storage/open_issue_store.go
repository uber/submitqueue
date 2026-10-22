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

//go:generate mockgen -source=open_issue_store.go -destination=mock/open_issue_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// OpenIssueStore contains only currently open issues, keyed by task and unit
// within the bound queue. A reopened issue must use a new first-seen build ID.
type OpenIssueStore interface {
	// Create requires a nonempty first-seen build ID.
	// An existing task/unit returns ErrAlreadyExists.
	Create(ctx context.Context, issue entity.OpenIssue) error

	// Get returns ErrNotFound when the task/unit is not currently open.
	Get(ctx context.Context, taskID, unitID string) (entity.OpenIssue, error)

	// Delete removes only the matching opening. It succeeds if that opening is
	// already absent and never removes a row with a different first-seen build.
	Delete(ctx context.Context, issue entity.OpenIssue) error
}
