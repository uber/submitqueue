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

//go:generate mockgen -source=issue_log_store.go -destination=mock/issue_log_store_mock.go -package=mock

import (
	"context"

	"github.com/uber/submitqueue/stovepipe/entity"
)

// IssueLogStore retains immutable issue transitions within the bound queue.
// Task, unit, build, and status identify one transition.
type IssueLogStore interface {
	// Create returns ErrAlreadyExists for a previously recorded transition.
	// A duplicate never replaces the original timestamp.
	Create(ctx context.Context, log entity.IssueLog) error

	// Get returns ErrNotFound when the transition has not been recorded.
	Get(ctx context.Context, taskID, unitID, buildID string, status entity.IssueStatus) (entity.IssueLog, error)
}
