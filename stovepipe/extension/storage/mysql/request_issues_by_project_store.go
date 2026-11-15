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

package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const listRequestIssuesByProjectQuery = `
	SELECT task_id, unit_id, project
	FROM request_issues_by_project WHERE queue = ? AND request_id = ? AND project = ?
	ORDER BY task_id ASC, unit_id ASC LIMIT ?`

const listRequestIssuesByProjectAfterCursorQuery = `
	SELECT task_id, unit_id, project
	FROM request_issues_by_project WHERE queue = ? AND request_id = ? AND project = ?
		AND (task_id > ? OR (task_id = ? AND unit_id > ?))
	ORDER BY task_id ASC, unit_id ASC LIMIT ?`

type requestIssuesByProjectStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewRequestIssuesByProjectStore binds the per-request project lookup to a queue.
func NewRequestIssuesByProjectStore(db *sql.DB, scope tally.Scope, queue string) storage.RequestIssuesByProjectStore {
	return &requestIssuesByProjectStore{db: db, scope: scope, queue: queue}
}

func (s *requestIssuesByProjectStore) Create(ctx context.Context, requestID string, entry entity.ProjectIssueAttribution) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := validateProjectIssueRequest(s.queue, requestID); err != nil {
		return err
	}
	if err := validateProjectIssueAttribution(entry); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO request_issues_by_project (queue, request_id, project, task_id, unit_id)
         VALUES (?, ?, ?, ?, ?)`,
		s.queue, requestID, entry.Project, entry.TaskID, entry.UnitID)
	if isDuplicateEntry(err) {
		return fmt.Errorf("request project issue already retained: %w", storage.ErrAlreadyExists)
	}
	if err != nil {
		return fmt.Errorf("failed to create request project issue: %w", err)
	}
	return nil
}

func (s *requestIssuesByProjectStore) List(ctx context.Context, requestID, project string, after entity.ProjectIssueAttribution, limit int) (ret []entity.ProjectIssueAttribution, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := validateProjectIssueRequest(s.queue, requestID); err != nil {
		return nil, err
	}
	if project == "" {
		return nil, fmt.Errorf("request project issue project must not be empty")
	}
	if len(project) > 1020 {
		return nil, fmt.Errorf("request project issue project exceeds 1020-byte limit")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("request project issue limit must be positive")
	}
	query := listRequestIssuesByProjectQuery
	args := []any{s.queue, requestID, project}
	if after != (entity.ProjectIssueAttribution{}) {
		if err := validateProjectIssueAttribution(after); err != nil {
			return nil, err
		}
		if after.Project != project {
			return nil, fmt.Errorf("request project issue cursor must match project")
		}
		query = listRequestIssuesByProjectAfterCursorQuery
		args = append(args, after.TaskID, after.TaskID, after.UnitID)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list request project issues: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var entry entity.ProjectIssueAttribution
		if err := rows.Scan(&entry.TaskID, &entry.UnitID, &entry.Project); err != nil {
			return nil, fmt.Errorf("failed to scan request project issue: %w", err)
		}
		ret = append(ret, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate request project issues: %w", err)
	}
	return ret, nil
}
