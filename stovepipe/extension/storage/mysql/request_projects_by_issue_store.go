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
	"github.com/uber/submitqueue/platform/base/id"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const listRequestProjectsByIssueQuery = `
	SELECT task_id, unit_id, project
	FROM request_projects_by_issue WHERE queue = ? AND request_id = ?
	ORDER BY task_id ASC, unit_id ASC, project ASC LIMIT ?`

const listRequestProjectsByIssueAfterCursorQuery = `
	SELECT task_id, unit_id, project
	FROM request_projects_by_issue WHERE queue = ? AND request_id = ?
		AND (task_id > ? OR (task_id = ? AND unit_id > ?)
			OR (task_id = ? AND unit_id = ? AND project > ?))
	ORDER BY task_id ASC, unit_id ASC, project ASC LIMIT ?`

type requestProjectsByIssueStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewRequestProjectsByIssueStore binds immutable attribution results to a queue.
func NewRequestProjectsByIssueStore(db *sql.DB, scope tally.Scope, queue string) storage.RequestProjectsByIssueStore {
	return &requestProjectsByIssueStore{db: db, scope: scope, queue: queue}
}

func (s *requestProjectsByIssueStore) Create(ctx context.Context, requestID string, entry entity.ProjectIssueAttribution) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := s.validatePrefix(requestID); err != nil {
		return err
	}
	if err := validateProjectIssueAttribution(entry); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO request_projects_by_issue (queue, request_id, task_id, unit_id, project)
         VALUES (?, ?, ?, ?, ?)`,
		s.queue, requestID, entry.TaskID, entry.UnitID, entry.Project)
	if isDuplicateEntry(err) {
		return fmt.Errorf("project issue attribution already retained: %w", storage.ErrAlreadyExists)
	}
	if err != nil {
		return fmt.Errorf("failed to create project issue attribution: %w", err)
	}
	return nil
}

func (s *requestProjectsByIssueStore) List(ctx context.Context, requestID string, after entity.ProjectIssueAttribution, limit int) (ret []entity.ProjectIssueAttribution, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := s.validatePrefix(requestID); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, fmt.Errorf("project issue attribution limit must be positive")
	}
	query := listRequestProjectsByIssueQuery
	args := []any{s.queue, requestID}
	if after != (entity.ProjectIssueAttribution{}) {
		if err := validateProjectIssueAttribution(after); err != nil {
			return nil, err
		}
		query = listRequestProjectsByIssueAfterCursorQuery
		args = append(args, after.TaskID, after.TaskID, after.UnitID, after.TaskID, after.UnitID, after.Project)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list project issue attributions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var entry entity.ProjectIssueAttribution
		if err := rows.Scan(&entry.TaskID, &entry.UnitID, &entry.Project); err != nil {
			return nil, fmt.Errorf("failed to scan project issue attribution: %w", err)
		}
		ret = append(ret, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate project issue attributions: %w", err)
	}
	return ret, nil
}

func (s *requestProjectsByIssueStore) validatePrefix(requestID string) error {
	if err := id.Validate(requestID); err != nil {
		return fmt.Errorf("invalid project issue attribution request ID: %w", err)
	}
	if s.queue == "" {
		return fmt.Errorf("project issue attribution queue must not be empty")
	}
	if len(s.queue) > 255 {
		return fmt.Errorf("project issue attribution queue exceeds 255-byte limit")
	}
	return nil
}

func validateProjectIssueAttribution(entry entity.ProjectIssueAttribution) error {
	if err := entry.Validate(); err != nil {
		return err
	}
	for _, field := range []struct {
		name     string
		value    string
		maxBytes int
	}{
		{"task ID", entry.TaskID, 255},
		{"unit ID", entry.UnitID, 1024},
		{"project", entry.Project, 1020},
	} {
		if len(field.value) > field.maxBytes {
			return fmt.Errorf("project issue attribution %s exceeds %d-byte limit", field.name, field.maxBytes)
		}
	}
	return nil
}
