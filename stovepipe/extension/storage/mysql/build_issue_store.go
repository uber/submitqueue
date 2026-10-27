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
	"errors"
	"fmt"

	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const listBuildIssuesQuery = `
	SELECT task_id, unit_id, fix_id, version
	FROM build_issue WHERE queue = ? AND build_id = ?
	ORDER BY task_id ASC, unit_id ASC LIMIT ?`

const listBuildIssuesAfterCursorQuery = `
	SELECT task_id, unit_id, fix_id, version
	FROM build_issue WHERE queue = ? AND build_id = ?
		AND (task_id > ? OR (task_id = ? AND unit_id > ?))
	ORDER BY task_id ASC, unit_id ASC LIMIT ?`

type buildIssueStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewBuildIssueStore binds observations to queue over the shared database.
func NewBuildIssueStore(db *sql.DB, scope tally.Scope, queue string) storage.BuildIssueStore {
	return &buildIssueStore{db: db, scope: scope, queue: queue}
}

func (s *buildIssueStore) Create(ctx context.Context, buildID string, issue entity.BuildIssue) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := s.validateKey(buildID, issue.TaskID, issue.UnitID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO build_issue (queue, build_id, task_id, unit_id, fix_id, version)
         VALUES (?, ?, ?, ?, ?, ?)`,
		s.queue, buildID, issue.TaskID, issue.UnitID, issue.FixID, issue.Version,
	)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("build issue build=%q task=%q unit=%q: %w", buildID, issue.TaskID, issue.UnitID, storage.ErrAlreadyExists)
		}

		return fmt.Errorf("failed to create build issue build=%q task=%q unit=%q: %w", buildID, issue.TaskID, issue.UnitID, err)
	}

	return nil
}

func (s *buildIssueStore) Get(ctx context.Context, buildID, taskID, unitID string) (ret entity.BuildIssue, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := s.validateKey(buildID, taskID, unitID); err != nil {
		return entity.BuildIssue{}, err
	}

	var issue entity.BuildIssue
	err := s.db.QueryRowContext(ctx,
		`SELECT task_id, unit_id, fix_id, version FROM build_issue
         WHERE queue = ? AND build_id = ? AND task_id = ? AND unit_id = ?`,
		s.queue, buildID, taskID, unitID,
	).Scan(&issue.TaskID, &issue.UnitID, &issue.FixID, &issue.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.BuildIssue{}, storage.WrapNotFound(err)
	}

	if err != nil {
		return entity.BuildIssue{}, fmt.Errorf("failed to get build issue build=%q task=%q unit=%q: %w", buildID, taskID, unitID, err)
	}

	return issue, nil
}

func (s *buildIssueStore) List(ctx context.Context, buildID string, bounds storage.BuildIssueRange) (ret []entity.BuildIssue, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if buildID == "" || bounds.Limit <= 0 {
		return nil, fmt.Errorf("build issue range requires a build ID and positive limit")
	}
	cursor := bounds.After
	if cursor.TaskID == "" && cursor.UnitID != "" {
		return nil, fmt.Errorf("build issue cursor requires a task ID when a unit ID is supplied")
	}
	if err := validateBuildIssueKey(s.queue, buildID, cursor.TaskID, cursor.UnitID); err != nil {
		return nil, err
	}

	var (
		rows *sql.Rows
		err  error
	)
	if cursor.TaskID == "" {
		rows, err = s.db.QueryContext(ctx, listBuildIssuesQuery, s.queue, buildID, bounds.Limit)
	} else {
		rows, err = s.db.QueryContext(ctx, listBuildIssuesAfterCursorQuery,
			s.queue, buildID, cursor.TaskID, cursor.TaskID, cursor.UnitID, bounds.Limit)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list build issues: %w", err)
	}
	defer rows.Close()

	issues := make([]entity.BuildIssue, 0)
	for rows.Next() {
		var issue entity.BuildIssue
		if err := rows.Scan(&issue.TaskID, &issue.UnitID, &issue.FixID, &issue.Version); err != nil {
			return nil, fmt.Errorf("failed to scan build issue: %w", err)
		}
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate build issues: %w", err)
	}

	return issues, nil
}

func (s *buildIssueStore) Update(ctx context.Context, buildID string, issue entity.BuildIssue, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := s.validateKey(buildID, issue.TaskID, issue.UnitID); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx,
		`UPDATE build_issue SET fix_id = ?, version = ?
         WHERE queue = ? AND build_id = ? AND task_id = ? AND unit_id = ? AND version = ?`,
		issue.FixID, newVersion, s.queue, buildID, issue.TaskID, issue.UnitID, oldVersion)
	if err != nil {
		return fmt.Errorf("failed to update build issue: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get build issue rows affected: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("build issue build=%q task=%q unit=%q expected_version=%d: %w",
			buildID, issue.TaskID, issue.UnitID, oldVersion, storage.ErrVersionMismatch)
	}
	return nil
}

func (s *buildIssueStore) validateKey(buildID, taskID, unitID string) error {
	if buildID == "" || taskID == "" {
		return fmt.Errorf("build issue requires nonempty build and task IDs")
	}
	return validateBuildIssueKey(s.queue, buildID, taskID, unitID)
}

func validateBuildIssueKey(queue, buildID, taskID, unitID string) error {
	for _, field := range []struct {
		name     string
		value    string
		maxBytes int
	}{
		{"queue", queue, 255},
		{"build ID", buildID, 1020},
		{"task ID", taskID, 255},
		{"unit ID", unitID, 1024},
	} {
		if len(field.value) > field.maxBytes {
			return fmt.Errorf("build issue %s exceeds %d-byte limit", field.name, field.maxBytes)
		}
	}

	return nil
}
