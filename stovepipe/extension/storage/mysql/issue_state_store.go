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

const listIssueStatesQuery = `
	SELECT task_id, unit_id, status, first_seen_build_id, version
	FROM issue_state WHERE queue = ?
	ORDER BY task_id ASC, unit_id ASC LIMIT ?`

const listIssueStatesAfterCursorQuery = `
	SELECT task_id, unit_id, status, first_seen_build_id, version
	FROM issue_state WHERE queue = ?
		AND (task_id > ? OR (task_id = ? AND unit_id > ?))
	ORDER BY task_id ASC, unit_id ASC LIMIT ?`

type issueStateStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewIssueStateStore binds retained issue states to one queue.
func NewIssueStateStore(db *sql.DB, scope tally.Scope, queue string) storage.IssueStateStore {
	return &issueStateStore{db: db, scope: scope, queue: queue}
}

func (s *issueStateStore) Create(ctx context.Context, issue entity.IssueState) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := s.validateIssue(issue); err != nil {
		return err
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO issue_state (queue, task_id, unit_id, status, first_seen_build_id, version)
         VALUES (?, ?, ?, ?, ?, ?)`,
		s.queue, issue.TaskID, issue.UnitID, issue.Status, issue.FirstSeenBuildID, issue.Version)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("issue state task=%q unit=%q: %w", issue.TaskID, issue.UnitID, storage.ErrAlreadyExists)
		}

		return fmt.Errorf("failed to create issue state: %w", err)
	}

	return nil
}

func (s *issueStateStore) Get(ctx context.Context, taskID, unitID string) (ret entity.IssueState, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if taskID == "" {
		return entity.IssueState{}, fmt.Errorf("issue state task ID must not be empty")
	}
	if err := validateBuildIssueKey(s.queue, "", taskID, unitID); err != nil {
		return entity.IssueState{}, err
	}

	var issue entity.IssueState
	err := s.db.QueryRowContext(ctx,
		`SELECT task_id, unit_id, status, first_seen_build_id, version
         FROM issue_state WHERE queue = ? AND task_id = ? AND unit_id = ?`,
		s.queue, taskID, unitID,
	).Scan(&issue.TaskID, &issue.UnitID, &issue.Status, &issue.FirstSeenBuildID, &issue.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.IssueState{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.IssueState{}, fmt.Errorf("failed to get issue state: %w", err)
	}

	return issue, nil
}

func (s *issueStateStore) List(ctx context.Context, bounds storage.IssueStateRange) (ret []entity.IssueState, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	cursor := bounds.After
	if bounds.Limit <= 0 {
		return nil, fmt.Errorf("issue state range requires a positive limit")
	}
	if cursor.TaskID == "" && cursor.UnitID != "" {
		return nil, fmt.Errorf("issue state cursor requires a task ID when a unit ID is supplied")
	}
	if err := validateBuildIssueKey(s.queue, "", cursor.TaskID, cursor.UnitID); err != nil {
		return nil, err
	}

	var (
		rows *sql.Rows
		err  error
	)
	if cursor.TaskID != "" {
		rows, err = s.db.QueryContext(ctx, listIssueStatesAfterCursorQuery,
			s.queue, cursor.TaskID, cursor.TaskID, cursor.UnitID, bounds.Limit)
	} else {
		rows, err = s.db.QueryContext(ctx, listIssueStatesQuery, s.queue, bounds.Limit)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list issue states: %w", err)
	}
	defer rows.Close()

	issues := make([]entity.IssueState, 0)
	for rows.Next() {
		var issue entity.IssueState
		if err := rows.Scan(&issue.TaskID, &issue.UnitID, &issue.Status, &issue.FirstSeenBuildID, &issue.Version); err != nil {
			return nil, fmt.Errorf("failed to scan issue state: %w", err)
		}
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate issue states: %w", err)
	}

	return issues, nil
}

func (s *issueStateStore) Update(ctx context.Context, issue entity.IssueState, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	replacement := issue
	replacement.Version = newVersion
	if err := s.validateIssue(replacement); err != nil {
		return err
	}
	if oldVersion <= 0 {
		return fmt.Errorf("issue state guard version must be positive")
	}
	result, err := s.db.ExecContext(ctx,
		`UPDATE issue_state SET status = ?, first_seen_build_id = ?, version = ? WHERE queue = ? AND task_id = ? AND unit_id = ? AND version = ?`,
		issue.Status, issue.FirstSeenBuildID, newVersion, s.queue, issue.TaskID, issue.UnitID, oldVersion)
	if err != nil {
		return fmt.Errorf("update issue state: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read issue state update count: %w", err)
	}
	if rows != 1 {
		return storage.ErrVersionMismatch
	}
	return nil
}

func (s *issueStateStore) validateIssue(issue entity.IssueState) error {
	if err := issue.Validate(); err != nil {
		return err
	}
	if err := validateBuildIssueKey(s.queue, issue.FirstSeenBuildID, issue.TaskID, issue.UnitID); err != nil {
		return err
	}
	return nil
}

var _ storage.IssueStateStore = (*issueStateStore)(nil)
