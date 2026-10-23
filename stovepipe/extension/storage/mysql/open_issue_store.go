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

const listOpenIssuesQuery = `
	SELECT task_id, unit_id, first_seen_build_id
	FROM open_issue WHERE queue = ?
	ORDER BY task_id ASC, unit_id ASC LIMIT ?`

const listOpenIssuesAfterCursorQuery = `
	SELECT task_id, unit_id, first_seen_build_id
	FROM open_issue WHERE queue = ?
		AND (task_id > ? OR (task_id = ? AND unit_id > ?))
	ORDER BY task_id ASC, unit_id ASC LIMIT ?`

type openIssueStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewOpenIssueStore binds currently open issues to queue over the shared database.
func NewOpenIssueStore(db *sql.DB, scope tally.Scope, queue string) storage.OpenIssueStore {
	return &openIssueStore{db: db, scope: scope, queue: queue}
}

func (s *openIssueStore) Create(ctx context.Context, issue entity.OpenIssue) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := s.validateIssue(issue); err != nil {
		return err
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO open_issue (queue, task_id, unit_id, first_seen_build_id)
         VALUES (?, ?, ?, ?)`,
		s.queue, issue.TaskID, issue.UnitID, issue.FirstSeenBuildID)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("open issue task=%q unit=%q: %w", issue.TaskID, issue.UnitID, storage.ErrAlreadyExists)
		}

		return fmt.Errorf("failed to create open issue: %w", err)
	}

	return nil
}

func (s *openIssueStore) Get(ctx context.Context, taskID, unitID string) (ret entity.OpenIssue, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if taskID == "" {
		return entity.OpenIssue{}, fmt.Errorf("open issue task ID must not be empty")
	}
	if err := validateBuildIssueKey(s.queue, "", taskID, unitID); err != nil {
		return entity.OpenIssue{}, err
	}

	var issue entity.OpenIssue
	err := s.db.QueryRowContext(ctx,
		`SELECT task_id, unit_id, first_seen_build_id
         FROM open_issue WHERE queue = ? AND task_id = ? AND unit_id = ?`,
		s.queue, taskID, unitID,
	).Scan(&issue.TaskID, &issue.UnitID, &issue.FirstSeenBuildID)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.OpenIssue{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.OpenIssue{}, fmt.Errorf("failed to get open issue: %w", err)
	}

	return issue, nil
}

func (s *openIssueStore) List(ctx context.Context, bounds storage.OpenIssueRange) (ret []entity.OpenIssue, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	cursor := bounds.After
	if bounds.Limit <= 0 {
		return nil, fmt.Errorf("open issue range requires a positive limit")
	}
	if cursor.TaskID == "" && cursor.UnitID != "" {
		return nil, fmt.Errorf("open issue cursor requires a task ID when a unit ID is supplied")
	}
	if err := validateBuildIssueKey(s.queue, "", cursor.TaskID, cursor.UnitID); err != nil {
		return nil, err
	}

	var (
		rows *sql.Rows
		err  error
	)
	if cursor.TaskID != "" {
		rows, err = s.db.QueryContext(ctx, listOpenIssuesAfterCursorQuery,
			s.queue, cursor.TaskID, cursor.TaskID, cursor.UnitID, bounds.Limit)
	} else {
		rows, err = s.db.QueryContext(ctx, listOpenIssuesQuery, s.queue, bounds.Limit)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list open issues: %w", err)
	}
	defer rows.Close()

	issues := make([]entity.OpenIssue, 0)
	for rows.Next() {
		var issue entity.OpenIssue
		if err := rows.Scan(&issue.TaskID, &issue.UnitID, &issue.FirstSeenBuildID); err != nil {
			return nil, fmt.Errorf("failed to scan open issue: %w", err)
		}
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate open issues: %w", err)
	}

	return issues, nil
}

func (s *openIssueStore) Delete(ctx context.Context, issue entity.OpenIssue) (retErr error) {
	op := metrics.Begin(s.scope, "delete", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := s.validateIssue(issue); err != nil {
		return err
	}

	_, err := s.db.ExecContext(ctx,
		`DELETE FROM open_issue
         WHERE queue = ? AND task_id = ? AND unit_id = ? AND first_seen_build_id = ?`,
		s.queue, issue.TaskID, issue.UnitID, issue.FirstSeenBuildID)
	if err != nil {
		return fmt.Errorf("failed to delete open issue: %w", err)
	}

	return nil
}

func (s *openIssueStore) validateIssue(issue entity.OpenIssue) error {
	if issue.TaskID == "" {
		return fmt.Errorf("open issue task ID must not be empty")
	}
	if err := validateBuildIssueKey(s.queue, issue.FirstSeenBuildID, issue.TaskID, issue.UnitID); err != nil {
		return err
	}
	if issue.FirstSeenBuildID == "" {
		return fmt.Errorf("open issue first-seen build ID must not be empty")
	}
	return nil
}
