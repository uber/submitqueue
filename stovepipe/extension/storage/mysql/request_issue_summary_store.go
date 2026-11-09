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
	"github.com/uber/submitqueue/platform/base/id"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const listRequestIssueSummariesQuery = `
 SELECT request_id, task_id, unit_id, kind, build_id, first_seen_build_id, version
 FROM request_issue_summary WHERE queue = ? AND request_id = ?
 ORDER BY task_id ASC, unit_id ASC LIMIT ?`

const listRequestIssueSummariesAfterCursorQuery = `
 SELECT request_id, task_id, unit_id, kind, build_id, first_seen_build_id, version
 FROM request_issue_summary WHERE queue = ? AND request_id = ?
 AND (task_id > ? OR (task_id = ? AND unit_id > ?))
 ORDER BY task_id ASC, unit_id ASC LIMIT ?`

type requestIssueSummaryStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewRequestIssueSummaryStore binds per-request issue results to one queue.
func NewRequestIssueSummaryStore(db *sql.DB, scope tally.Scope, queue string) storage.RequestIssueSummaryStore {
	return &requestIssueSummaryStore{db: db, scope: scope, queue: queue}
}

func (s *requestIssueSummaryStore) Create(ctx context.Context, issue entity.RequestIssueSummary) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if err := issue.Validate(); err != nil {
		return err
	}
	if err := s.validateKey(issue.RequestID, issue.TaskID, issue.UnitID); err != nil {
		return err
	}
	if err := validateBuildIssueKey(s.queue, issue.BuildID, issue.TaskID, issue.UnitID); err != nil {
		return err
	}
	if err := validateBuildIssueKey(s.queue, issue.FirstSeenBuildID, issue.TaskID, issue.UnitID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO request_issue_summary (queue, request_id, task_id, unit_id, kind, build_id, first_seen_build_id, version)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, s.queue, issue.RequestID, issue.TaskID, issue.UnitID, issue.Kind, issue.BuildID, issue.FirstSeenBuildID, issue.Version)
	if isDuplicateEntry(err) {
		return storage.ErrAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create request issue: %w", err)
	}
	return nil
}

func (s *requestIssueSummaryStore) Get(ctx context.Context, requestID, taskID, unitID string) (ret entity.RequestIssueSummary, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if taskID == "" {
		return ret, fmt.Errorf("request issue task ID must not be empty")
	}
	if err := s.validateKey(requestID, taskID, unitID); err != nil {
		return ret, err
	}
	err := s.db.QueryRowContext(ctx,
		`SELECT request_id, task_id, unit_id, kind, build_id, first_seen_build_id, version
 FROM request_issue_summary WHERE queue = ? AND request_id = ? AND task_id = ? AND unit_id = ?`,
		s.queue, requestID, taskID, unitID).Scan(&ret.RequestID, &ret.TaskID, &ret.UnitID, &ret.Kind, &ret.BuildID, &ret.FirstSeenBuildID, &ret.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.RequestIssueSummary{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.RequestIssueSummary{}, fmt.Errorf("get request issue: %w", err)
	}
	return ret, nil
}

func (s *requestIssueSummaryStore) List(ctx context.Context, requestID string, bounds storage.RequestIssueSummaryRange) (ret []entity.RequestIssueSummary, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if bounds.Limit <= 0 {
		return nil, fmt.Errorf("request issue range requires a positive limit")
	}
	cursor := bounds.After
	if cursor.TaskID == "" && cursor.UnitID != "" {
		return nil, fmt.Errorf("request issue cursor requires a task ID")
	}
	if err := s.validateKey(requestID, cursor.TaskID, cursor.UnitID); err != nil {
		return nil, err
	}
	var rows *sql.Rows
	var err error
	if cursor.TaskID == "" {
		rows, err = s.db.QueryContext(ctx, listRequestIssueSummariesQuery, s.queue, requestID, bounds.Limit)
	} else {
		rows, err = s.db.QueryContext(ctx, listRequestIssueSummariesAfterCursorQuery, s.queue, requestID, cursor.TaskID, cursor.TaskID, cursor.UnitID, bounds.Limit)
	}
	if err != nil {
		return nil, fmt.Errorf("list request issues: %w", err)
	}
	defer rows.Close()
	issues := make([]entity.RequestIssueSummary, 0)
	for rows.Next() {
		var issue entity.RequestIssueSummary
		if err := rows.Scan(&issue.RequestID, &issue.TaskID, &issue.UnitID, &issue.Kind, &issue.BuildID, &issue.FirstSeenBuildID, &issue.Version); err != nil {
			return nil, fmt.Errorf("scan request issue: %w", err)
		}
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate request issues: %w", err)
	}
	return issues, nil
}

func (s *requestIssueSummaryStore) validateKey(requestID, taskID, unitID string) error {
	if err := id.Validate(requestID); err != nil {
		return err
	}
	return validateBuildIssueKey(s.queue, "", taskID, unitID)
}

var _ storage.RequestIssueSummaryStore = (*requestIssueSummaryStore)(nil)

func (s *requestIssueSummaryStore) Update(ctx context.Context, summary entity.RequestIssueSummary, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if err := summary.Validate(); err != nil {
		return err
	}
	if err := s.validateKey(summary.RequestID, summary.TaskID, summary.UnitID); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx,
		`UPDATE request_issue_summary SET kind = ?, build_id = ?, first_seen_build_id = ?, version = ?
  WHERE queue = ? AND request_id = ? AND task_id = ? AND unit_id = ? AND version = ?`,
		summary.Kind, summary.BuildID, summary.FirstSeenBuildID, newVersion, s.queue, summary.RequestID, summary.TaskID, summary.UnitID, oldVersion)
	if err != nil {
		return fmt.Errorf("update request issue summary: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read request issue summary update count: %w", err)
	}
	if affected != 1 {
		return storage.ErrVersionMismatch
	}
	return nil
}
