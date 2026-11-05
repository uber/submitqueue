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

const requestIssueColumns = `request_id, build_id, task_id, unit_id, fix_id, version`
const listRequestIssuesQuery = `SELECT ` + requestIssueColumns + ` FROM request_issue_summary
    WHERE queue = ? AND request_id = ? ORDER BY build_id ASC, task_id ASC, unit_id ASC LIMIT ?`
const listRequestIssuesAfterQuery = `SELECT ` + requestIssueColumns + ` FROM request_issue_summary
    WHERE queue = ? AND request_id = ? AND
    (build_id > ? OR (build_id = ? AND task_id > ?) OR (build_id = ? AND task_id = ? AND unit_id > ?))
    ORDER BY build_id ASC, task_id ASC, unit_id ASC LIMIT ?`

type requestIssueSummaryStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewRequestIssueSummaryStore binds request issue projections to one queue.
func NewRequestIssueSummaryStore(db *sql.DB, scope tally.Scope, queue string) storage.RequestIssueSummaryStore {
	return &requestIssueSummaryStore{db: db, scope: scope, queue: queue}
}

func (s *requestIssueSummaryStore) Create(ctx context.Context, summary entity.RequestIssueSummary) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if err := s.validateSummary(summary); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO request_issue_summary (queue, `+requestIssueColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		s.queue, summary.RequestID, summary.BuildID, summary.TaskID, summary.UnitID, summary.FixID, summary.Version)
	if isDuplicateEntry(err) {
		return storage.ErrAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create request issue summary: %w", err)
	}
	return nil
}

func (s *requestIssueSummaryStore) Get(ctx context.Context, requestID, buildID, taskID, unitID string) (summary entity.RequestIssueSummary, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if err := s.validateSummary(entity.RequestIssueSummary{RequestID: requestID, BuildID: buildID, TaskID: taskID, UnitID: unitID, Version: 1}); err != nil {
		return summary, err
	}
	err := s.db.QueryRowContext(ctx, `SELECT `+requestIssueColumns+` FROM request_issue_summary WHERE queue = ? AND request_id = ? AND build_id = ? AND task_id = ? AND unit_id = ?`,
		s.queue, requestID, buildID, taskID, unitID).Scan(&summary.RequestID, &summary.BuildID, &summary.TaskID, &summary.UnitID, &summary.FixID, &summary.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.RequestIssueSummary{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.RequestIssueSummary{}, fmt.Errorf("get request issue summary: %w", err)
	}
	return summary, nil
}

func (s *requestIssueSummaryStore) Update(ctx context.Context, summary entity.RequestIssueSummary, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	replacement := summary
	replacement.Version = newVersion
	if err := s.validateSummary(replacement); err != nil {
		return err
	}
	if oldVersion <= 0 {
		return fmt.Errorf("request issue guard version must be positive")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE request_issue_summary SET fix_id = ?, version = ?
        WHERE queue = ? AND request_id = ? AND build_id = ? AND task_id = ? AND unit_id = ? AND version = ?`,
		summary.FixID, newVersion, s.queue, summary.RequestID, summary.BuildID, summary.TaskID, summary.UnitID, oldVersion)
	if err != nil {
		return fmt.Errorf("update request issue summary: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read request issue update count: %w", err)
	}
	if affected != 1 {
		return storage.ErrVersionMismatch
	}
	return nil
}

func (s *requestIssueSummaryStore) List(ctx context.Context, requestID string, bounds storage.RequestIssueRange) (_ []entity.RequestIssueSummary, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if err := id.Validate(requestID); err != nil {
		return nil, err
	}
	after := bounds.After
	if bounds.Limit <= 0 || (after.BuildID == "" && (after.TaskID != "" || after.UnitID != "")) || (after.TaskID == "" && after.UnitID != "") {
		return nil, fmt.Errorf("invalid request issue range")
	}
	if err := validateBuildIssueKey(s.queue, after.BuildID, after.TaskID, after.UnitID); err != nil {
		return nil, err
	}
	var rows *sql.Rows
	var err error
	if after.BuildID == "" {
		rows, err = s.db.QueryContext(ctx, listRequestIssuesQuery, s.queue, requestID, bounds.Limit)
	} else {
		rows, err = s.db.QueryContext(ctx, listRequestIssuesAfterQuery, s.queue, requestID,
			after.BuildID, after.BuildID, after.TaskID, after.BuildID, after.TaskID, after.UnitID, bounds.Limit)
	}
	if err != nil {
		return nil, fmt.Errorf("list request issue summaries: %w", err)
	}
	defer rows.Close()
	summaries := []entity.RequestIssueSummary{}
	for rows.Next() {
		var summary entity.RequestIssueSummary
		if err := rows.Scan(&summary.RequestID, &summary.BuildID, &summary.TaskID, &summary.UnitID, &summary.FixID, &summary.Version); err != nil {
			return nil, fmt.Errorf("scan request issue summary: %w", err)
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate request issue summaries: %w", err)
	}
	return summaries, nil
}

func (s *requestIssueSummaryStore) validateSummary(summary entity.RequestIssueSummary) error {
	if err := summary.Validate(); err != nil {
		return err
	}
	if len(summary.FixID) > 1020 {
		return fmt.Errorf("request issue repair ID exceeds 1020-byte limit")
	}
	return validateBuildIssueKey(s.queue, summary.BuildID, summary.TaskID, summary.UnitID)
}

var _ storage.RequestIssueSummaryStore = (*requestIssueSummaryStore)(nil)
