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

type issueLogStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewIssueLogStore binds issue lifecycle history to queue over the shared database.
func NewIssueLogStore(db *sql.DB, scope tally.Scope, queue string) storage.IssueLogStore {
	return &issueLogStore{db: db, scope: scope, queue: queue}
}

func (s *issueLogStore) Create(ctx context.Context, log entity.IssueLog) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := log.Validate(); err != nil {
		return err
	}
	if err := validateBuildIssueKey(s.queue, log.FirstSeenBuildID, log.TaskID, log.UnitID); err != nil {
		return err
	}
	if err := s.validateKey(log.TaskID, log.UnitID, log.BuildID, log.Event); err != nil {
		return err
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO issue_log (queue, task_id, unit_id, build_id, event, timestamp_ms, first_seen_build_id)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
		s.queue, log.TaskID, log.UnitID, log.BuildID, log.Event, log.TimestampMs, log.FirstSeenBuildID)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("issue log task=%q unit=%q build=%q event=%q: %w",
				log.TaskID, log.UnitID, log.BuildID, log.Event, storage.ErrAlreadyExists)
		}

		return fmt.Errorf("failed to create issue log: %w", err)
	}

	return nil
}

func (s *issueLogStore) Get(ctx context.Context, taskID, unitID, buildID string, event entity.IssueEvent) (ret entity.IssueLog, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := s.validateKey(taskID, unitID, buildID, event); err != nil {
		return entity.IssueLog{}, err
	}

	var log entity.IssueLog
	err := s.db.QueryRowContext(ctx,
		`SELECT task_id, unit_id, build_id, event, timestamp_ms, first_seen_build_id
         FROM issue_log WHERE queue = ? AND task_id = ? AND unit_id = ? AND build_id = ? AND event = ?`,
		s.queue, taskID, unitID, buildID, event,
	).Scan(&log.TaskID, &log.UnitID, &log.BuildID, &log.Event, &log.TimestampMs, &log.FirstSeenBuildID)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.IssueLog{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.IssueLog{}, fmt.Errorf("failed to get issue log: %w", err)
	}

	return log, nil
}

func (s *issueLogStore) validateKey(taskID, unitID, buildID string, event entity.IssueEvent) error {
	if err := validateBuildIssueKey(s.queue, buildID, taskID, unitID); err != nil {
		return err
	}
	if buildID == "" {
		return fmt.Errorf("issue log build ID must not be empty")
	}
	switch event {
	case entity.IssueEventOpened, entity.IssueEventReported, entity.IssueEventClosed:
		return nil
	default:
		return fmt.Errorf("invalid issue event %q", event)
	}
}

var _ storage.IssueLogStore = (*issueLogStore)(nil)

const listIssueLogsQuery = `SELECT task_id, unit_id, build_id, event, timestamp_ms, first_seen_build_id
 FROM issue_log WHERE queue = ? AND task_id = ? AND unit_id = ?`
const issueLogOrder = ` ORDER BY timestamp_ms ASC, build_id ASC, event ASC LIMIT ?`

func (s *issueLogStore) List(ctx context.Context, taskID, unitID string, bounds storage.IssueLogRange) (ret []entity.IssueLog, retErr error) {
	op := metrics.Begin(s.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if taskID == "" || bounds.Limit <= 0 {
		return nil, fmt.Errorf("issue history requires a task and positive limit")
	}
	if err := validateBuildIssueKey(s.queue, "", taskID, unitID); err != nil {
		return nil, err
	}
	query := listIssueLogsQuery
	args := []any{s.queue, taskID, unitID}
	after := bounds.After
	if after != (storage.IssueLogCursor{}) {
		if after.TimestampMs <= 0 {
			return nil, fmt.Errorf("issue history cursor requires a positive timestamp")
		}
		if err := s.validateKey(taskID, unitID, after.BuildID, after.Event); err != nil {
			return nil, err
		}
		query += ` AND (timestamp_ms, build_id, event) > (?, ?, ?)`
		args = append(args, after.TimestampMs, after.BuildID, after.Event)
	}
	args = append(args, bounds.Limit)
	rows, err := s.db.QueryContext(ctx, query+issueLogOrder, args...)
	if err != nil {
		return nil, fmt.Errorf("list issue history: %w", err)
	}
	defer rows.Close()
	logs := make([]entity.IssueLog, 0)
	for rows.Next() {
		var log entity.IssueLog
		if err := rows.Scan(&log.TaskID, &log.UnitID, &log.BuildID, &log.Event, &log.TimestampMs, &log.FirstSeenBuildID); err != nil {
			return nil, fmt.Errorf("scan issue history: %w", err)
		}
		logs = append(logs, log)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate issue history: %w", err)
	}
	return logs, nil
}
