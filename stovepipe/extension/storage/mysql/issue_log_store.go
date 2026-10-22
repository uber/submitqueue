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
	if err := s.validateKey(log.TaskID, log.UnitID, log.BuildID, log.Event); err != nil {
		return err
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO issue_log (queue, task_id, unit_id, build_id, event, timestamp_ms)
         VALUES (?, ?, ?, ?, ?, ?)`,
		s.queue, log.TaskID, log.UnitID, log.BuildID, log.Event, log.TimestampMs)
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
		`SELECT task_id, unit_id, build_id, event, timestamp_ms
         FROM issue_log WHERE queue = ? AND task_id = ? AND unit_id = ? AND build_id = ? AND event = ?`,
		s.queue, taskID, unitID, buildID, event,
	).Scan(&log.TaskID, &log.UnitID, &log.BuildID, &log.Event, &log.TimestampMs)
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
	case entity.IssueEventOpened, entity.IssueEventResolved:
		return nil
	default:
		return fmt.Errorf("invalid issue event %q", event)
	}
}

var _ storage.IssueLogStore = (*issueLogStore)(nil)
