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

type issueSummaryStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewIssueSummaryStore binds issue summaries to one queue.
func NewIssueSummaryStore(db *sql.DB, scope tally.Scope, queue string) storage.IssueSummaryStore {
	return &issueSummaryStore{db: db, scope: scope, queue: queue}
}

func (s *issueSummaryStore) Create(ctx context.Context, summary entity.IssueSummary) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := s.validateSummary(summary); err != nil {
		return err
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO issue_summary (queue, task_id, unit_id, status, first_seen_build_id, request_id, build_id, changed_at_ms, version)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.queue, summary.TaskID, summary.UnitID, summary.Status, summary.FirstSeenBuildID,
		summary.RequestID, summary.BuildID, summary.ChangedAtMs, summary.Version)
	if isDuplicateEntry(err) {
		return storage.ErrAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create issue summary: %w", err)
	}
	return nil
}

func (s *issueSummaryStore) Get(ctx context.Context, taskID, unitID string) (summary entity.IssueSummary, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if taskID == "" {
		return entity.IssueSummary{}, fmt.Errorf("issue summary task ID must not be empty")
	}
	if err := validateBuildIssueKey(s.queue, "", taskID, unitID); err != nil {
		return entity.IssueSummary{}, err
	}

	err := s.db.QueryRowContext(ctx,
		`SELECT task_id, unit_id, status, first_seen_build_id, request_id, build_id, changed_at_ms, version
         FROM issue_summary WHERE queue = ? AND task_id = ? AND unit_id = ?`,
		s.queue, taskID, unitID,
	).Scan(&summary.TaskID, &summary.UnitID, &summary.Status, &summary.FirstSeenBuildID,
		&summary.RequestID, &summary.BuildID, &summary.ChangedAtMs, &summary.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.IssueSummary{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.IssueSummary{}, fmt.Errorf("get issue summary: %w", err)
	}
	return summary, nil
}

func (s *issueSummaryStore) Update(ctx context.Context, summary entity.IssueSummary, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	replacement := summary
	replacement.Version = newVersion
	if err := s.validateSummary(replacement); err != nil {
		return err
	}
	if oldVersion <= 0 {
		return fmt.Errorf("issue summary guard version must be positive")
	}

	result, err := s.db.ExecContext(ctx,
		`UPDATE issue_summary SET status = ?, first_seen_build_id = ?, request_id = ?, build_id = ?, changed_at_ms = ?, version = ?
         WHERE queue = ? AND task_id = ? AND unit_id = ? AND version = ?`,
		summary.Status, summary.FirstSeenBuildID, summary.RequestID, summary.BuildID, summary.ChangedAtMs,
		newVersion, s.queue, summary.TaskID, summary.UnitID, oldVersion)
	if err != nil {
		return fmt.Errorf("update issue summary: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read issue summary update count: %w", err)
	}
	if rows != 1 {
		return storage.ErrVersionMismatch
	}
	return nil
}

func (s *issueSummaryStore) validateSummary(summary entity.IssueSummary) error {
	if err := summary.Validate(); err != nil {
		return err
	}
	if err := validateBuildIssueKey(s.queue, summary.BuildID, summary.TaskID, summary.UnitID); err != nil {
		return err
	}
	if err := validateBuildIssueKey(s.queue, summary.FirstSeenBuildID, summary.TaskID, summary.UnitID); err != nil {
		return err
	}
	return nil
}

var _ storage.IssueSummaryStore = (*issueSummaryStore)(nil)
