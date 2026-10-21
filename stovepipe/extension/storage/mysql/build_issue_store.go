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

	if err := validateBuildIssueKey(s.queue, buildID, issue.TaskID, issue.UnitID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO build_issue (queue, build_id, task_id, unit_id)
         VALUES (?, ?, ?, ?)`,
		s.queue, buildID, issue.TaskID, issue.UnitID,
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

	if err := validateBuildIssueKey(s.queue, buildID, taskID, unitID); err != nil {
		return entity.BuildIssue{}, err
	}

	var issue entity.BuildIssue
	err := s.db.QueryRowContext(ctx,
		`SELECT task_id, unit_id FROM build_issue
         WHERE queue = ? AND build_id = ? AND task_id = ? AND unit_id = ?`,
		s.queue, buildID, taskID, unitID,
	).Scan(&issue.TaskID, &issue.UnitID)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.BuildIssue{}, storage.WrapNotFound(err)
	}

	if err != nil {
		return entity.BuildIssue{}, fmt.Errorf("failed to get build issue build=%q task=%q unit=%q: %w", buildID, taskID, unitID, err)
	}

	return issue, nil
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
