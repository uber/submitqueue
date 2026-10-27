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

type issueTrackingStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewIssueTrackingStore binds operational reconciliation progress to one queue.
func NewIssueTrackingStore(db *sql.DB, scope tally.Scope, queue string) storage.IssueTrackingStore {
	return &issueTrackingStore{db: db, scope: scope, queue: queue}
}

func (s *issueTrackingStore) Get(ctx context.Context) (state entity.IssueTrackingState, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	err := s.db.QueryRowContext(ctx,
		`SELECT request_id, build_id, notification_id, after_task_id, after_unit_id, phase, version FROM issue_tracking WHERE queue = ?`, s.queue).
		Scan(&state.RequestID, &state.BuildID, &state.NotificationID, &state.AfterTaskID, &state.AfterUnitID, &state.Phase, &state.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.IssueTrackingState{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.IssueTrackingState{}, fmt.Errorf("read issue tracking state: %w", err)
	}
	return state, nil
}

func (s *issueTrackingStore) Create(ctx context.Context, state entity.IssueTrackingState) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if err := s.validateState(state); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO issue_tracking (queue, request_id, build_id, notification_id, after_task_id, after_unit_id, phase, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		s.queue, state.RequestID, state.BuildID, state.NotificationID, state.AfterTaskID, state.AfterUnitID, state.Phase, state.Version)
	if isDuplicateEntry(err) {
		return storage.ErrAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create issue tracking state: %w", err)
	}
	return nil
}

func (s *issueTrackingStore) Update(ctx context.Context, state entity.IssueTrackingState, oldVersion, newVersion int32) (retErr error) {
	op := metrics.Begin(s.scope, "update", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	replacement := state
	replacement.Version = newVersion
	if err := s.validateState(replacement); err != nil {
		return err
	}
	if oldVersion <= 0 {
		return fmt.Errorf("issue tracking guard version must be positive")
	}
	result, err := s.db.ExecContext(ctx,
		`UPDATE issue_tracking SET request_id = ?, build_id = ?, notification_id = ?, after_task_id = ?, after_unit_id = ?, phase = ?, version = ? WHERE queue = ? AND version = ?`,
		state.RequestID, state.BuildID, state.NotificationID, state.AfterTaskID, state.AfterUnitID, state.Phase, newVersion, s.queue, oldVersion)
	if err != nil {
		return fmt.Errorf("update issue tracking state: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read issue tracking update count: %w", err)
	}
	if rows != 1 {
		return storage.ErrVersionMismatch
	}
	return nil
}

func (s *issueTrackingStore) validateState(state entity.IssueTrackingState) error {
	if err := state.Validate(); err != nil {
		return err
	}
	// The shared key validator below covers queue, build, task, and unit lengths.
	if len(state.NotificationID) > 1020 {
		return fmt.Errorf("issue tracking notification ID exceeds 1020-byte limit")
	}
	return validateBuildIssueKey(s.queue, state.BuildID, state.AfterTaskID, state.AfterUnitID)
}

var _ storage.IssueTrackingStore = (*issueTrackingStore)(nil)
