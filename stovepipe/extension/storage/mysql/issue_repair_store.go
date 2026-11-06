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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

type issueRepairStore struct {
	db    *sql.DB
	queue string
}

// NewIssueRepairStore binds repair submission state to a queue.
func NewIssueRepairStore(db *sql.DB, queue string) storage.IssueRepairStore {
	return &issueRepairStore{db: db, queue: queue}
}

func (s *issueRepairStore) validateKey(issue entity.OpenIssue, executorID string) error {
	if s.queue == "" || issue.FirstSeenBuildID == "" || issue.TaskID == "" || executorID == "" || len(executorID) > 128 {
		return fmt.Errorf("issue repair requires queue, opening, task, and executor IDs (executor at most 128 bytes)")
	}
	return validateBuildIssueKey(s.queue, issue.FirstSeenBuildID, issue.TaskID, issue.UnitID)
}

func (s *issueRepairStore) Get(ctx context.Context, issue entity.OpenIssue, executorID string) (entity.IssueRepair, error) {
	if err := s.validateKey(issue, executorID); err != nil {
		return entity.IssueRepair{}, err
	}
	result := entity.IssueRepair{Issue: issue, ExecutorID: executorID}
	var metadataJSON []byte
	err := s.db.QueryRowContext(ctx, `SELECT fix_id, skip_reason, metadata, version FROM issue_repair
        WHERE queue=? AND opening_build_id=? AND task_id=? AND unit_id=? AND executor_id=?`,
		s.queue, issue.FirstSeenBuildID, issue.TaskID, issue.UnitID, executorID).Scan(&result.FixID, &result.SkipReason, &metadataJSON, &result.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.IssueRepair{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.IssueRepair{}, fmt.Errorf("read issue repair: %w", err)
	}
	if err := json.Unmarshal(metadataJSON, &result.Metadata); err != nil {
		return entity.IssueRepair{}, fmt.Errorf("decode issue repair metadata: %w", err)
	}
	if result.Metadata == nil {
		result.Metadata = map[string]string{}
	}
	return result, nil
}

func (s *issueRepairStore) Create(ctx context.Context, result entity.IssueRepair) error {
	if err := s.validateRepair(result); err != nil {
		return err
	}
	metadataJSON, err := marshalRepairMetadata(result.Metadata)
	if err != nil {
		return fmt.Errorf("encode issue repair metadata: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO issue_repair
		(queue,opening_build_id,task_id,unit_id,executor_id,fix_id,skip_reason,metadata,version) VALUES (?,?,?,?,?,?,?,?,?)`,
		s.queue, result.Issue.FirstSeenBuildID, result.Issue.TaskID, result.Issue.UnitID, result.ExecutorID, result.FixID, result.SkipReason, metadataJSON, result.Version)
	if isDuplicateEntry(err) {
		return storage.ErrAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create issue repair: %w", err)
	}
	return nil
}

func (s *issueRepairStore) Update(ctx context.Context, repair entity.IssueRepair, oldVersion, newVersion int32) error {
	replacement := repair
	replacement.Version = newVersion
	if err := s.validateRepair(replacement); err != nil {
		return err
	}
	if oldVersion <= 0 {
		return fmt.Errorf("issue repair guard version must be positive")
	}
	metadataJSON, err := marshalRepairMetadata(repair.Metadata)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE issue_repair SET fix_id=?, skip_reason=?, metadata=?, version=?
        WHERE queue=? AND opening_build_id=? AND task_id=? AND unit_id=? AND executor_id=? AND version=?`,
		repair.FixID, repair.SkipReason, metadataJSON, newVersion,
		s.queue, repair.Issue.FirstSeenBuildID, repair.Issue.TaskID, repair.Issue.UnitID, repair.ExecutorID, oldVersion)
	if err != nil {
		return fmt.Errorf("update issue repair: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read issue repair update count: %w", err)
	}
	if rows != 1 {
		return storage.ErrVersionMismatch
	}
	return nil
}

func (s *issueRepairStore) validateRepair(repair entity.IssueRepair) error {
	if err := s.validateKey(repair.Issue, repair.ExecutorID); err != nil {
		return err
	}
	if repair.FixID != "" && repair.SkipReason != "" || len(repair.FixID) > 255 || len(repair.SkipReason) > 255 {
		return fmt.Errorf("issue repair permits at most one fix ID or skip reason (at most 255 bytes)")
	}
	if repair.Version <= 0 {
		return fmt.Errorf("issue repair version must be positive")
	}
	return nil
}

func marshalRepairMetadata(metadata map[string]string) ([]byte, error) {
	if metadata == nil {
		metadata = map[string]string{}
	}
	return json.Marshal(metadata)
}
