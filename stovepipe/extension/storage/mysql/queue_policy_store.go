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

type queuePolicyStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewQueuePolicyStore binds policy primitives to one queue over the supplied pool.
func NewQueuePolicyStore(db *sql.DB, scope tally.Scope, queue string) storage.QueuePolicyStore {
	return &queuePolicyStore{db: db, scope: scope, queue: queue}
}

func (s *queuePolicyStore) GetCurrent(ctx context.Context) (head entity.QueuePolicyHead, retErr error) {
	op := metrics.Begin(s.scope, "get_current", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	err := s.db.QueryRowContext(ctx,
		`SELECT queue, transition_id, revision, version FROM queue_policy WHERE queue = ?`, s.queue,
	).Scan(&head.Queue, &head.TransitionID, &head.Revision, &head.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.QueuePolicyHead{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.QueuePolicyHead{}, fmt.Errorf("read queue policy for %q: %w", s.queue, err)
	}
	return head, nil
}

func (s *queuePolicyStore) CreateCurrent(ctx context.Context, head entity.QueuePolicyHead) (retErr error) {
	op := metrics.Begin(s.scope, "create_current", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if head.Queue != s.queue {
		return fmt.Errorf("policy queue %q differs from bound queue %q", head.Queue, s.queue)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO queue_policy (queue, transition_id, revision, version) VALUES (?, ?, ?, ?)`,
		head.Queue, head.TransitionID, head.Revision, head.Version,
	)
	if isDuplicateEntry(err) {
		return storage.ErrAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create queue policy for %q: %w", s.queue, err)
	}
	return nil
}

func (s *queuePolicyStore) UpdateCurrent(ctx context.Context, head entity.QueuePolicyHead, oldVersion, newVersion int64) (retErr error) {
	op := metrics.Begin(s.scope, "update_current", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if head.Queue != s.queue {
		return fmt.Errorf("policy queue %q differs from bound queue %q", head.Queue, s.queue)
	}
	result, err := s.db.ExecContext(ctx,
		`UPDATE queue_policy SET transition_id = ?, revision = ?, version = ? WHERE queue = ? AND version = ?`,
		head.TransitionID, head.Revision, newVersion, s.queue, oldVersion,
	)
	if err != nil {
		return fmt.Errorf("update queue policy for %q: %w", s.queue, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read policy update count for %q: %w", s.queue, err)
	}
	if count != 1 {
		return storage.ErrVersionMismatch
	}
	return nil
}

func (s *queuePolicyStore) GetTransition(ctx context.Context, id string) (transition entity.QueuePolicyTransition, retErr error) {
	op := metrics.Begin(s.scope, "get_transition", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	err := s.db.QueryRowContext(ctx,
		`SELECT queue, id, previous_id, revision, state, effective_from_commit_uri, changed_at_ms
		 FROM queue_policy_transition WHERE queue = ? AND id = ?`, s.queue, id,
	).Scan(&transition.Queue, &transition.ID, &transition.PreviousID, &transition.Policy.Revision,
		&transition.Policy.State, &transition.Policy.EffectiveFromCommitURI, &transition.Policy.ChangedAtMs)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.QueuePolicyTransition{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.QueuePolicyTransition{}, fmt.Errorf("read policy transition %q in queue %q: %w", id, s.queue, err)
	}
	return transition, nil
}

func (s *queuePolicyStore) CreateTransition(ctx context.Context, transition entity.QueuePolicyTransition) (retErr error) {
	op := metrics.Begin(s.scope, "create_transition", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()
	if transition.Queue != s.queue {
		return fmt.Errorf("transition queue %q differs from bound queue %q", transition.Queue, s.queue)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO queue_policy_transition (queue, id, previous_id, revision, state, effective_from_commit_uri, changed_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`, transition.Queue, transition.ID, transition.PreviousID,
		transition.Policy.Revision, transition.Policy.State, transition.Policy.EffectiveFromCommitURI, transition.Policy.ChangedAtMs,
	)
	if isDuplicateEntry(err) {
		return storage.ErrAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create policy transition %q in queue %q: %w", transition.ID, s.queue, err)
	}
	return nil
}
