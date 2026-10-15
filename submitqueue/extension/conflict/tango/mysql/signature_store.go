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

	"github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/extension/conflict/tango"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type signatureStore struct {
	db    *sql.DB
	scope tally.Scope
	// queue is the queue name this store instance is bound to; every read and
	// write is scoped to it.
	queue string
}

// NewSignatureStore creates a new MySQL-backed TargetSignatureStore.
func NewSignatureStore(db *sql.DB, scope tally.Scope, queue string) tango.SignatureStore {
	return &signatureStore{db: db, scope: scope, queue: queue}
}

// Create inserts a signature. Returns ErrAlreadyExists if the batch already has one.
func (s *signatureStore) Create(ctx context.Context, signature tango.TargetSignature) (retErr error) {
	op := metrics.Begin(s.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if signature.Queue != s.queue {
		return fmt.Errorf("target signature batch_id=%s queue %q does not match the store's bound queue %q", signature.BatchID, signature.Queue, s.queue)
	}

	_, err := s.db.ExecContext(ctx,
		"INSERT INTO target_signature (queue, batch_id, base_revision, targets, created_at) VALUES (?, ?, ?, ?, ?)",
		signature.Queue, signature.BatchID, signature.BaseRevision, encodeSignatureTargets(signature.Targets), signature.CreatedAt,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrDuplicateEntry {
			return fmt.Errorf("target signature batch_id=%s: %w", signature.BatchID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert target signature batch_id=%s: %w", signature.BatchID, err)
	}
	return nil
}

// Get returns the bound queue's signature for batchID. The bool is false when
// the batch has none.
func (s *signatureStore) Get(ctx context.Context, batchID string) (ret tango.TargetSignature, found bool, retErr error) {
	op := metrics.Begin(s.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	signature := tango.TargetSignature{Queue: s.queue, BatchID: batchID}
	var encodedTargets []byte
	err := s.db.QueryRowContext(ctx,
		"SELECT base_revision, targets, created_at FROM target_signature WHERE queue = ? AND batch_id = ?",
		s.queue, batchID,
	).Scan(&signature.BaseRevision, &encodedTargets, &signature.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return tango.TargetSignature{}, false, nil
	}
	if err != nil {
		return tango.TargetSignature{}, false, fmt.Errorf("failed to get target signature batch_id=%s from the database: %w", batchID, err)
	}

	signature.Targets, err = decodeSignatureTargets(encodedTargets)
	if err != nil {
		return tango.TargetSignature{}, false, fmt.Errorf("failed to decode targets of target signature batch_id=%s: %w", batchID, err)
	}
	return signature, true, nil
}
