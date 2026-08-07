// Copyright (c) 2025 Uber Technologies, Inc.
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

type validationFactStore struct {
	db    *sql.DB
	scope tally.Scope
	// queue is the queue name this store instance is bound to; every read and
	// write is scoped to it.
	queue string
}

// NewValidationFactStore creates a new MySQL-backed ValidationFactStore.
func NewValidationFactStore(db *sql.DB, scope tally.Scope, queue string) storage.ValidationFactStore {
	return &validationFactStore{db: db, scope: scope, queue: queue}
}

// Create writes one immutable fact for the bound queue. Returns ErrAlreadyExists if the
// queue already holds a fact for the (uri, project) pair.
func (v *validationFactStore) Create(ctx context.Context, fact entity.ValidationFact) (retErr error) {
	op := metrics.Begin(v.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	queue := v.queue
	_, err := v.db.ExecContext(ctx,
		`INSERT INTO validation_fact (queue, uri, project, degree, request_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		queue,
		fact.URI,
		fact.Project,
		fact.Degree,
		fact.RequestID,
		fact.CreatedAt,
	)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("validation_fact queue=%s uri=%s project=%s: %w", queue, fact.URI, fact.Project, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert validation fact queue=%s uri=%s project=%s: %w", queue, fact.URI, fact.Project, err)
	}

	return nil
}

// Get returns the bound queue's fact for uri and project. Returns ErrNotFound if absent.
func (v *validationFactStore) Get(ctx context.Context, uri, project string) (ret entity.ValidationFact, retErr error) {
	op := metrics.Begin(v.scope, "get", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	queue := v.queue
	var fact entity.ValidationFact
	err := v.db.QueryRowContext(ctx,
		"SELECT uri, project, degree, request_id, created_at FROM validation_fact WHERE queue = ? AND uri = ? AND project = ?",
		queue, uri, project,
	).Scan(
		&fact.URI,
		&fact.Project,
		&fact.Degree,
		&fact.RequestID,
		&fact.CreatedAt,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return entity.ValidationFact{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entity.ValidationFact{}, fmt.Errorf("failed to get validation fact queue=%s uri=%s project=%s from the database: %w", queue, uri, project, err)
	}

	return fact, nil
}
