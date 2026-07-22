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
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

// requestURIInitialVersion is the version written for a new mapping. The mapping is insert-once,
// so the column is constant today; it exists for record consistency with the other stores and to
// reserve optimistic-locking semantics if the mapping ever becomes mutable.
const requestURIInitialVersion = 1

type requestURIStore struct {
	db    *sql.DB
	scope tally.Scope
}

// NewRequestURIStore creates a new MySQL-backed RequestURIStore.
func NewRequestURIStore(db *sql.DB, scope tally.Scope) storage.RequestURIStore {
	return &requestURIStore{db: db, scope: scope}
}

// Create records the (queue, uri) -> id reverse index. Returns ErrAlreadyExists if (queue, uri)
// is already mapped to a request.
func (r *requestURIStore) Create(ctx context.Context, queue, uri, id string) (retErr error) {
	op := metrics.Begin(r.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	_, err := r.db.ExecContext(ctx,
		"INSERT INTO request_uri (queue, uri, request_id, version) VALUES (?, ?, ?, ?)",
		queue, uri, id, requestURIInitialVersion,
	)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("request_uri queue=%s uri=%s: %w", queue, uri, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to map queue=%s uri=%s to request id=%s: %w", queue, uri, id, err)
	}

	return nil
}

// GetIDByURI returns the id of the request validating (queue, uri). Returns ErrNotFound if absent.
func (r *requestURIStore) GetIDByURI(ctx context.Context, queue, uri string) (ret string, retErr error) {
	op := metrics.Begin(r.scope, "get_id_by_uri", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	var id string
	err := r.db.QueryRowContext(ctx,
		"SELECT request_id FROM request_uri WHERE queue = ? AND uri = ?",
		queue, uri,
	).Scan(&id)

	if errors.Is(err, sql.ErrNoRows) {
		return "", storage.WrapNotFound(err)
	}
	if err != nil {
		return "", fmt.Errorf("failed to get request id for queue=%s uri=%s from the database: %w", queue, uri, err)
	}

	return id, nil
}
