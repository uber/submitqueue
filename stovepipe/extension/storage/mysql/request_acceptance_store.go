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
	"fmt"

	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/stovepipe/entity"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
)

const listRequestAcceptancesQuery = `
	SELECT queue, accepted_at_ms, request_id
	FROM request_acceptance
	WHERE queue = ? AND accepted_at_ms >= ? AND accepted_at_ms < ?
	ORDER BY accepted_at_ms DESC, request_id DESC LIMIT ?`

const listRequestAcceptancesBeforeCursorQuery = `
	SELECT queue, accepted_at_ms, request_id
	FROM request_acceptance
	WHERE queue = ? AND accepted_at_ms >= ? AND accepted_at_ms < ?
		AND (accepted_at_ms < ? OR (accepted_at_ms = ? AND request_id < ?))
	ORDER BY accepted_at_ms DESC, request_id DESC LIMIT ?`

type requestAcceptanceStore struct {
	db    *sql.DB
	scope tally.Scope
	queue string
}

// NewRequestAcceptanceStore creates a MySQL-backed, queue-scoped RequestAcceptanceStore.
func NewRequestAcceptanceStore(db *sql.DB, scope tally.Scope, queue string) storage.RequestAcceptanceStore {
	return &requestAcceptanceStore{db: db, scope: scope, queue: queue}
}

func (r *requestAcceptanceStore) Create(ctx context.Context, acceptance entity.RequestAcceptance) (retErr error) {
	op := metrics.Begin(r.scope, "create", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if acceptance.Queue != r.queue {
		return fmt.Errorf("request acceptance queue %q does not match the store's bound queue %q", acceptance.Queue, r.queue)
	}
	if acceptance.AcceptedAtMs <= 0 || acceptance.RequestID == "" {
		return fmt.Errorf("request acceptance requires a positive timestamp and nonempty request ID")
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO request_acceptance (queue, accepted_at_ms, request_id)
		VALUES (?, ?, ?)`, acceptance.Queue, acceptance.AcceptedAtMs, acceptance.RequestID)
	if err != nil {
		if isDuplicateEntry(err) {
			return fmt.Errorf("request acceptance request_id=%q: %w", acceptance.RequestID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to insert request acceptance request_id=%q: %w", acceptance.RequestID, err)
	}
	return nil
}

func (r *requestAcceptanceStore) List(ctx context.Context, bounds storage.RequestAcceptanceRange) (ret []entity.RequestAcceptance, retErr error) {
	op := metrics.Begin(r.scope, "list", metrics.StorageLatencyBuckets)
	defer func() { op.Complete(retErr) }()

	if err := validateRequestAcceptanceRange(bounds); err != nil {
		return nil, err
	}

	return r.queryRequestAcceptances(ctx, bounds)
}

func validateRequestAcceptanceRange(bounds storage.RequestAcceptanceRange) error {
	if bounds.AcceptedAtOrAfterMs < 0 || bounds.AcceptedBeforeMs <= bounds.AcceptedAtOrAfterMs || bounds.Limit <= 0 {
		return fmt.Errorf("request acceptance range requires ordered nonnegative bounds and a positive limit")
	}
	cursor := bounds.Before
	if cursor.AcceptedAtMs < 0 || (cursor.AcceptedAtMs == 0) != (cursor.RequestID == "") {
		return fmt.Errorf("request acceptance cursor requires a positive timestamp and nonempty request ID, or the zero value")
	}
	return nil
}

func (r *requestAcceptanceStore) queryRequestAcceptances(ctx context.Context, bounds storage.RequestAcceptanceRange) ([]entity.RequestAcceptance, error) {
	var (
		rows *sql.Rows
		err  error
	)
	cursor := bounds.Before
	if cursor.AcceptedAtMs == 0 {
		rows, err = r.db.QueryContext(ctx, listRequestAcceptancesQuery,
			r.queue, bounds.AcceptedAtOrAfterMs, bounds.AcceptedBeforeMs, bounds.Limit,
		)
	} else {
		rows, err = r.db.QueryContext(ctx, listRequestAcceptancesBeforeCursorQuery,
			r.queue, bounds.AcceptedAtOrAfterMs, bounds.AcceptedBeforeMs,
			cursor.AcceptedAtMs, cursor.AcceptedAtMs, cursor.RequestID, bounds.Limit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list request acceptances queue=%q: %w", r.queue, err)
	}
	defer rows.Close()

	return scanRequestAcceptanceRows(rows, r.queue)
}

func scanRequestAcceptanceRows(rows *sql.Rows, queue string) ([]entity.RequestAcceptance, error) {
	acceptances := make([]entity.RequestAcceptance, 0)
	for rows.Next() {
		var acceptance entity.RequestAcceptance
		if err := rows.Scan(&acceptance.Queue, &acceptance.AcceptedAtMs, &acceptance.RequestID); err != nil {
			return nil, fmt.Errorf("failed to scan request acceptance queue=%q: %w", queue, err)
		}
		acceptances = append(acceptances, acceptance)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate request acceptances queue=%q: %w", queue, err)
	}
	return acceptances, nil
}
