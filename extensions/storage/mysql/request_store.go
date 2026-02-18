package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"github.com/uber/submitqueue/entities"
	"github.com/uber/submitqueue/extensions/storage"
)

const maxCreateRetries = 1000

type requestStore struct {
	db *sql.DB
}

// NewRequestStore creates a new MySQL-backed RequestStore.
func NewRequestStore(db *sql.DB) storage.RequestStore {
	return &requestStore{db: db}
}

// Get retrieves a land request by ID. Returns ErrNotFound if the request is not found.
func (r *requestStore) Get(ctx context.Context, id string) (entities.Request, error) {
	queue, seq, err := entities.ParseRequestID(id)
	if err != nil {
		return entities.Request{}, fmt.Errorf("failed to parse request ID %s: %w", id, err)
	}

	var req entities.Request
	var changeIDsJSON []byte

	err = r.db.QueryRowContext(ctx,
		"SELECT queue, seq, change_source, change_ids, land_strategy, state, version FROM request WHERE queue = ? AND seq = ?",
		queue, seq,
	).Scan(&req.Queue, &req.Seq, &req.Change.Source, &changeIDsJSON, &req.LandStrategy, &req.State, &req.Version)

	if errors.Is(err, sql.ErrNoRows) {
		return entities.Request{}, storage.WrapNotFound(err)
	}
	if err != nil {
		return entities.Request{}, fmt.Errorf("failed to get request entity id=%s from the database: %w", id, err)
	}

	if err := json.Unmarshal(changeIDsJSON, &req.Change.IDs); err != nil {
		return entities.Request{}, fmt.Errorf("failed to unmarshal change IDs for request entity id=%s from the database: %w", id, err)
	}

	return req, nil
}

// Create creates a new land request. Returns the created request object with generated sequence number.
// It uses optimistic locking: obtains the current max sequence number, attempts to insert with seq+1,
// and retries with an incremented sequence number on primary key conflict.
func (r *requestStore) Create(ctx context.Context, queue string, change entities.Change, strategy entities.RequestLandStrategy, state entities.RequestState) (entities.Request, error) {
	changeIDsJSON, err := json.Marshal(change.IDs)
	if err != nil {
		return entities.Request{}, fmt.Errorf("failed to marshal change IDs=%v queue=%s for Create request entity: %w", change.IDs, queue, err)
	}

	var seq int64
	err = r.db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(seq), 0) + 1 FROM request WHERE queue = ?",
		queue,
	).Scan(&seq)
	if err != nil {
		return entities.Request{}, fmt.Errorf("failed to get next sequence number for queue=%s: %w", queue, err)
	}

	// Version always start from 1 as per protocol.
	version := int32(1)

	// retry up to maxCreateRetries times to insert the request entity, incrementing the sequence number on primary key conflict
	for attempt := 0; attempt < maxCreateRetries; attempt++ {
		_, err = r.db.ExecContext(ctx,
			"INSERT INTO request (queue, seq, change_source, change_ids, land_strategy, state, version) VALUES (?, ?, ?, ?, ?, ?, ?)",
			queue, seq, change.Source, changeIDsJSON, strategy, state, version,
		)
		if err == nil {
			return entities.Request{
				Queue:        queue,
				Seq:          seq,
				Change:       change,
				LandStrategy: strategy,
				State:        state,
				Version:      version,
			}, nil
		}

		// if the error is a MySQL primary key conflict error, increment the sequence number and retry
		// It relies on MySQL-specific error code 1062 for primary key conflict. Hopefully this will not change in the future.
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			seq++
			continue
		}

		return entities.Request{}, fmt.Errorf("failed to insert request entity queue=%s seq=%d: %w", queue, seq, err)
	}

	return entities.Request{}, fmt.Errorf("failed to insert request entity queue=%s change=%v: exceeded %d retry attempts due to primary key conflicts", queue, change, maxCreateRetries)
}

// UpdateState updates the state of a land request if the current version matches the expected version. If versions do not match, returns ErrVersionMismatch.
// The implementation increments the version by 1 atomically with the state update.
func (r *requestStore) UpdateState(ctx context.Context, id string, version int32, newState entities.RequestState) error {
	queue, seq, err := entities.ParseRequestID(id)
	if err != nil {
		return fmt.Errorf("failed to parse request ID=%q: %w", id, err)
	}

	result, err := r.db.ExecContext(ctx,
		"UPDATE request SET state = ?, version = version + 1 WHERE queue = ? AND seq = ? AND version = ?",
		newState, queue, seq, version,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to update request state for queue=%q seq=%d version=%d newState=%v: %w",
			queue, seq, version, newState, err,
		)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf(
			"failed to get rows affected from update for queue=%q seq=%d version=%d newState=%v: %w",
			queue, seq, version, newState, err,
		)
	}

	if rowsAffected != 1 {
		return fmt.Errorf(
			"version mismatch for request update: queue=%q seq=%d expected_version=%d newState=%v: %w",
			queue, seq, version, newState, storage.ErrVersionMismatch,
		)
	}

	return nil
}
