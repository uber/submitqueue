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
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

const activeCompletedAtMs = int64(1<<63 - 1)

// maxSummaryUpsertAttempts bounds the optimistic-concurrency retry loop in UpsertFromLog. Writes
// to a single request's summary are almost always serialized through the pipeline, so contention
// is rare and a small bound is sufficient; exceeding it surfaces as storage.ErrVersionMismatch.
const maxSummaryUpsertAttempts = 8

// initialSummaryVersion is the optimistic-lock version a freshly inserted summary row starts at.
const initialSummaryVersion = int64(1)

type requestSummaryStore struct {
	db    *sql.DB
	scope tally.Scope
}

type requestSummaryRow struct {
	summary               entity.RequestSummary
	requestVersion        int32
	statusTimestampMs     int64
	winnerTerminalVersion bool
	dbCompletedAtMs       int64
	// version is the optimistic-lock version of the summary row itself, distinct from
	// requestVersion (the reconciliation version sourced from the Request entity). It is an
	// internal read-model concern, populated on read and never exposed through the
	// RequestSummaryStore interface; insert owns the initial value.
	version int64
}

// NewRequestSummaryStore creates a new MySQL-backed RequestSummaryStore.
func NewRequestSummaryStore(db *sql.DB, scope tally.Scope) storage.RequestSummaryStore {
	return &requestSummaryStore{db: db, scope: scope}
}

// UpsertFromLog incrementally merges one request-log event into the summary read model.
//
// This repo forbids database transactions, so the merge uses optimistic concurrency instead of a
// SELECT ... FOR UPDATE: read the current summary without a lock, merge the incoming log in memory,
// then write back with a conditional update guarded by the row version (the same pattern as
// requestStore.UpdateState). A concurrent writer — another gateway write path or a redelivered log
// event — invalidates the read, in which case we re-read and re-merge. Merges are monotonic (the
// winner only advances by request version or timestamp), so the loop converges; re-applying a log
// that has already been merged is a no-op.
func (s *requestSummaryStore) UpsertFromLog(ctx context.Context, log entity.RequestLog) (retErr error) {
	op := metrics.Begin(s.scope, "upsert_from_log")
	defer func() { op.Complete(retErr) }()

	if log.Queue == "" {
		log.Queue = entity.QueueFromRequestID(log.RequestID)
	}
	if log.Queue == "" {
		return fmt.Errorf("request summary upsert requires queue for request_id=%s", log.RequestID)
	}

	for attempt := 0; attempt < maxSummaryUpsertAttempts; attempt++ {
		existing, found, err := s.get(ctx, log.Queue, log.RequestID)
		if err != nil {
			return err
		}

		if !found {
			inserted, err := s.insert(ctx, rowFromLog(log))
			if err != nil {
				return err
			}
			if inserted {
				return nil
			}
			// Lost the insert race; another writer created the row first. Retry into the merge path.
			continue
		}

		// Version arithmetic is owned here, not in the conditional write: compute the next version
		// and only assign it on a successful write (newVersion = oldVersion + 1).
		next := mergeSummary(existing, log)
		updated, err := s.update(ctx, existing.version, existing.version+1, next)
		if err != nil {
			return err
		}
		if updated {
			return nil
		}
		// The row version moved under us; re-read and merge against the new winner.
	}

	return fmt.Errorf("request summary upsert for request_id=%s did not converge after %d attempts: %w", log.RequestID, maxSummaryUpsertAttempts, storage.ErrVersionMismatch)
}

// List returns a page of request summaries matching the queue, time window, and optional statuses.
func (s *requestSummaryStore) List(ctx context.Context, opts storage.RequestSummaryListOptions) (ret storage.RequestSummaryListResult, retErr error) {
	op := metrics.Begin(s.scope, "list")
	defer func() { op.Complete(retErr) }()

	if opts.Limit <= 0 {
		return storage.RequestSummaryListResult{}, fmt.Errorf("request summary list requires a positive limit")
	}

	// request_summary intentionally has no secondary indexes. The composite primary key is
	// (queue, request_id), so List is a queue-scoped scan with SQL-side filtering, sorting, and
	// pagination over the queue's retained summary rows.
	args := []any{opts.Queue, opts.EndTimeMs, opts.StartTimeMs}
	clauses := []string{
		"queue = ?",
		"started_at_ms < ?",
		"completed_at_ms > ?",
	}

	if len(opts.Statuses) > 0 {
		placeholders := make([]string, len(opts.Statuses))
		for i, status := range opts.Statuses {
			placeholders[i] = "?"
			args = append(args, status)
		}
		clauses = append(clauses, "status IN ("+strings.Join(placeholders, ", ")+")")
	}

	cursorClause, orderBy, err := listSortSQL(opts.Sort)
	if err != nil {
		return storage.RequestSummaryListResult{}, err
	}
	if opts.Cursor != nil {
		clauses = append(clauses, cursorClause)
		args = append(args, opts.Cursor.StartedAtMs, opts.Cursor.StartedAtMs, opts.Cursor.RequestID)
	}

	args = append(args, opts.Limit+1)
	query := "SELECT request_id, queue, change_uri, status, request_version, status_timestamp_ms, winner_terminal_version, last_error, metadata, started_at_ms, updated_at_ms, completed_at_ms, terminal, version FROM request_summary WHERE " +
		strings.Join(clauses, " AND ") +
		orderBy +
		" LIMIT ?"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return storage.RequestSummaryListResult{}, fmt.Errorf("failed to list request summaries for queue=%s: %w", opts.Queue, err)
	}
	defer rows.Close()

	var summaries []entity.RequestSummary
	for rows.Next() {
		row, err := scanRequestSummary(rows)
		if err != nil {
			return storage.RequestSummaryListResult{}, err
		}
		summaries = append(summaries, row.summary)
	}
	if err := rows.Err(); err != nil {
		return storage.RequestSummaryListResult{}, fmt.Errorf("failed to iterate request summaries for queue=%s: %w", opts.Queue, err)
	}

	var nextCursor *storage.RequestSummaryCursor
	if len(summaries) > opts.Limit {
		summaries = summaries[:opts.Limit]
		last := summaries[len(summaries)-1]
		nextCursor = &storage.RequestSummaryCursor{StartedAtMs: last.StartedAtMs, RequestID: last.RequestID}
	}

	return storage.RequestSummaryListResult{Requests: summaries, NextCursor: nextCursor}, nil
}

func listSortSQL(sort storage.RequestSummarySort) (cursorClause string, orderBy string, err error) {
	switch sort {
	case storage.RequestSummarySortAdmittedAsc, "":
		return "(started_at_ms > ? OR (started_at_ms = ? AND request_id > ?))",
			" ORDER BY started_at_ms ASC, request_id ASC",
			nil
	case storage.RequestSummarySortAdmittedDesc:
		return "(started_at_ms < ? OR (started_at_ms = ? AND request_id < ?))",
			" ORDER BY started_at_ms DESC, request_id DESC",
			nil
	default:
		return "", "", fmt.Errorf("unsupported request summary sort %q", sort)
	}
}

// get reads the current summary row without locking. Returns found=false when no row exists.
func (s *requestSummaryStore) get(ctx context.Context, queue string, requestID string) (requestSummaryRow, bool, error) {
	row := s.db.QueryRowContext(ctx,
		"SELECT request_id, queue, change_uri, status, request_version, status_timestamp_ms, winner_terminal_version, last_error, metadata, started_at_ms, updated_at_ms, completed_at_ms, terminal, version FROM request_summary WHERE queue = ? AND request_id = ?",
		queue, requestID,
	)
	summary, err := scanRequestSummary(row)
	if errors.Is(err, sql.ErrNoRows) {
		return requestSummaryRow{}, false, nil
	}
	if err != nil {
		return requestSummaryRow{}, false, fmt.Errorf("failed to get request summary for queue=%s request_id=%s: %w", queue, requestID, err)
	}
	return summary, true, nil
}

// insert creates a fresh summary row at version 1. Returns inserted=false (no error) when a
// concurrent writer already created the row (duplicate primary key), so the caller can re-read
// and merge instead.
func (s *requestSummaryStore) insert(ctx context.Context, row requestSummaryRow) (bool, error) {
	changeURIsJSON, metadataJSON, err := marshalSummaryJSON(row.summary)
	if err != nil {
		return false, err
	}
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO request_summary (request_id, queue, change_uri, status, request_version, status_timestamp_ms, winner_terminal_version, last_error, metadata, started_at_ms, updated_at_ms, completed_at_ms, terminal, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		row.summary.RequestID, row.summary.Queue, changeURIsJSON, row.summary.Status, row.requestVersion, row.statusTimestampMs, row.winnerTerminalVersion, row.summary.LastError, metadataJSON, row.summary.StartedAtMs, row.summary.UpdatedAtMs, row.dbCompletedAtMs, row.summary.Terminal, initialSummaryVersion,
	)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		// MySQL error 1062 is "Duplicate entry": another writer inserted this request_id first.
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return false, nil
		}
		return false, fmt.Errorf("failed to insert request summary request_id=%s: %w", row.summary.RequestID, err)
	}
	return true, nil
}

// update is a pure conditional write guarded by the row version: it writes newVersion only if the
// persisted version still matches oldVersion. Returns updated=false (no error) on a version
// mismatch so the caller can re-read and retry. Version arithmetic is owned by the caller.
func (s *requestSummaryStore) update(ctx context.Context, oldVersion, newVersion int64, row requestSummaryRow) (bool, error) {
	changeURIsJSON, metadataJSON, err := marshalSummaryJSON(row.summary)
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx,
		"UPDATE request_summary SET change_uri = ?, status = ?, request_version = ?, status_timestamp_ms = ?, winner_terminal_version = ?, last_error = ?, metadata = ?, started_at_ms = ?, updated_at_ms = ?, completed_at_ms = ?, terminal = ?, version = ? WHERE queue = ? AND request_id = ? AND version = ?",
		changeURIsJSON, row.summary.Status, row.requestVersion, row.statusTimestampMs, row.winnerTerminalVersion, row.summary.LastError, metadataJSON, row.summary.StartedAtMs, row.summary.UpdatedAtMs, row.dbCompletedAtMs, row.summary.Terminal, newVersion, row.summary.Queue, row.summary.RequestID, oldVersion,
	)
	if err != nil {
		return false, fmt.Errorf("failed to update request summary request_id=%s: %w", row.summary.RequestID, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to read rows affected for request summary request_id=%s: %w", row.summary.RequestID, err)
	}
	return rowsAffected == 1, nil
}

type summaryScanner interface {
	Scan(dest ...any) error
}

func scanRequestSummary(scanner summaryScanner) (requestSummaryRow, error) {
	var row requestSummaryRow
	var changeURIsJSON []byte
	var metadataJSON []byte
	err := scanner.Scan(
		&row.summary.RequestID,
		&row.summary.Queue,
		&changeURIsJSON,
		&row.summary.Status,
		&row.requestVersion,
		&row.statusTimestampMs,
		&row.winnerTerminalVersion,
		&row.summary.LastError,
		&metadataJSON,
		&row.summary.StartedAtMs,
		&row.summary.UpdatedAtMs,
		&row.dbCompletedAtMs,
		&row.summary.Terminal,
		&row.version,
	)
	if err != nil {
		return requestSummaryRow{}, err
	}
	if err := json.Unmarshal(changeURIsJSON, &row.summary.ChangeURIs); err != nil {
		return requestSummaryRow{}, fmt.Errorf("failed to unmarshal request summary change URIs for request_id=%s: %w", row.summary.RequestID, err)
	}
	if err := json.Unmarshal(metadataJSON, &row.summary.Metadata); err != nil {
		return requestSummaryRow{}, fmt.Errorf("failed to unmarshal request summary metadata for request_id=%s: %w", row.summary.RequestID, err)
	}
	if row.summary.ChangeURIs == nil {
		row.summary.ChangeURIs = []string{}
	}
	if row.summary.Metadata == nil {
		row.summary.Metadata = map[string]string{}
	}
	if row.dbCompletedAtMs == activeCompletedAtMs {
		row.summary.CompletedAtMs = 0
	} else {
		row.summary.CompletedAtMs = row.dbCompletedAtMs
	}
	return row, nil
}

func rowFromLog(log entity.RequestLog) requestSummaryRow {
	terminal := entity.IsRequestStateTerminal(entity.RequestState(string(log.Status)))
	completedAtMs := activeCompletedAtMs
	if terminal {
		completedAtMs = log.TimestampMs
	}
	return requestSummaryRow{
		summary: entity.RequestSummary{
			RequestID:     log.RequestID,
			Queue:         log.Queue,
			ChangeURIs:    append([]string(nil), log.ChangeURIs...),
			Status:        log.Status,
			LastError:     log.LastError,
			Metadata:      cloneMetadata(log.Metadata),
			StartedAtMs:   log.TimestampMs,
			UpdatedAtMs:   log.TimestampMs,
			CompletedAtMs: completedAtMsForEntity(completedAtMs),
			Terminal:      terminal,
		},
		requestVersion:        log.RequestVersion,
		statusTimestampMs:     log.TimestampMs,
		winnerTerminalVersion: isTerminalVersion(log),
		dbCompletedAtMs:       completedAtMs,
	}
}

func mergeSummary(existing requestSummaryRow, log entity.RequestLog) requestSummaryRow {
	next := existing
	if next.summary.Queue == "" && log.Queue != "" {
		next.summary.Queue = log.Queue
	}
	if len(next.summary.ChangeURIs) == 0 && len(log.ChangeURIs) > 0 {
		next.summary.ChangeURIs = append([]string(nil), log.ChangeURIs...)
	}
	if log.TimestampMs > 0 && (next.summary.StartedAtMs == 0 || log.TimestampMs < next.summary.StartedAtMs) {
		next.summary.StartedAtMs = log.TimestampMs
	}

	if shouldReplaceWinner(existing, log) {
		incoming := rowFromLog(log)
		next.summary.Status = incoming.summary.Status
		next.summary.LastError = incoming.summary.LastError
		next.summary.Metadata = incoming.summary.Metadata
		next.summary.UpdatedAtMs = incoming.summary.UpdatedAtMs
		next.summary.CompletedAtMs = incoming.summary.CompletedAtMs
		next.summary.Terminal = incoming.summary.Terminal
		next.requestVersion = incoming.requestVersion
		next.statusTimestampMs = incoming.statusTimestampMs
		next.winnerTerminalVersion = incoming.winnerTerminalVersion
		next.dbCompletedAtMs = incoming.dbCompletedAtMs
	}
	return next
}

func shouldReplaceWinner(existing requestSummaryRow, log entity.RequestLog) bool {
	incomingTerminalVersion := isTerminalVersion(log)
	if incomingTerminalVersion {
		if !existing.winnerTerminalVersion {
			return true
		}
		return log.RequestVersion > existing.requestVersion ||
			(log.RequestVersion == existing.requestVersion && log.TimestampMs > existing.statusTimestampMs)
	}
	if existing.winnerTerminalVersion {
		return false
	}
	return log.TimestampMs > existing.statusTimestampMs
}

func isTerminalVersion(log entity.RequestLog) bool {
	return log.RequestVersion > 0 && entity.IsRequestStateTerminal(entity.RequestState(string(log.Status)))
}

func completedAtMsForEntity(dbValue int64) int64 {
	if dbValue == activeCompletedAtMs {
		return 0
	}
	return dbValue
}

func marshalSummaryJSON(summary entity.RequestSummary) ([]byte, []byte, error) {
	changeURIs := summary.ChangeURIs
	if changeURIs == nil {
		changeURIs = []string{}
	}
	changeURIsJSON, err := json.Marshal(changeURIs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal request summary change URIs for request_id=%s: %w", summary.RequestID, err)
	}
	metadataJSON, err := json.Marshal(summary.Metadata)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal request summary metadata for request_id=%s: %w", summary.RequestID, err)
	}
	return changeURIsJSON, metadataJSON, nil
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return map[string]string{}
	}
	clone := make(map[string]string, len(metadata))
	for k, v := range metadata {
		clone[k] = v
	}
	return clone
}
