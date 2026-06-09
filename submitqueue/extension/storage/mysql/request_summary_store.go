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

	"github.com/uber-go/tally/v4"

	"github.com/uber/submitqueue/core/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

const activeCompletedAtMs = int64(1<<63 - 1)

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
}

// NewRequestSummaryStore creates a new MySQL-backed RequestSummaryStore.
func NewRequestSummaryStore(db *sql.DB, scope tally.Scope) storage.RequestSummaryStore {
	return &requestSummaryStore{db: db, scope: scope}
}

// UpsertFromLog incrementally merges one request-log event into the summary read model.
func (s *requestSummaryStore) UpsertFromLog(ctx context.Context, log entity.RequestLog) (retErr error) {
	op := metrics.Begin(s.scope, "upsert_from_log")
	defer func() { op.Complete(retErr) }()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin request summary upsert transaction for request_id=%s: %w", log.RequestID, err)
	}
	defer func() {
		if retErr != nil {
			_ = tx.Rollback()
		}
	}()

	existing, err := s.getForUpdate(ctx, tx, log.RequestID)
	if errors.Is(err, sql.ErrNoRows) {
		if log.Queue == "" {
			log.Queue = entity.QueueFromRequestID(log.RequestID)
		}
		if log.Queue == "" {
			return fmt.Errorf("request summary upsert requires queue for request_id=%s", log.RequestID)
		}
		if err := s.insert(ctx, tx, rowFromLog(log)); err != nil {
			return err
		}
		return tx.Commit()
	}
	if err != nil {
		return err
	}

	next := mergeSummary(existing, log)
	if err := s.update(ctx, tx, next); err != nil {
		return err
	}
	return tx.Commit()
}

// List returns a page of request summaries matching the queue, time window, and optional statuses.
func (s *requestSummaryStore) List(ctx context.Context, opts storage.RequestSummaryListOptions) (ret storage.RequestSummaryListResult, retErr error) {
	op := metrics.Begin(s.scope, "list")
	defer func() { op.Complete(retErr) }()

	if opts.Limit <= 0 {
		return storage.RequestSummaryListResult{}, fmt.Errorf("request summary list requires a positive limit")
	}

	args := []any{opts.Queue, opts.EndTimeMs, opts.StartTimeMs}
	clauses := []string{
		"queue = ?",
		"started_at_ms < ?",
		"completed_at_ms >= ?",
	}

	if len(opts.Statuses) > 0 {
		placeholders := make([]string, len(opts.Statuses))
		for i, status := range opts.Statuses {
			placeholders[i] = "?"
			args = append(args, status)
		}
		clauses = append(clauses, "status IN ("+strings.Join(placeholders, ", ")+")")
	}

	if opts.Cursor != nil {
		clauses = append(clauses, "(started_at_ms < ? OR (started_at_ms = ? AND request_id < ?))")
		args = append(args, opts.Cursor.StartedAtMs, opts.Cursor.StartedAtMs, opts.Cursor.RequestID)
	}

	args = append(args, opts.Limit+1)
	query := "SELECT request_id, queue, change_uri, status, request_version, status_timestamp_ms, winner_terminal_version, last_error, metadata, started_at_ms, updated_at_ms, completed_at_ms, terminal FROM request_summary WHERE " +
		strings.Join(clauses, " AND ") +
		" ORDER BY started_at_ms DESC, request_id DESC LIMIT ?"

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

func (s *requestSummaryStore) getForUpdate(ctx context.Context, tx *sql.Tx, requestID string) (requestSummaryRow, error) {
	row := tx.QueryRowContext(ctx,
		"SELECT request_id, queue, change_uri, status, request_version, status_timestamp_ms, winner_terminal_version, last_error, metadata, started_at_ms, updated_at_ms, completed_at_ms, terminal FROM request_summary WHERE request_id = ? FOR UPDATE",
		requestID,
	)
	return scanRequestSummary(row)
}

func (s *requestSummaryStore) insert(ctx context.Context, tx *sql.Tx, row requestSummaryRow) error {
	changeURIsJSON, metadataJSON, err := marshalSummaryJSON(row.summary)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		"INSERT INTO request_summary (request_id, queue, change_uri, status, request_version, status_timestamp_ms, winner_terminal_version, last_error, metadata, started_at_ms, updated_at_ms, completed_at_ms, terminal) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		row.summary.RequestID, row.summary.Queue, changeURIsJSON, row.summary.Status, row.requestVersion, row.statusTimestampMs, row.winnerTerminalVersion, row.summary.LastError, metadataJSON, row.summary.StartedAtMs, row.summary.UpdatedAtMs, row.dbCompletedAtMs, row.summary.Terminal,
	)
	if err != nil {
		return fmt.Errorf("failed to insert request summary request_id=%s: %w", row.summary.RequestID, err)
	}
	return nil
}

func (s *requestSummaryStore) update(ctx context.Context, tx *sql.Tx, row requestSummaryRow) error {
	changeURIsJSON, metadataJSON, err := marshalSummaryJSON(row.summary)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		"UPDATE request_summary SET queue = ?, change_uri = ?, status = ?, request_version = ?, status_timestamp_ms = ?, winner_terminal_version = ?, last_error = ?, metadata = ?, started_at_ms = ?, updated_at_ms = ?, completed_at_ms = ?, terminal = ? WHERE request_id = ?",
		row.summary.Queue, changeURIsJSON, row.summary.Status, row.requestVersion, row.statusTimestampMs, row.winnerTerminalVersion, row.summary.LastError, metadataJSON, row.summary.StartedAtMs, row.summary.UpdatedAtMs, row.dbCompletedAtMs, row.summary.Terminal, row.summary.RequestID,
	)
	if err != nil {
		return fmt.Errorf("failed to update request summary request_id=%s: %w", row.summary.RequestID, err)
	}
	return nil
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
	if log.Queue != "" {
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
