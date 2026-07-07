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

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/platform/metrics"
	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

type requestSummaryStore struct {
	db    *sql.DB
	scope tally.Scope
}

func NewRequestSummaryStore(db *sql.DB, scope tally.Scope) storage.RequestSummaryStore {
	return &requestSummaryStore{db: db, scope: scope}
}

func (s *requestSummaryStore) Create(ctx context.Context, summary entity.RequestSummary) (retErr error) {
	op := metrics.Begin(s.scope, "create")
	defer func() { op.Complete(retErr) }()

	changeURIsJSON, metadataJSON, err := marshalSummaryJSON(summary)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO request_summary (request_id, queue, change_uri, status, request_version, status_timestamp_ms, winner_terminal_version, last_error, metadata, started_at_ms, updated_at_ms, completed_at_ms, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		summary.RequestID, summary.Queue, changeURIsJSON, summary.Status, summary.RequestVersion, summary.StatusTimestampMs, summary.WinnerTerminalVersion, summary.LastError, metadataJSON, summary.StartedAtMs, summary.UpdatedAtMs, summary.CompletedAtMs, summary.Version,
	)
	if err != nil {
		var mysqlErr *mysqlDriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return fmt.Errorf("request summary request_id=%s: %w", summary.RequestID, storage.ErrAlreadyExists)
		}
		return fmt.Errorf("failed to create request summary request_id=%s: %w", summary.RequestID, err)
	}
	return nil
}

func (s *requestSummaryStore) Get(ctx context.Context, queue, requestID string) (ret entity.RequestSummary, retErr error) {
	op := metrics.Begin(s.scope, "get")
	defer func() { op.Complete(retErr) }()

	row := s.db.QueryRowContext(ctx,
		"SELECT request_id, queue, change_uri, status, request_version, status_timestamp_ms, winner_terminal_version, last_error, metadata, started_at_ms, updated_at_ms, completed_at_ms, version FROM request_summary WHERE queue = ? AND request_id = ?",
		queue, requestID,
	)
	ret, err := scanRequestSummary(row)
	if errors.Is(err, sql.ErrNoRows) {
		return entity.RequestSummary{}, fmt.Errorf("request summary request_id=%s: %w", requestID, storage.ErrNotFound)
	}
	if err != nil {
		return entity.RequestSummary{}, err
	}
	return ret, nil
}

func (s *requestSummaryStore) Update(ctx context.Context, summary entity.RequestSummary, oldVersion, newVersion int64) (retErr error) {
	op := metrics.Begin(s.scope, "update")
	defer func() { op.Complete(retErr) }()

	changeURIsJSON, metadataJSON, err := marshalSummaryJSON(summary)
	if err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx,
		"UPDATE request_summary SET change_uri = ?, status = ?, request_version = ?, status_timestamp_ms = ?, winner_terminal_version = ?, last_error = ?, metadata = ?, started_at_ms = ?, updated_at_ms = ?, completed_at_ms = ?, version = ? WHERE queue = ? AND request_id = ? AND version = ?",
		changeURIsJSON, summary.Status, summary.RequestVersion, summary.StatusTimestampMs, summary.WinnerTerminalVersion, summary.LastError, metadataJSON, summary.StartedAtMs, summary.UpdatedAtMs, summary.CompletedAtMs, newVersion, summary.Queue, summary.RequestID, oldVersion,
	)
	if err != nil {
		return fmt.Errorf("failed to update request summary request_id=%s: %w", summary.RequestID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read updated request summary rows request_id=%s: %w", summary.RequestID, err)
	}
	if affected == 0 {
		return fmt.Errorf("request summary request_id=%s old_version=%d: %w", summary.RequestID, oldVersion, storage.ErrVersionMismatch)
	}
	return nil
}

func (s *requestSummaryStore) List(ctx context.Context, opts storage.RequestSummaryListOptions) (ret storage.RequestSummaryListResult, retErr error) {
	op := metrics.Begin(s.scope, "list")
	defer func() { op.Complete(retErr) }()
	if opts.Limit <= 0 {
		return storage.RequestSummaryListResult{}, fmt.Errorf("request summary list requires a positive limit")
	}
	cursorClause, orderBy, err := listSortSQL(opts.Sort)
	if err != nil {
		return storage.RequestSummaryListResult{}, err
	}
	args := []any{opts.Queue, opts.StartTimeMs, opts.EndTimeMs}
	clauses := []string{"queue = ?", "started_at_ms >= ?", "started_at_ms < ?"}
	if len(opts.Statuses) > 0 {
		placeholders := make([]string, len(opts.Statuses))
		for index, status := range opts.Statuses {
			placeholders[index] = "?"
			args = append(args, status)
		}
		clauses = append(clauses, "status IN ("+strings.Join(placeholders, ", ")+")")
	}
	if opts.Cursor != nil {
		clauses = append(clauses, cursorClause)
		args = append(args, opts.Cursor.StartedAtMs, opts.Cursor.StartedAtMs, opts.Cursor.RequestID)
	}
	args = append(args, opts.Limit+1)
	query := "SELECT request_id, queue, change_uri, status, request_version, status_timestamp_ms, winner_terminal_version, last_error, metadata, started_at_ms, updated_at_ms, completed_at_ms, version FROM request_summary WHERE " + strings.Join(clauses, " AND ") + orderBy + " LIMIT ?"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return storage.RequestSummaryListResult{}, fmt.Errorf("failed to list request summaries for queue=%s: %w", opts.Queue, err)
	}
	defer rows.Close()
	var summaries []entity.RequestSummary
	for rows.Next() {
		summary, err := scanRequestSummary(rows)
		if err != nil {
			return storage.RequestSummaryListResult{}, err
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return storage.RequestSummaryListResult{}, fmt.Errorf("failed to iterate request summaries for queue=%s: %w", opts.Queue, err)
	}
	result := storage.RequestSummaryListResult{Requests: summaries}
	if len(result.Requests) > opts.Limit {
		result.Requests = result.Requests[:opts.Limit]
		last := result.Requests[len(result.Requests)-1]
		result.NextCursor = &storage.RequestSummaryCursor{StartedAtMs: last.StartedAtMs, RequestID: last.RequestID}
	}
	return result, nil
}

func listSortSQL(sort storage.RequestSummarySort) (string, string, error) {
	switch sort {
	case "", storage.RequestSummarySortAdmittedAsc:
		return "(started_at_ms > ? OR (started_at_ms = ? AND request_id > ?))", " ORDER BY started_at_ms ASC, request_id ASC", nil
	case storage.RequestSummarySortAdmittedDesc:
		return "(started_at_ms < ? OR (started_at_ms = ? AND request_id < ?))", " ORDER BY started_at_ms DESC, request_id DESC", nil
	default:
		return "", "", fmt.Errorf("unsupported request summary sort %q", sort)
	}
}

type summaryScanner interface{ Scan(...any) error }

func scanRequestSummary(scanner summaryScanner) (ret entity.RequestSummary, retErr error) {
	var changeURIsJSON, metadataJSON []byte
	var completedAtMs int64
	if err := scanner.Scan(&ret.RequestID, &ret.Queue, &changeURIsJSON, &ret.Status, &ret.RequestVersion, &ret.StatusTimestampMs, &ret.WinnerTerminalVersion, &ret.LastError, &metadataJSON, &ret.StartedAtMs, &ret.UpdatedAtMs, &completedAtMs, &ret.Version); err != nil {
		return entity.RequestSummary{}, fmt.Errorf("failed to scan request summary: %w", err)
	}
	if err := json.Unmarshal(changeURIsJSON, &ret.ChangeURIs); err != nil {
		return entity.RequestSummary{}, fmt.Errorf("failed to unmarshal request summary change URIs request_id=%s: %w", ret.RequestID, err)
	}
	if err := json.Unmarshal(metadataJSON, &ret.Metadata); err != nil {
		return entity.RequestSummary{}, fmt.Errorf("failed to unmarshal request summary metadata request_id=%s: %w", ret.RequestID, err)
	}
	if ret.ChangeURIs == nil {
		ret.ChangeURIs = []string{}
	}
	if ret.Metadata == nil {
		ret.Metadata = map[string]string{}
	}
	ret.CompletedAtMs = completedAtMs
	return ret, nil
}

func marshalSummaryJSON(summary entity.RequestSummary) ([]byte, []byte, error) {
	changeURIs := summary.ChangeURIs
	if changeURIs == nil {
		changeURIs = []string{}
	}
	metadata := summary.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	changeURIsJSON, err := json.Marshal(changeURIs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal request summary change URIs request_id=%s: %w", summary.RequestID, err)
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal request summary metadata request_id=%s: %w", summary.RequestID, err)
	}
	return changeURIsJSON, metadataJSON, nil
}
