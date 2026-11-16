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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/submitqueue/submitqueue/entity"
	"github.com/uber/submitqueue/submitqueue/extension/storage"
)

func reconciliationSnapshot() entity.DLQReconciliation {
	return entity.DLQReconciliation{
		ID: "7", Queue: "queue",
		Source:   entity.DLQReconciliationSource{Topic: "speculate.final", PartitionKey: "partition", MessageID: "message"},
		BatchIDs: []string{"1", "2"}, Completed: true, Version: 99,
	}
}

func TestDLQReconciliationStore_Get(t *testing.T) {
	want := reconciliationSnapshot()
	tests := []struct {
		name     string
		rows     *sqlmock.Rows
		queryErr error
		wantErr  error
	}{
		{name: "round trip", rows: sqlmock.NewRows([]string{"id", "batch_ids", "completed", "version"}).AddRow(want.ID, `["1","2"]`, want.Completed, want.Version)},
		{name: "missing", queryErr: sql.ErrNoRows, wantErr: storage.ErrNotFound},
		{name: "database failure", queryErr: errors.New("database unavailable")},
		{name: "corrupt targets", rows: sqlmock.NewRows([]string{"id", "batch_ids", "completed", "version"}).AddRow(want.ID, `{}`, true, 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			query := mock.ExpectQuery("SELECT id, batch_ids, completed, version FROM dlq_reconciliation").
				WithArgs(want.Queue, want.Source.Topic, want.Source.PartitionKey, want.Source.MessageID)
			if tt.queryErr != nil {
				query.WillReturnError(tt.queryErr)
			} else {
				query.WillReturnRows(tt.rows)
			}
			store := NewDLQReconciliationStore(db, testMetrics(), want.Queue)
			got, err := store.Get(context.Background(), want.Source)
			if tt.name == "round trip" {
				require.NoError(t, err)
				assert.Equal(t, want, got)
			} else {
				require.Error(t, err)
				if tt.wantErr != nil {
					assert.ErrorIs(t, err, tt.wantErr)
				}
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDLQReconciliationStore_Write(t *testing.T) {
	databaseFailure := errors.New("database unavailable")
	tests := []struct {
		name       string
		update     bool
		otherQueue bool
		execErr    error
		result     sql.Result
		wantErr    error
	}{
		{name: "create"},
		{name: "duplicate create", execErr: &mysql.MySQLError{Number: mysqlErrDuplicateEntry}, wantErr: storage.ErrAlreadyExists},
		{name: "create failure", execErr: databaseFailure, wantErr: databaseFailure},
		{name: "create queue mismatch", otherQueue: true},
		{name: "replace with explicit version", update: true},
		{name: "stale replacement", update: true, result: sqlmock.NewResult(0, 0), wantErr: storage.ErrVersionMismatch},
		{name: "update failure", update: true, execErr: databaseFailure, wantErr: databaseFailure},
		{name: "update result failure", update: true, result: sqlmock.NewErrorResult(databaseFailure), wantErr: databaseFailure},
		{name: "update queue mismatch", update: true, otherQueue: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			record := reconciliationSnapshot()
			store := NewDLQReconciliationStore(db, testMetrics(), record.Queue)
			if tt.otherQueue {
				record.Queue = "other"
			}
			if !tt.otherQueue {
				var query *sqlmock.ExpectedExec
				if tt.update {
					query = mock.ExpectExec("UPDATE dlq_reconciliation SET batch_ids").
						WithArgs([]byte(`["1","2"]`), record.Completed, int32(4), record.Queue, record.Source.Topic, record.Source.PartitionKey, record.Source.MessageID, record.ID, int32(3))
				} else {
					query = mock.ExpectExec("INSERT INTO dlq_reconciliation").
						WithArgs(record.Queue, record.Source.Topic, record.Source.PartitionKey, record.Source.MessageID, record.ID, []byte(`["1","2"]`), record.Completed, record.Version)
				}
				if tt.execErr != nil {
					query.WillReturnError(tt.execErr)
				} else if tt.result != nil {
					query.WillReturnResult(tt.result)
				} else {
					query.WillReturnResult(sqlmock.NewResult(0, 1))
				}
			}
			if tt.update {
				err = store.Update(context.Background(), record, 3, 4)
			} else {
				err = store.Create(context.Background(), record)
			}
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else if tt.otherQueue {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, int32(99), record.Version)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
