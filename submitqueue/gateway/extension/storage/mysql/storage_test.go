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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/uber/submitqueue/submitqueue/entity"
)

// testMetrics returns a test metrics scope for use in tests.
func testMetrics() tally.Scope {
	return tally.NoopScope
}

func TestNewStorage(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	s, err := NewStorage(db, testMetrics())
	require.NoError(t, err)

	bound, err := s.For("monorepo")
	require.NoError(t, err)
	assert.NotNil(t, bound.GetRequestLogStore())
	assert.NotNil(t, bound.GetRequestSummaryStore())
	assert.NotNil(t, bound.GetRequestURIStore())
	assert.NotNil(t, bound.GetQueueRequestStatusStore())

	_, err = s.For("")
	assert.Error(t, err, "resolving an empty queue name must fail")
}

func TestMysqlStorage_Close(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	s, err := NewStorage(db, testMetrics())
	require.NoError(t, err)

	mock.ExpectClose()
	require.NoError(t, s.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestNewStorage_QueueRequestStatusRecords covers the opt-in: without it the deployment may
// not have the table, so record writes touch nothing and List refuses to report an empty
// queue; with it the records are read from and written to the table.
func TestNewStorage_QueueRequestStatusRecords(t *testing.T) {
	record := entity.QueueRequestStatus{Queue: "monorepo", Status: entity.RequestStatusStarted, RequestID: "1", ReceivedAtMs: 10, EnteredAtMs: 20}
	tests := map[string]struct {
		opts        []StorageOption
		setup       func(sqlmock.Sqlmock)
		wantListErr bool
	}{
		"records off": {
			setup:       func(sqlmock.Sqlmock) {},
			wantListErr: true,
		},
		"records on": {
			opts: []StorageOption{WithQueueRequestStatusRecords()},
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO queue_request_status").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("DELETE FROM queue_request_status").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("FROM queue_request_status").
					WithArgs("monorepo", string(entity.RequestStatusStarted)).
					WillReturnRows(sqlmock.NewRows(queueRequestStatusColumns))
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			tt.setup(mock)

			s, err := NewStorage(db, testMetrics(), tt.opts...)
			require.NoError(t, err)
			bound, err := s.For("monorepo")
			require.NoError(t, err)
			records := bound.GetQueueRequestStatusStore()

			require.NoError(t, records.Put(context.Background(), record))
			require.NoError(t, records.Delete(context.Background(), record.Status, record.RequestID))
			got, err := records.List(context.Background(), entity.RequestStatusStarted)
			if tt.wantListErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Empty(t, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
