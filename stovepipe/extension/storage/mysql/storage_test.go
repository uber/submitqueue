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
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"github.com/uber/submitqueue/stovepipe/extension/storage"
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

	bound, err := s.For("monorepo/main")
	require.NoError(t, err)
	assert.NotNil(t, bound.GetRequestStore())
	assert.NotNil(t, bound.GetRequestURIStore())
	assert.NotNil(t, bound.GetRequestLogStore())
	assert.NotNil(t, bound.GetRequestSummaryStore())
	assert.NotNil(t, bound.GetQueueStore())
	assert.NotNil(t, bound.GetBuildStore())

	_, err = s.For("")
	assert.Error(t, err, "resolving an empty queue name must fail")
}

func TestMysqlStorage_Close(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectClose()

	s, err := NewStorage(db, testMetrics())
	require.NoError(t, err)

	require.NoError(t, s.Close())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStorageForQueueMetrics(t *testing.T) {
	tests := []struct {
		name string
		op   string
		read func(storage.Storage, string) error
	}{
		{
			name: "request_store",
			op:   "get",
			read: func(bound storage.Storage, _ string) error {
				_, err := bound.GetRequestStore().Get(context.Background(), "request-id")
				return err
			},
		},
		{
			name: "request_uri_store",
			op:   "get_id_by_uri",
			read: func(bound storage.Storage, _ string) error {
				_, err := bound.GetRequestURIStore().GetIDByURI(context.Background(), "change-uri")
				return err
			},
		},
		{
			name: "request_log_store",
			op:   "get",
			read: func(bound storage.Storage, _ string) error {
				_, err := bound.GetRequestLogStore().Get(context.Background(), "request-id", "log-id")
				return err
			},
		},
		{
			name: "request_summary_store",
			op:   "get",
			read: func(bound storage.Storage, _ string) error {
				_, err := bound.GetRequestSummaryStore().Get(context.Background(), "request-id")
				return err
			},
		},
		{
			name: "queue_store",
			op:   "get",
			read: func(bound storage.Storage, queue string) error {
				_, err := bound.GetQueueStore().Get(context.Background(), queue)
				return err
			},
		},
		{
			name: "build_store",
			op:   "get",
			read: func(bound storage.Storage, _ string) error {
				_, err := bound.GetBuildStore().Get(context.Background(), "build-id")
				return err
			},
		},
		{
			name: "validation_fact_store",
			op:   "get",
			read: func(bound storage.Storage, _ string) error {
				_, err := bound.GetValidationFactStore().Get(context.Background(), "change-uri", "project")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			scope := tally.NewTestScope("storage", map[string]string{"component": "test"})
			s, err := NewStorage(db, scope)
			require.NoError(t, err)
			dbErr := errors.New("database unavailable")
			for _, queue := range []string{"queue-a", "queue-b", "queue-a"} {
				bound, err := s.For(queue)
				require.NoError(t, err)
				mock.ExpectQuery("SELECT").WillReturnError(dbErr)
				require.ErrorIs(t, tt.read(bound, queue), dbErr)
			}

			scope.Counter("unbound").Inc(1)
			snapshot := scope.Snapshot()
			require.Len(t, snapshot.Counters(), 3)
			require.Contains(t, snapshot.Counters(), "storage.unbound+component=test")
			require.Len(t, snapshot.Histograms(), 2)
			for queue, count := range map[string]int64{"queue-a": 2, "queue-b": 1} {
				metric := "storage." + tt.name + "." + tt.op
				start := metric + ".start+component=test,queue=" + queue
				counter, ok := snapshot.Counters()[start]
				require.True(t, ok, "missing metric %s", start)
				assert.Equal(t, count, counter.Value())
				finish := metric + ".finish+component=test,queue=" + queue + ",result=error"
				histogram, ok := snapshot.Histograms()[finish]
				require.True(t, ok, "missing metric %s", finish)
				var samples int64
				for _, value := range histogram.Durations() {
					samples += value
				}
				assert.Equal(t, count, samples)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestStorageForQueueMetricsOutcomes(t *testing.T) {
	for _, tt := range []struct {
		result string
		err    error
	}{
		{result: "success"},
		{result: "error", err: errors.New("database unavailable")},
		{result: "cancel", err: context.Canceled},
	} {
		t.Run(tt.result, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			scope := tally.NewTestScope("storage", nil)
			s, err := NewStorage(db, scope)
			require.NoError(t, err)
			bound, err := s.For("queue-a")
			require.NoError(t, err)
			expectation := mock.ExpectExec("INSERT INTO request_uri").WithArgs("queue-a", "change-uri", "request-id", requestURIInitialVersion)
			if tt.err != nil {
				expectation.WillReturnError(tt.err)
			} else {
				expectation.WillReturnResult(sqlmock.NewResult(1, 1))
			}
			err = bound.GetRequestURIStore().Create(context.Background(), "change-uri", "request-id")
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
			}
			snapshot := scope.Snapshot()
			require.Contains(t, snapshot.Counters(), "storage.request_uri_store.create.start+queue=queue-a")
			require.Contains(t, snapshot.Histograms(), "storage.request_uri_store.create.finish+queue=queue-a,result="+tt.result)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
