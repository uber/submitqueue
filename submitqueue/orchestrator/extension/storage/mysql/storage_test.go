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
	assert.NotNil(t, bound.GetRequestStore())
	assert.NotNil(t, bound.GetRequestBatchStore())
	assert.NotNil(t, bound.GetChangeStore())
	assert.NotNil(t, bound.GetBatchStore())
	assert.NotNil(t, bound.GetBatchDependentStore())
	assert.NotNil(t, bound.GetQueueBatchStateStore())
	assert.NotNil(t, bound.GetBuildStore())
	assert.NotNil(t, bound.GetSpeculationPathSetStore())

	_, err = s.For("")
	assert.Error(t, err, "resolving an empty queue name must fail")
}

func TestNewStorage_QueueRequestStateRecords(t *testing.T) {
	tests := []struct {
		name        string
		opts        []StorageOption
		wantListErr error
	}{
		{name: "disabled by default", wantListErr: errQueueRequestStateRecordsDisabled},
		{name: "enabled by option", opts: []StorageOption{WithQueueRequestStateRecords()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			if tt.wantListErr == nil {
				mock.ExpectQuery("SELECT queue, state, request_id, entered_at_ms FROM queue_request_state").
					WillReturnRows(sqlmock.NewRows([]string{"queue", "state", "request_id", "entered_at_ms"}))
			}

			s, err := NewStorage(db, testMetrics(), tt.opts...)
			require.NoError(t, err)
			bound, err := s.For("monorepo")
			require.NoError(t, err)

			_, err = bound.GetQueueRequestStateStore().List(context.Background(), entity.RequestStateStarted)
			if tt.wantListErr != nil {
				require.ErrorIs(t, err, tt.wantListErr)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
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
