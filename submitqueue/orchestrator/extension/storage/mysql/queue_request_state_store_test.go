// Copyright (c) 2026 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestQueueRequestStateStore_List(t *testing.T) {
	first := entity.QueueRequestState{Queue: "monorepo", State: entity.RequestStateValidated, RequestID: "1", EnteredAtMs: 100}
	second := entity.QueueRequestState{Queue: "monorepo", State: entity.RequestStateValidated, RequestID: "2", EnteredAtMs: 200}
	columns := []string{"queue", "state", "request_id", "entered_at_ms"}
	storeErr := errors.New("storage failed")

	tests := []struct {
		name    string
		setup   func(sqlmock.Sqlmock)
		want    []entity.QueueRequestState
		wantErr bool
	}{
		{
			name: "success",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, state, request_id, entered_at_ms FROM queue_request_state").
					WithArgs("monorepo", string(entity.RequestStateValidated)).
					WillReturnRows(sqlmock.NewRows(columns).
						AddRow(first.Queue, string(first.State), first.RequestID, first.EnteredAtMs).
						AddRow(second.Queue, string(second.State), second.RequestID, second.EnteredAtMs))
			},
			want: []entity.QueueRequestState{first, second},
		},
		{
			name: "no request in the state",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, state, request_id, entered_at_ms FROM queue_request_state").
					WithArgs("monorepo", string(entity.RequestStateValidated)).
					WillReturnRows(sqlmock.NewRows(columns))
			},
		},
		{
			name: "query fails",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, state, request_id, entered_at_ms FROM queue_request_state").
					WithArgs("monorepo", string(entity.RequestStateValidated)).
					WillReturnError(storeErr)
			},
			wantErr: true,
		},
		{
			name: "row iteration fails",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT queue, state, request_id, entered_at_ms FROM queue_request_state").
					WithArgs("monorepo", string(entity.RequestStateValidated)).
					WillReturnRows(sqlmock.NewRows(columns).
						AddRow(first.Queue, string(first.State), first.RequestID, first.EnteredAtMs).
						RowError(0, storeErr))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			tt.setup(mock)

			got, err := NewQueueRequestStateStore(db, testMetrics(), "monorepo").List(context.Background(), entity.RequestStateValidated)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDisabledQueueRequestStateStore_ListFails(t *testing.T) {
	_, err := disabledQueueRequestStateStore{}.List(context.Background(), entity.RequestStateStarted)
	require.ErrorIs(t, err, errQueueRequestStateRecordsDisabled)
}
