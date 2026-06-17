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
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"
)

func setupSubscriberHeartbeatStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, subscriberHeartbeatStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := newSubscriberHeartbeatStore(db, zaptest.NewLogger(t).Sugar(), tally.NoopScope, time.Now)

	return db, mock, store
}

func TestSubscriberHeartbeatStore_Heartbeat(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(mock sqlmock.Sqlmock)
		wantErr bool
	}{
		{
			name: "successfully send heartbeat",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO queue_subscriber_heartbeats").
					WithArgs(testConsumerGroup, "test_topic", testSubscriberName, sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			wantErr: false,
		},
		{
			name: "update existing heartbeat",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO queue_subscriber_heartbeats").
					WithArgs(testConsumerGroup, "test_topic", testSubscriberName, sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(0, 2)) // ON DUPLICATE KEY UPDATE returns 2 for update
			},
			wantErr: false,
		},
		{
			name: "database error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO queue_subscriber_heartbeats").
					WithArgs(testConsumerGroup, "test_topic", testSubscriberName, sqlmock.AnyArg()).
					WillReturnError(fmt.Errorf("db error"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSubscriberHeartbeatStoreTest(t)
			defer db.Close()

			ctx := context.Background()
			tt.setup(mock)

			err := store.Heartbeat(ctx, "test_topic", testSubscriberName, testConsumerGroup)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSubscriberHeartbeatStore_ActiveSubscribers(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		wantNames []string
		wantErr   bool
	}{
		{
			name: "multiple active subscribers",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"subscriber_name"}).
					AddRow("sub-1").AddRow("sub-2").AddRow("sub-3")
				mock.ExpectQuery("SELECT subscriber_name").
					WithArgs(testConsumerGroup, "test_topic", sqlmock.AnyArg()).
					WillReturnRows(rows)
			},
			wantNames: []string{"sub-1", "sub-2", "sub-3"},
			wantErr:   false,
		},
		{
			name: "no active subscribers",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"subscriber_name"})
				mock.ExpectQuery("SELECT subscriber_name").
					WithArgs(testConsumerGroup, "test_topic", sqlmock.AnyArg()).
					WillReturnRows(rows)
			},
			wantNames: nil,
			wantErr:   false,
		},
		{
			name: "database error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT subscriber_name").
					WithArgs(testConsumerGroup, "test_topic", sqlmock.AnyArg()).
					WillReturnError(fmt.Errorf("db error"))
			},
			wantNames: nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSubscriberHeartbeatStoreTest(t)
			defer db.Close()

			ctx := context.Background()
			tt.setup(mock)

			names, err := store.ActiveSubscribers(ctx, "test_topic", testConsumerGroup, testLeaseDurationMs)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantNames, names)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSubscriberHeartbeatStore_ActiveSubscribers_ExcludesDeregistered(t *testing.T) {
	db, mock, store := setupSubscriberHeartbeatStoreTest(t)
	defer db.Close()

	ctx := context.Background()

	// Verify the query filters by deregistered_at = 0
	rows := sqlmock.NewRows([]string{"subscriber_name"}).AddRow("sub-1").AddRow("sub-2")
	mock.ExpectQuery(`SELECT subscriber_name FROM queue_subscriber_heartbeats.*deregistered_at = 0`).
		WithArgs(testConsumerGroup, "test_topic", sqlmock.AnyArg()).
		WillReturnRows(rows)

	names, err := store.ActiveSubscribers(ctx, "test_topic", testConsumerGroup, testLeaseDurationMs)
	require.NoError(t, err)
	require.Equal(t, []string{"sub-1", "sub-2"}, names)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubscriberHeartbeatStore_Deregister_SoftDelete(t *testing.T) {
	db, mock, store := setupSubscriberHeartbeatStoreTest(t)
	defer db.Close()

	ctx := context.Background()

	// Verify deregister uses UPDATE (not DELETE) and targets only active rows (deregistered_at = 0)
	mock.ExpectExec(`UPDATE queue_subscriber_heartbeats SET deregistered_at.*AND deregistered_at = 0`).
		WithArgs(sqlmock.AnyArg(), testConsumerGroup, "test_topic", testSubscriberName).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := store.Deregister(ctx, "test_topic", testSubscriberName, testConsumerGroup)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubscriberHeartbeatStore_ReRegistration(t *testing.T) {
	db, mock, store := setupSubscriberHeartbeatStoreTest(t)
	defer db.Close()

	ctx := context.Background()

	// Step 1: Initial heartbeat registers the subscriber
	mock.ExpectExec("INSERT INTO queue_subscriber_heartbeats").
		WithArgs(testConsumerGroup, "test_topic", testSubscriberName, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Step 2: Deregister soft-deletes the subscriber
	mock.ExpectExec("UPDATE queue_subscriber_heartbeats").
		WithArgs(sqlmock.AnyArg(), testConsumerGroup, "test_topic", testSubscriberName).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Step 3: Heartbeat again re-registers (ON DUPLICATE KEY UPDATE resets deregistered_at = 0)
	mock.ExpectExec("INSERT INTO queue_subscriber_heartbeats").
		WithArgs(testConsumerGroup, "test_topic", testSubscriberName, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 2)) // 2 = ON DUPLICATE KEY UPDATE

	err := store.Heartbeat(ctx, "test_topic", testSubscriberName, testConsumerGroup)
	require.NoError(t, err)

	err = store.Deregister(ctx, "test_topic", testSubscriberName, testConsumerGroup)
	require.NoError(t, err)

	err = store.Heartbeat(ctx, "test_topic", testSubscriberName, testConsumerGroup)
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubscriberHeartbeatStore_Deregister(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(mock sqlmock.Sqlmock)
		wantErr bool
	}{
		{
			name: "successfully deregister",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE queue_subscriber_heartbeats").
					WithArgs(sqlmock.AnyArg(), testConsumerGroup, "test_topic", testSubscriberName).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantErr: false,
		},
		{
			name: "idempotent - already deregistered",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE queue_subscriber_heartbeats").
					WithArgs(sqlmock.AnyArg(), testConsumerGroup, "test_topic", testSubscriberName).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr: false,
		},
		{
			name: "database error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE queue_subscriber_heartbeats").
					WithArgs(sqlmock.AnyArg(), testConsumerGroup, "test_topic", testSubscriberName).
					WillReturnError(fmt.Errorf("db error"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSubscriberHeartbeatStoreTest(t)
			defer db.Close()

			ctx := context.Background()
			tt.setup(mock)

			err := store.Deregister(ctx, "test_topic", testSubscriberName, testConsumerGroup)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
