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
)

func setupSubscriberHeartbeatStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, subscriberHeartbeatStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := newSubscriberHeartbeatStore(db, tally.NoopScope, time.Now)

	return db, mock, store
}

func TestSubscriberHeartbeatStore_ForTenants(t *testing.T) {
	db, mock, store := setupSubscriberHeartbeatStoreTest(t)
	defer db.Close()

	tenants := []string{"alpha", "beta"}
	ctx := context.Background()

	mock.ExpectExec("INSERT INTO queue_subscriber_heartbeats").
		WithArgs(
			"alpha", testConsumerGroup, "test_topic", testSubscriberName, sqlmock.AnyArg(),
			"beta", testConsumerGroup, "test_topic", testSubscriberName, sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(2, 2))
	mock.ExpectQuery(`(?s)SELECT tenant, subscriber_name FROM queue_subscriber_heartbeats.*deregistered_at = 0`).
		WithArgs("alpha", "beta", testConsumerGroup, "test_topic", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"tenant", "subscriber_name"}).
			AddRow("alpha", "s1").
			AddRow("alpha", "s2").
			AddRow("beta", "s1"))
	mock.ExpectExec("DELETE FROM queue_subscriber_heartbeats").
		WithArgs("alpha", "beta", testConsumerGroup, "test_topic", testSubscriberName).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("DELETE FROM queue_subscriber_heartbeats").
		WithArgs("alpha", "beta", testConsumerGroup, "test_topic", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, store.HeartbeatForTenants(ctx, tenants, "test_topic", testSubscriberName, testConsumerGroup))
	active, err := store.ActiveSubscribersForTenants(ctx, tenants, "test_topic", testConsumerGroup, testLeaseDurationMs)
	require.NoError(t, err)
	require.Equal(t, map[string][]string{"alpha": {"s1", "s2"}, "beta": {"s1"}}, active)
	require.NoError(t, store.DeregisterForTenants(ctx, tenants, "test_topic", testSubscriberName, testConsumerGroup))
	require.NoError(t, store.PurgeStaleForTenants(ctx, tenants, "test_topic", testConsumerGroup, 300_000))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubscriberHeartbeatStore_ForTenants_EmptyIsNoop(t *testing.T) {
	db, mock, store := setupSubscriberHeartbeatStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	require.NoError(t, store.HeartbeatForTenants(ctx, nil, "test_topic", testSubscriberName, testConsumerGroup))
	active, err := store.ActiveSubscribersForTenants(ctx, nil, "test_topic", testConsumerGroup, testLeaseDurationMs)
	require.NoError(t, err)
	require.Empty(t, active)
	require.NoError(t, store.DeregisterForTenants(ctx, nil, "test_topic", testSubscriberName, testConsumerGroup))
	require.NoError(t, store.PurgeStaleForTenants(ctx, nil, "test_topic", testConsumerGroup, 300_000))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubscriberHeartbeatStore_ForTenants_ReregisterAfterDelete(t *testing.T) {
	db, mock, store := setupSubscriberHeartbeatStoreTest(t)
	defer db.Close()

	tenants := []string{"alpha"}
	ctx := context.Background()
	mock.ExpectExec("INSERT INTO queue_subscriber_heartbeats").
		WithArgs("alpha", testConsumerGroup, "test_topic", testSubscriberName, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM queue_subscriber_heartbeats").
		WithArgs("alpha", testConsumerGroup, "test_topic", testSubscriberName).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO queue_subscriber_heartbeats").
		WithArgs("alpha", testConsumerGroup, "test_topic", testSubscriberName, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	require.NoError(t, store.HeartbeatForTenants(ctx, tenants, "test_topic", testSubscriberName, testConsumerGroup))
	require.NoError(t, store.DeregisterForTenants(ctx, tenants, "test_topic", testSubscriberName, testConsumerGroup))
	require.NoError(t, store.HeartbeatForTenants(ctx, tenants, "test_topic", testSubscriberName, testConsumerGroup))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestSubscriberHeartbeatStore_ForTenants_Errors(t *testing.T) {
	tenants := []string{"alpha"}
	dbErr := fmt.Errorf("db error")
	tests := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		call  func(subscriberHeartbeatStore) error
	}{
		{
			name: "heartbeat",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO queue_subscriber_heartbeats").
					WillReturnError(dbErr)
			},
			call: func(store subscriberHeartbeatStore) error {
				return store.HeartbeatForTenants(context.Background(), tenants, "test_topic", testSubscriberName, testConsumerGroup)
			},
		},
		{
			name: "active subscribers",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`(?s)SELECT tenant, subscriber_name FROM queue_subscriber_heartbeats.*deregistered_at = 0`).
					WillReturnError(dbErr)
			},
			call: func(store subscriberHeartbeatStore) error {
				_, err := store.ActiveSubscribersForTenants(context.Background(), tenants, "test_topic", testConsumerGroup, testLeaseDurationMs)
				return err
			},
		},
		{
			name: "deregister",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_subscriber_heartbeats").
					WillReturnError(dbErr)
			},
			call: func(store subscriberHeartbeatStore) error {
				return store.DeregisterForTenants(context.Background(), tenants, "test_topic", testSubscriberName, testConsumerGroup)
			},
		},
		{
			name: "purge stale",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_subscriber_heartbeats").
					WillReturnError(dbErr)
			},
			call: func(store subscriberHeartbeatStore) error {
				return store.PurgeStaleForTenants(context.Background(), tenants, "test_topic", testConsumerGroup, 300_000)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupSubscriberHeartbeatStoreTest(t)
			defer db.Close()
			tt.setup(mock)
			require.Error(t, tt.call(store))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
