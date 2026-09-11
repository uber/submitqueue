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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"
)

const testLeaseDurationMs = 30000 // 30 seconds in milliseconds

func setuppartitionLeaseStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, partitionLeaseStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := newPartitionLeaseStore(db, zaptest.NewLogger(t).Sugar(), tally.NoopScope)

	return db, mock, store
}

func TestPartitionLeaseStore_TryAcquireLease(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(mock sqlmock.Sqlmock)
		acquired bool
		wantErr  bool
	}{
		{
			name: "successfully acquire lease",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO queue_partition_leases").
					WithArgs(testTenant, testConsumerGroup, "test_topic", "part1", testSubscriberName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
				rows := sqlmock.NewRows([]string{"leased_by"}).AddRow(testSubscriberName)
				mock.ExpectQuery("SELECT leased_by FROM queue_partition_leases").
					WithArgs(testTenant, testConsumerGroup, "test_topic", "part1").
					WillReturnRows(rows)
			},
			acquired: true,
			wantErr:  false,
		},
		{
			name: "lease acquired by other worker",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO queue_partition_leases").
					WillReturnResult(sqlmock.NewResult(1, 1))
				rows := sqlmock.NewRows([]string{"leased_by"}).AddRow("other-worker")
				mock.ExpectQuery("SELECT leased_by FROM queue_partition_leases").
					WithArgs(testTenant, testConsumerGroup, "test_topic", "part1").
					WillReturnRows(rows)
			},
			acquired: false,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setuppartitionLeaseStoreTest(t)
			defer db.Close()

			ctx := context.Background()
			topic := "test_topic"
			partitionKey := "part1"

			tt.setup(mock)

			acquired, err := store.TryAcquireLease(ctx, testTenant, topic, partitionKey, testSubscriberName, testConsumerGroup, testLeaseDurationMs)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.acquired, acquired)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPartitionLeaseStore_ReleaseLease(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(mock sqlmock.Sqlmock)
		wantErr bool
	}{
		{
			name: "successfully release lease",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_partition_leases").
					WithArgs(testTenant, testConsumerGroup, "test_topic", "part1", testSubscriberName).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantErr: false,
		},
		{
			name: "idempotent - already released",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_partition_leases").
					WithArgs(testTenant, testConsumerGroup, "test_topic", "part1", testSubscriberName).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setuppartitionLeaseStoreTest(t)
			defer db.Close()

			ctx := context.Background()
			topic := "test_topic"
			partitionKey := "part1"

			tt.setup(mock)

			err := store.ReleaseLease(ctx, testTenant, topic, partitionKey, testSubscriberName, testConsumerGroup)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPartitionLeaseStore_DiscoverPartitionsForTenants(t *testing.T) {
	db, mock, store := setuppartitionLeaseStoreTest(t)
	defer db.Close()

	tenants := []string{"alpha", "beta"}
	mock.ExpectQuery("SELECT DISTINCT tenant, partition_key FROM queue_messages").
		WithArgs("alpha", "beta", "test_topic").
		WillReturnRows(sqlmock.NewRows([]string{"tenant", "partition_key"}).
			AddRow("alpha", "p1").
			AddRow("beta", "p2").
			AddRow("beta", "p3"))

	got, err := store.DiscoverPartitions(context.Background(), tenants, "test_topic")
	require.NoError(t, err)
	require.Equal(t, map[string][]string{
		"alpha": {"p1"},
		"beta":  {"p2", "p3"},
	}, got)

	empty, err := store.DiscoverPartitions(context.Background(), nil, "test_topic")
	require.NoError(t, err)
	require.Empty(t, empty)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPartitionLeaseStore_GetLeasedPartitionsForTenants(t *testing.T) {
	db, mock, store := setuppartitionLeaseStoreTest(t)
	defer db.Close()

	tenants := []string{"alpha", "gamma"}
	mock.ExpectQuery("SELECT tenant, partition_key FROM queue_partition_leases").
		WithArgs("alpha", "gamma", testConsumerGroup, "test_topic", testSubscriberName).
		WillReturnRows(sqlmock.NewRows([]string{"tenant", "partition_key"}).
			AddRow("alpha", "p1"))

	got, err := store.GetLeasedPartitionsForTenants(context.Background(), tenants, "test_topic", testSubscriberName, testConsumerGroup)
	require.NoError(t, err)
	require.Equal(t, map[string][]string{"alpha": {"p1"}}, got)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPartitionLeaseStore_GetAllLeasesForTenants(t *testing.T) {
	db, mock, store := setuppartitionLeaseStoreTest(t)
	defer db.Close()

	tenants := []string{"alpha", "beta", "gamma"}
	mock.ExpectQuery("SELECT tenant, partition_key, leased_by, lease_renewed_at FROM queue_partition_leases").
		WithArgs("alpha", "beta", "gamma", testConsumerGroup, "test_topic").
		WillReturnRows(sqlmock.NewRows([]string{"tenant", "partition_key", "leased_by", "lease_renewed_at"}).
			AddRow("alpha", "p1", testSubscriberName, int64(1000)).
			AddRow("beta", "p2", "other-worker", int64(2000)).
			AddRow("beta", "p3", testSubscriberName, int64(3000)))

	got, err := store.GetAllLeasesForTenants(context.Background(), tenants, "test_topic", testConsumerGroup)
	require.NoError(t, err)
	require.Equal(t, map[string][]leaseInfo{
		"alpha": {
			{PartitionKey: "p1", LeasedBy: testSubscriberName, LeaseRenewedAt: 1000},
		},
		"beta": {
			{PartitionKey: "p2", LeasedBy: "other-worker", LeaseRenewedAt: 2000},
			{PartitionKey: "p3", LeasedBy: testSubscriberName, LeaseRenewedAt: 3000},
		},
	}, got)

	empty, err := store.GetAllLeasesForTenants(context.Background(), nil, "test_topic", testConsumerGroup)
	require.NoError(t, err)
	require.Empty(t, empty)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPartitionLeaseStore_RenewAndReleaseOwnedLeases(t *testing.T) {
	db, mock, store := setuppartitionLeaseStoreTest(t)
	defer db.Close()

	tenants := []string{"alpha", "beta"}
	mock.ExpectExec("UPDATE queue_partition_leases").
		WithArgs(sqlmock.AnyArg(), "alpha", "beta", testConsumerGroup, "test_topic", testSubscriberName).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("DELETE FROM queue_partition_leases").
		WithArgs("alpha", "beta", testConsumerGroup, "test_topic", testSubscriberName).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("DELETE FROM queue_partition_leases").
		WithArgs("alpha", "beta", testConsumerGroup, "test_topic", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, store.RenewOwnedLeases(context.Background(), tenants, "test_topic", testSubscriberName, testConsumerGroup))
	require.NoError(t, store.ReleaseOwnedLeases(context.Background(), tenants, "test_topic", testSubscriberName, testConsumerGroup))
	require.NoError(t, store.PurgeStaleForTenants(context.Background(), tenants, "test_topic", testConsumerGroup, 300_000))
	require.NoError(t, mock.ExpectationsWereMet())
}
