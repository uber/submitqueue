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
	"time"

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
					WithArgs(testConsumerGroup, "test_topic", "part1", testSubscriberName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
				rows := sqlmock.NewRows([]string{"leased_by"}).AddRow(testSubscriberName)
				mock.ExpectQuery("SELECT leased_by FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic", "part1").
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
					WithArgs(testConsumerGroup, "test_topic", "part1").
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

			acquired, err := store.TryAcquireLease(ctx, topic, partitionKey, testSubscriberName, testConsumerGroup, testLeaseDurationMs)
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

func TestPartitionLeaseStore_RenewLease(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(mock sqlmock.Sqlmock)
		wantErr bool
	}{
		{
			name: "successfully renew lease",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE queue_partition_leases").
					WithArgs(sqlmock.AnyArg(), testConsumerGroup, "test_topic", "part1", testSubscriberName).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantErr: false,
		},
		{
			name: "lease not owned",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE queue_partition_leases").
					WithArgs(sqlmock.AnyArg(), testConsumerGroup, "test_topic", "part1", testSubscriberName).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr: true,
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

			err := store.RenewLease(ctx, topic, partitionKey, testSubscriberName, testConsumerGroup, testLeaseDurationMs)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
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
					WithArgs(testConsumerGroup, "test_topic", "part1", testSubscriberName).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantErr: false,
		},
		{
			name: "idempotent - already released",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic", "part1", testSubscriberName).
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

			err := store.ReleaseLease(ctx, topic, partitionKey, testSubscriberName, testConsumerGroup)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPartitionLeaseStore_GetLeasedPartitions(t *testing.T) {
	db, mock, store := setuppartitionLeaseStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"

	rows := sqlmock.NewRows([]string{"partition_key"}).
		AddRow("part1").
		AddRow("part2").
		AddRow("part3")

	mock.ExpectQuery("SELECT partition_key FROM queue_partition_leases").
		WithArgs(testConsumerGroup, topic, testSubscriberName).
		WillReturnRows(rows)

	partitions, err := store.GetLeasedPartitions(ctx, topic, testSubscriberName, testConsumerGroup)
	require.NoError(t, err)
	require.Len(t, partitions, 3)
	require.Equal(t, []string{"part1", "part2", "part3"}, partitions)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPartitionLeaseStore_GetAllLeases(t *testing.T) {
	tests := []struct {
		name  string
		setup func(mock sqlmock.Sqlmock)
		want  []leaseInfo
	}{
		{
			name: "returns leases held by any subscriber",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"partition_key", "leased_by", "lease_renewed_at"}).
					AddRow("part1", testSubscriberName, int64(1000)).
					AddRow("part2", "other-worker", int64(2000))
				mock.ExpectQuery("SELECT partition_key, leased_by, lease_renewed_at FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic").
					WillReturnRows(rows)
			},
			want: []leaseInfo{
				{PartitionKey: "part1", LeasedBy: testSubscriberName, LeaseRenewedAt: 1000},
				{PartitionKey: "part2", LeasedBy: "other-worker", LeaseRenewedAt: 2000},
			},
		},
		{
			name: "no leases returns empty",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT partition_key, leased_by, lease_renewed_at FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic").
					WillReturnRows(sqlmock.NewRows([]string{"partition_key", "leased_by", "lease_renewed_at"}))
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setuppartitionLeaseStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			leases, err := store.GetAllLeases(context.Background(), "test_topic", testConsumerGroup)
			require.NoError(t, err)
			require.Equal(t, tt.want, leases)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPartitionLeaseStore_DiscoverAndAcquirePartitions(t *testing.T) {
	leaseColumns := []string{"partition_key", "leased_by", "lease_renewed_at"}
	freshMs := time.Now().UnixMilli()
	staleMs := freshMs - testLeaseDurationMs - 60_000

	// expectDiscover mocks the DISTINCT partition scan.
	expectDiscover := func(mock sqlmock.Sqlmock, partitions ...string) {
		rows := sqlmock.NewRows([]string{"partition_key"})
		for _, pk := range partitions {
			rows.AddRow(pk)
		}
		mock.ExpectQuery("SELECT DISTINCT partition_key FROM queue_messages").
			WithArgs("test_topic").
			WillReturnRows(rows)
	}

	// expectAcquire mocks one TryAcquireLease attempt whose ownership check
	// reports the given owner.
	expectAcquire := func(mock sqlmock.Sqlmock, owner string) {
		mock.ExpectExec("INSERT INTO queue_partition_leases").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectQuery("SELECT leased_by FROM queue_partition_leases").
			WillReturnRows(sqlmock.NewRows([]string{"leased_by"}).AddRow(owner))
	}

	tests := []struct {
		name          string
		maxPartitions int
		setup         func(mock sqlmock.Sqlmock)
		wantAcquired  int
	}{
		{
			name:          "acquires unleased, skips fresh lease held by other",
			maxPartitions: 0,
			setup: func(mock sqlmock.Sqlmock) {
				expectDiscover(mock, "part1", "part2")
				mock.ExpectQuery("SELECT partition_key, leased_by, lease_renewed_at FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic").
					WillReturnRows(sqlmock.NewRows(leaseColumns).
						AddRow("part2", "other-worker", freshMs))
				// Only unleased part1 is attempted; part2's fresh lease is
				// never write-probed.
				expectAcquire(mock, testSubscriberName)
			},
			wantAcquired: 1,
		},
		{
			name:          "stale lease held by other is stealable",
			maxPartitions: 0,
			setup: func(mock sqlmock.Sqlmock) {
				expectDiscover(mock, "part1")
				mock.ExpectQuery("SELECT partition_key, leased_by, lease_renewed_at FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic").
					WillReturnRows(sqlmock.NewRows(leaseColumns).
						AddRow("part1", "other-worker", staleMs))
				expectAcquire(mock, testSubscriberName)
			},
			wantAcquired: 1,
		},
		{
			name:          "self-owned partitions are not re-probed",
			maxPartitions: 0,
			setup: func(mock sqlmock.Sqlmock) {
				expectDiscover(mock, "part1", "part2")
				mock.ExpectQuery("SELECT partition_key, leased_by, lease_renewed_at FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic").
					WillReturnRows(sqlmock.NewRows(leaseColumns).
						AddRow("part1", testSubscriberName, freshMs))
				// Only part2 is attempted; renewal of part1 is the lease
				// tick's job.
				expectAcquire(mock, testSubscriberName)
			},
			wantAcquired: 1,
		},
		{
			name:          "stops acquiring when cap reached",
			maxPartitions: 2,
			setup: func(mock sqlmock.Sqlmock) {
				expectDiscover(mock, "part1", "part2", "part3")
				mock.ExpectQuery("SELECT partition_key, leased_by, lease_renewed_at FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic").
					WillReturnRows(sqlmock.NewRows(leaseColumns))
				// part1 and part2 acquired; part3 never attempted at the cap.
				expectAcquire(mock, testSubscriberName)
				expectAcquire(mock, testSubscriberName)
			},
			wantAcquired: 2,
		},
		{
			name:          "pre-owned partitions count toward cap",
			maxPartitions: 3,
			setup: func(mock sqlmock.Sqlmock) {
				expectDiscover(mock, "part1", "part2", "part3")
				mock.ExpectQuery("SELECT partition_key, leased_by, lease_renewed_at FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic").
					WillReturnRows(sqlmock.NewRows(leaseColumns).
						AddRow("existing1", testSubscriberName, freshMs).
						AddRow("existing2", testSubscriberName, freshMs))
				// One acquisition reaches the cap of 3; part2/part3 skipped.
				expectAcquire(mock, testSubscriberName)
			},
			wantAcquired: 1,
		},
		{
			name:          "already at cap acquires nothing",
			maxPartitions: 2,
			setup: func(mock sqlmock.Sqlmock) {
				expectDiscover(mock, "part1", "part2")
				mock.ExpectQuery("SELECT partition_key, leased_by, lease_renewed_at FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic").
					WillReturnRows(sqlmock.NewRows(leaseColumns).
						AddRow("existing1", testSubscriberName, freshMs).
						AddRow("existing2", testSubscriberName, freshMs))
				// No acquire attempts.
			},
			wantAcquired: 0,
		},
		{
			name:          "lost race counts nothing",
			maxPartitions: 0,
			setup: func(mock sqlmock.Sqlmock) {
				expectDiscover(mock, "part1")
				mock.ExpectQuery("SELECT partition_key, leased_by, lease_renewed_at FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic").
					WillReturnRows(sqlmock.NewRows(leaseColumns))
				// Attempted while unleased, but another subscriber won the
				// atomic acquire between the read and the write.
				expectAcquire(mock, "other-worker")
			},
			wantAcquired: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setuppartitionLeaseStoreTest(t)
			defer db.Close()

			tt.setup(mock)

			acquired, discoveredPartitions, err := store.DiscoverAndAcquirePartitions(context.Background(), "test_topic", testSubscriberName, testConsumerGroup, testLeaseDurationMs, tt.maxPartitions)
			require.NoError(t, err)
			require.Equal(t, tt.wantAcquired, acquired)
			require.NotNil(t, discoveredPartitions)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
