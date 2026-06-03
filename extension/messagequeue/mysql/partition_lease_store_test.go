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
	"github.com/uber-go/tally/v4"
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

func TestPartitionLeaseStore_DiscoverAndAcquirePartitions(t *testing.T) {
	tests := []struct {
		name          string
		maxPartitions int
		setup         func(mock sqlmock.Sqlmock)
		wantAcquired  int
		wantErr       bool
	}{
		{
			name:          "unlimited - acquires all available",
			maxPartitions: 0,
			setup: func(mock sqlmock.Sqlmock) {
				// Discover partitions
				rows := sqlmock.NewRows([]string{"partition_key"}).
					AddRow("part1").
					AddRow("part2")
				mock.ExpectQuery("SELECT DISTINCT partition_key FROM queue_messages").
					WithArgs("test_topic").
					WillReturnRows(rows)

				// Acquire part1 - success
				mock.ExpectExec("INSERT INTO queue_partition_leases").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectQuery("SELECT leased_by FROM queue_partition_leases").
					WillReturnRows(sqlmock.NewRows([]string{"leased_by"}).AddRow(testSubscriberName))

				// Acquire part2 - taken by other worker
				mock.ExpectExec("INSERT INTO queue_partition_leases").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectQuery("SELECT leased_by FROM queue_partition_leases").
					WillReturnRows(sqlmock.NewRows([]string{"leased_by"}).AddRow("other-worker"))
			},
			wantAcquired: 1,
		},
		{
			name:          "stops acquiring when cap reached",
			maxPartitions: 2,
			setup: func(mock sqlmock.Sqlmock) {
				// Discover 3 partitions
				rows := sqlmock.NewRows([]string{"partition_key"}).
					AddRow("part1").
					AddRow("part2").
					AddRow("part3")
				mock.ExpectQuery("SELECT DISTINCT partition_key FROM queue_messages").
					WithArgs("test_topic").
					WillReturnRows(rows)

				// Pre-loop GetLeasedPartitions: owns 0 partitions
				mock.ExpectQuery("SELECT partition_key FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic", testSubscriberName).
					WillReturnRows(sqlmock.NewRows([]string{"partition_key"}))

				// Acquire part1 - success
				mock.ExpectExec("INSERT INTO queue_partition_leases").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectQuery("SELECT leased_by FROM queue_partition_leases").
					WillReturnRows(sqlmock.NewRows([]string{"leased_by"}).AddRow(testSubscriberName))

				// Acquire part2 - success (now at cap of 2, stops)
				mock.ExpectExec("INSERT INTO queue_partition_leases").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectQuery("SELECT leased_by FROM queue_partition_leases").
					WillReturnRows(sqlmock.NewRows([]string{"leased_by"}).AddRow(testSubscriberName))

				// part3 is never attempted because ownedCount (2) >= maxPartitions (2)
			},
			wantAcquired: 2,
		},
		{
			name:          "pre-owned partitions count toward cap",
			maxPartitions: 3,
			setup: func(mock sqlmock.Sqlmock) {
				// Discover 3 partitions
				rows := sqlmock.NewRows([]string{"partition_key"}).
					AddRow("part1").
					AddRow("part2").
					AddRow("part3")
				mock.ExpectQuery("SELECT DISTINCT partition_key FROM queue_messages").
					WithArgs("test_topic").
					WillReturnRows(rows)

				// Pre-loop GetLeasedPartitions: already owns 2 partitions
				mock.ExpectQuery("SELECT partition_key FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic", testSubscriberName).
					WillReturnRows(sqlmock.NewRows([]string{"partition_key"}).
						AddRow("existing1").
						AddRow("existing2"))

				// Acquire part1 - success (now at 3, cap reached)
				mock.ExpectExec("INSERT INTO queue_partition_leases").
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectQuery("SELECT leased_by FROM queue_partition_leases").
					WillReturnRows(sqlmock.NewRows([]string{"leased_by"}).AddRow(testSubscriberName))

				// part2, part3 never attempted because ownedCount (3) >= maxPartitions (3)
			},
			wantAcquired: 1,
		},
		{
			name:          "already at cap - acquires nothing",
			maxPartitions: 2,
			setup: func(mock sqlmock.Sqlmock) {
				// Discover 2 partitions
				rows := sqlmock.NewRows([]string{"partition_key"}).
					AddRow("part1").
					AddRow("part2")
				mock.ExpectQuery("SELECT DISTINCT partition_key FROM queue_messages").
					WithArgs("test_topic").
					WillReturnRows(rows)

				// Pre-loop GetLeasedPartitions: already owns 2 partitions (at cap)
				mock.ExpectQuery("SELECT partition_key FROM queue_partition_leases").
					WithArgs(testConsumerGroup, "test_topic", testSubscriberName).
					WillReturnRows(sqlmock.NewRows([]string{"partition_key"}).
						AddRow("existing1").
						AddRow("existing2"))

				// No acquire attempts - immediately breaks
			},
			wantAcquired: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setuppartitionLeaseStoreTest(t)
			defer db.Close()

			ctx := context.Background()
			topic := "test_topic"

			tt.setup(mock)

			acquired, discoveredPartitions, err := store.DiscoverAndAcquirePartitions(ctx, topic, testSubscriberName, testConsumerGroup, testLeaseDurationMs, tt.maxPartitions)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantAcquired, acquired)
				require.NotNil(t, discoveredPartitions)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
