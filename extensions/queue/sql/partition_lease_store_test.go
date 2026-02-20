package sql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"
)

func setuppartitionLeaseStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, partitionLeaseStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	config := DefaultConfig("test-consumer", "test-worker")
	store := newPartitionLeaseStore(db, config, zaptest.NewLogger(t), tally.NoopScope)

	return db, mock, store
}

func TestpartitionLeaseStore_TryAcquireLease(t *testing.T) {
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
					WithArgs("test-consumer", "test_topic", "part1", "test-worker", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
				rows := sqlmock.NewRows([]string{"leased_by"}).AddRow("test-worker")
				mock.ExpectQuery("SELECT leased_by FROM queue_partition_leases").
					WithArgs("test-consumer", "test_topic", "part1").
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
					WithArgs("test-consumer", "test_topic", "part1").
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

			acquired, err := store.TryAcquireLease(ctx, topic, partitionKey)
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

func TestpartitionLeaseStore_RenewLease(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(mock sqlmock.Sqlmock)
		wantErr bool
	}{
		{
			name: "successfully renew lease",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE queue_partition_leases").
					WithArgs(sqlmock.AnyArg(), "test-consumer", "test_topic", "part1", "test-worker").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantErr: false,
		},
		{
			name: "lease not owned",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE queue_partition_leases").
					WithArgs(sqlmock.AnyArg(), "test-consumer", "test_topic", "part1", "test-worker").
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

			err := store.RenewLease(ctx, topic, partitionKey)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestpartitionLeaseStore_ReleaseLease(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(mock sqlmock.Sqlmock)
		wantErr bool
	}{
		{
			name: "successfully release lease",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_partition_leases").
					WithArgs("test-consumer", "test_topic", "part1", "test-worker").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			wantErr: false,
		},
		{
			name: "idempotent - already released",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM queue_partition_leases").
					WithArgs("test-consumer", "test_topic", "part1", "test-worker").
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

			err := store.ReleaseLease(ctx, topic, partitionKey)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestpartitionLeaseStore_GetLeasedPartitions(t *testing.T) {
	db, mock, store := setuppartitionLeaseStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"

	rows := sqlmock.NewRows([]string{"partition_key"}).
		AddRow("part1").
		AddRow("part2").
		AddRow("part3")

	mock.ExpectQuery("SELECT partition_key FROM queue_partition_leases").
		WithArgs("test-consumer", topic, "test-worker").
		WillReturnRows(rows)

	partitions, err := store.GetLeasedPartitions(ctx, topic)
	require.NoError(t, err)
	require.Len(t, partitions, 3)
	require.Equal(t, []string{"part1", "part2", "part3"}, partitions)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestpartitionLeaseStore_DiscoverAndAcquirePartitions(t *testing.T) {
	db, mock, store := setuppartitionLeaseStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"

	// Expect query for distinct partition keys
	rows := sqlmock.NewRows([]string{"partition_key"}).
		AddRow("part1").
		AddRow("part2")

	mock.ExpectQuery("SELECT DISTINCT partition_key FROM queue_messages").
		WithArgs(topic).
		WillReturnRows(rows)

	// For each partition, expect acquire attempt
	for i := 0; i < 2; i++ {
		// Expect insert/update
		mock.ExpectExec("INSERT INTO queue_partition_leases").
			WillReturnResult(sqlmock.NewResult(1, 1))

		// Expect ownership check - first one acquired, second not
		owner := "test-worker"
		if i == 1 {
			owner = "other-worker"
		}
		ownerRows := sqlmock.NewRows([]string{"leased_by"}).AddRow(owner)
		mock.ExpectQuery("SELECT leased_by FROM queue_partition_leases").
			WillReturnRows(ownerRows)
	}

	acquired, err := store.DiscoverAndAcquirePartitions(ctx, topic)
	require.NoError(t, err)
	require.Equal(t, 1, acquired) // Only 1 out of 2 was acquired
	require.NoError(t, mock.ExpectationsWereMet())
}
