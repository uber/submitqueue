package mysql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const (
	testConsumerGroup  = "test-consumer"
	testSubscriberName = "test-subscriber"
)

func setupoffsetStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, offsetStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := newOffsetStore(db, zaptest.NewLogger(t), testMetrics())

	return db, mock, store
}

func TestoffsetStore_Initialize(t *testing.T) {
	db, mock, store := setupoffsetStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	partitionKey := "part1"

	mock.ExpectExec("INSERT IGNORE INTO queue_offsets").
		WithArgs(testConsumerGroup, topic, partitionKey, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := store.Initialize(ctx, topic, partitionKey, testConsumerGroup)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestoffsetStore_GetAckedOffset(t *testing.T) {
	tests := []struct {
		name           string
		setup          func(mock sqlmock.Sqlmock)
		expectedOffset int64
		wantErr        bool
	}{
		{
			name: "offset found",
			setup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"offset_acked"}).AddRow(int64(100))
				mock.ExpectQuery("SELECT offset_acked FROM queue_offsets").
					WithArgs(testConsumerGroup, "test_topic", "part1").
					WillReturnRows(rows)
			},
			expectedOffset: 100,
			wantErr:        false,
		},
		{
			name: "offset not found returns zero",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT offset_acked FROM queue_offsets").
					WithArgs(testConsumerGroup, "test_topic", "part1").
					WillReturnError(sql.ErrNoRows)
			},
			expectedOffset: 0,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupoffsetStoreTest(t)
			defer db.Close()

			ctx := context.Background()
			topic := "test_topic"
			partitionKey := "part1"

			tt.setup(mock)

			offset, err := store.GetAckedOffset(ctx, topic, partitionKey, testConsumerGroup)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedOffset, offset)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestoffsetStore_UpdateAckedOffset(t *testing.T) {
	db, mock, store := setupoffsetStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	partitionKey := "part1"
	offset := int64(150)

	mock.ExpectExec("UPDATE queue_offsets").
		WithArgs(offset, sqlmock.AnyArg(), testConsumerGroup, topic, partitionKey, offset).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := store.UpdateAckedOffset(ctx, topic, partitionKey, offset, testConsumerGroup)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestoffsetStore_AckMessage(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(mock sqlmock.Sqlmock)
		wantErr bool
	}{
		{
			name: "successful ack",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("DELETE FROM queue_messages").
					WithArgs("test_topic", "part1", "msg1").
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectExec("INSERT INTO queue_offsets").
					WithArgs(testConsumerGroup, "test_topic", "part1", int64(100), sqlmock.AnyArg()).
					WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			},
			wantErr: false,
		},
		{
			name: "transaction error",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("DELETE FROM queue_messages").
					WithArgs("test_topic", "part1", "msg1").
					WillReturnError(sql.ErrConnDone)
				mock.ExpectRollback()
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupoffsetStoreTest(t)
			defer db.Close()

			ctx := context.Background()
			topic := "test_topic"
			partitionKey := "part1"
			messageID := "msg1"
			offset := int64(100)

			tt.setup(mock)

			err := store.AckMessage(ctx, topic, partitionKey, messageID, offset, testConsumerGroup, nil)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
