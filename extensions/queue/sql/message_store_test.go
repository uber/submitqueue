package sql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap/zaptest"

	"github.com/uber/submitqueue/entities/queue"
)

// testMetrics returns a test metrics scope for use in tests
func testMetrics() tally.Scope {
	return tally.NoopScope
}

func setupmessageStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, messageStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	config := DefaultConfig("test-consumer", "test-worker")
	store := newMessageStore(db, config, zaptest.NewLogger(t), testMetrics())

	return db, mock, store
}

func TestmessageStore_Insert(t *testing.T) {
	tests := []struct {
		name     string
		messages []queue.Message
		setup    func(mock sqlmock.Sqlmock, messages []queue.Message)
		wantErr  bool
	}{
		{
			name: "successful insert with multiple messages",
			messages: []queue.Message{
				{ID: "msg1", Payload: []byte("payload1"), PartitionKey: "part1", PublishedAt: time.Now().UnixMilli()},
				{ID: "msg2", Payload: []byte("payload2"), PartitionKey: "part1", PublishedAt: time.Now().UnixMilli()},
			},
			setup: func(mock sqlmock.Sqlmock, messages []queue.Message) {
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO queue_messages")
				for range messages {
					mock.ExpectExec("INSERT INTO queue_messages").
						WillReturnResult(sqlmock.NewResult(1, 1))
				}
				mock.ExpectCommit()
			},
			wantErr: false,
		},
		{
			name:     "empty messages should succeed",
			messages: []queue.Message{},
			setup:    func(mock sqlmock.Sqlmock, messages []queue.Message) {},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupmessageStoreTest(t)
			defer db.Close()

			tt.setup(mock, tt.messages)

			ctx := context.Background()
			err := store.Insert(ctx, "test_topic", tt.messages)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestmessageStore_Delete(t *testing.T) {
	db, mock, store := setupmessageStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	messageID := "msg1"

	mock.ExpectExec("DELETE FROM queue_messages").
		WithArgs(topic, messageID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := store.Delete(ctx, topic, messageID)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestmessageStore_FetchByOffset(t *testing.T) {
	db, mock, store := setupmessageStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	partitionKey := "part1"
	currentOffset := int64(0)
	limit := 10

	// Expect transaction begin
	mock.ExpectBegin()

	// Mock query results
	rows := sqlmock.NewRows([]string{"offset", "id", "payload", "metadata", "partition_key", "retry_count", "published_at"}).
		AddRow(int64(1), "msg1", []byte("payload1"), []byte("{}"), "part1", 0, time.Now().UnixMilli())

	mock.ExpectQuery("SELECT (.+) FROM queue_messages").
		WithArgs(topic, partitionKey, currentOffset, sqlmock.AnyArg(), limit).
		WillReturnRows(rows)

	// Expect update for visibility timeout
	mock.ExpectExec("UPDATE queue_messages").
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Expect commit
	mock.ExpectCommit()

	results, err := store.FetchByOffset(ctx, topic, partitionKey, currentOffset, limit)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "msg1", results[0].ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestmessageStore_SetVisibilityTimeout(t *testing.T) {
	db, mock, store := setupmessageStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	messageID := "msg1"
	visibilityTimeoutMillis := int64(5000)

	mock.ExpectExec("UPDATE queue_messages").
		WithArgs(sqlmock.AnyArg(), topic, messageID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := store.SetVisibilityTimeout(ctx, topic, messageID, visibilityTimeoutMillis)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestmessageStore_MoveToDLQ(t *testing.T) {
	db, mock, store := setupmessageStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	messageID := "msg1"
	failureCount := 3
	lastError := "test error"

	// Expect transaction begin
	mock.ExpectBegin()

	// Mock query for fetching message - SELECT payload, metadata, partition_key, created_at, published_at
	rows := sqlmock.NewRows([]string{"payload", "metadata", "partition_key", "created_at", "published_at"}).
		AddRow([]byte("payload1"), []byte("{}"), "part1", time.Now().UnixMilli(), time.Now().UnixMilli())

	mock.ExpectQuery("SELECT (.+) FROM queue_messages").
		WithArgs(topic, messageID).
		WillReturnRows(rows)

	// Expect insert into DLQ
	mock.ExpectExec("INSERT INTO queue_dlq").
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Expect delete from main table
	mock.ExpectExec("DELETE FROM queue_messages").
		WithArgs(topic, messageID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Expect commit
	mock.ExpectCommit()

	err := store.MoveToDLQ(ctx, topic, messageID, failureCount, lastError)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
