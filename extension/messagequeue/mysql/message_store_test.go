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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap/zaptest"

	entityqueue "github.com/uber/submitqueue/entity/messagequeue"
)

// testMetrics returns a test metrics scope for use in tests
func testMetrics() tally.Scope {
	return tally.NoopScope
}

func setupmessageStoreTest(t *testing.T) (*sql.DB, sqlmock.Sqlmock, messageStore) {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	store := newMessageStore(db, zaptest.NewLogger(t).Sugar(), testMetrics())

	return db, mock, store
}

func TestMessageStore_Insert(t *testing.T) {
	tests := []struct {
		name     string
		messages []entityqueue.Message
		setup    func(mock sqlmock.Sqlmock, messages []entityqueue.Message)
		wantErr  bool
	}{
		{
			name: "successful insert with multiple messages",
			messages: []entityqueue.Message{
				{ID: "msg1", Payload: []byte("payload1"), PartitionKey: "part1", PublishedAt: time.Now().UnixMilli()},
				{ID: "msg2", Payload: []byte("payload2"), PartitionKey: "part1", PublishedAt: time.Now().UnixMilli()},
			},
			setup: func(mock sqlmock.Sqlmock, messages []entityqueue.Message) {
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
			messages: []entityqueue.Message{},
			setup:    func(mock sqlmock.Sqlmock, messages []entityqueue.Message) {},
			wantErr:  false,
		},
		{
			// Regression: re-publishing the same (topic, partition_key, id) tuple
			// must succeed silently. sqlmock returns 0 affected rows to simulate
			// MySQL's ON DUPLICATE KEY UPDATE swallowing the unique-key collision.
			name: "duplicate publish is idempotent",
			messages: []entityqueue.Message{
				{ID: "msg-dup", Payload: []byte("payload"), PartitionKey: "part1", PublishedAt: time.Now().UnixMilli()},
			},
			setup: func(mock sqlmock.Sqlmock, messages []entityqueue.Message) {
				mock.ExpectBegin()
				mock.ExpectPrepare("INSERT INTO queue_messages")
				mock.ExpectExec("INSERT INTO queue_messages").
					WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectCommit()
			},
			wantErr: false,
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

func TestMessageStore_Delete(t *testing.T) {
	db, mock, store := setupmessageStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	partitionKey := "part1"
	messageID := "msg1"

	mock.ExpectExec("DELETE FROM queue_messages").
		WithArgs(topic, partitionKey, messageID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := store.Delete(ctx, topic, partitionKey, messageID)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMessageStore_FetchByOffset(t *testing.T) {
	db, mock, store := setupmessageStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	partitionKey := "part1"
	currentOffset := int64(0)
	nowMs := time.Now().UnixMilli()
	limit := 10

	// Mock query results (no transaction, simple SELECT)
	rows := sqlmock.NewRows([]string{"offset", "id", "payload", "metadata", "partition_key", "published_at", "failed_at", "failure_count", "last_error", "original_topic"}).
		AddRow(int64(1), "msg1", []byte("payload1"), []byte("{}"), "part1", time.Now().UnixMilli(), int64(0), 0, "", "")

	mock.ExpectQuery("SELECT (.+) FROM queue_messages").
		WithArgs(topic, partitionKey, currentOffset, nowMs, limit).
		WillReturnRows(rows)

	results, err := store.FetchByOffset(ctx, topic, partitionKey, currentOffset, nowMs, limit)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "msg1", results[0].ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMessageStore_FetchByOffset_SkipsDelayed(t *testing.T) {
	db, mock, store := setupmessageStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	partitionKey := "part1"
	currentOffset := int64(0)
	nowMs := int64(1000)
	limit := 10

	// The SQL filter (visible_after <= nowMs) is applied by the DB; sqlmock just
	// verifies the parameter binding. An empty result row simulates the case
	// where the only message is still deferred.
	mock.ExpectQuery("SELECT (.+) FROM queue_messages").
		WithArgs(topic, partitionKey, currentOffset, nowMs, limit).
		WillReturnRows(sqlmock.NewRows([]string{"offset", "id", "payload", "metadata", "partition_key", "published_at", "failed_at", "failure_count", "last_error", "original_topic"}))

	results, err := store.FetchByOffset(ctx, topic, partitionKey, currentOffset, nowMs, limit)
	require.NoError(t, err)
	require.Empty(t, results)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMessageStore_InsertDelayed(t *testing.T) {
	db, mock, store := setupmessageStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	visibleAfter := time.Now().UnixMilli() + 5000
	msg := entityqueue.Message{ID: "msg-delayed", Payload: []byte("p"), PartitionKey: "part1", PublishedAt: time.Now().UnixMilli()}

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO queue_messages")
	mock.ExpectExec("INSERT INTO queue_messages").
		WithArgs("test_topic", msg.ID, msg.Payload, []byte(nil), msg.PartitionKey, sqlmock.AnyArg(), msg.PublishedAt, visibleAfter).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := store.InsertDelayed(ctx, "test_topic", []entityqueue.Message{msg}, visibleAfter)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMessageStore_MoveToDLQ(t *testing.T) {
	db, mock, store := setupmessageStoreTest(t)
	defer db.Close()

	ctx := context.Background()
	topic := "test_topic"
	partitionKey := "part1"
	messageID := "msg1"
	failureCount := 3
	lastError := "test error"
	dlqTopicSuffix := "_dlq"
	dlqTopic := topic + dlqTopicSuffix

	// Expect transaction begin
	mock.ExpectBegin()

	// Mock query for fetching message (now includes partition_key in WHERE)
	rows := sqlmock.NewRows([]string{"payload", "metadata", "partition_key", "created_at", "published_at"}).
		AddRow([]byte("payload1"), []byte(`{"key":"value"}`), "part1", time.Now().UnixMilli(), time.Now().UnixMilli())

	mock.ExpectQuery("SELECT (.+) FROM queue_messages").
		WithArgs(topic, partitionKey, messageID).
		WillReturnRows(rows)

	// Expect insert into queue_messages with DLQ topic
	mock.ExpectExec("INSERT INTO queue_messages").
		WithArgs(dlqTopic, messageID, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), failureCount, lastError, topic).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// Expect delete from main table (now includes partition_key in WHERE)
	mock.ExpectExec("DELETE FROM queue_messages").
		WithArgs(topic, partitionKey, messageID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Expect commit
	mock.ExpectCommit()

	err := store.MoveToDLQ(ctx, topic, partitionKey, messageID, failureCount, lastError, dlqTopicSuffix)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMessageStore_GetOffsetsAbove(t *testing.T) {
	tests := []struct {
		name        string
		afterOffset int64
		limit       int
		offsets     []int64
		wantErr     bool
	}{
		{
			name:        "returns offsets in ascending order",
			afterOffset: 5,
			limit:       1000,
			offsets:     []int64{6, 7, 8},
		},
		{
			name:        "returns offsets with AUTO_INCREMENT gaps",
			afterOffset: 5,
			limit:       1000,
			offsets:     []int64{6, 9, 15},
		},
		{
			name:        "no messages above offset",
			afterOffset: 100,
			limit:       1000,
			offsets:     nil,
		},
		{
			name:        "db error",
			afterOffset: 0,
			limit:       1000,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupmessageStoreTest(t)
			defer db.Close()

			if tt.wantErr {
				mock.ExpectQuery("SELECT offset FROM queue_messages").
					WithArgs("test_topic", "part-1", tt.afterOffset, tt.limit).
					WillReturnError(fmt.Errorf("db error"))
			} else {
				rows := sqlmock.NewRows([]string{"offset"})
				for _, offset := range tt.offsets {
					rows.AddRow(offset)
				}
				mock.ExpectQuery("SELECT offset FROM queue_messages").
					WithArgs("test_topic", "part-1", tt.afterOffset, tt.limit).
					WillReturnRows(rows)
			}

			offsets, err := store.GetOffsetsAbove(context.Background(), "test_topic", "part-1", tt.afterOffset, tt.limit)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.offsets, offsets)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMessageStore_GarbageCollect(t *testing.T) {
	tests := []struct {
		name           string
		minAckedOffset int64
		deleteErr      bool
		wantDeleted    int64
		wantErr        bool
	}{
		{
			name:           "deletes messages up to min offset",
			minAckedOffset: 10,
			wantDeleted:    5,
		},
		{
			name:           "zero offset returns 0 deleted",
			minAckedOffset: 0,
			wantDeleted:    0,
		},
		{
			name:           "delete error",
			minAckedOffset: 10,
			deleteErr:      true,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, store := setupmessageStoreTest(t)
			defer db.Close()

			if tt.minAckedOffset > 0 {
				if tt.deleteErr {
					mock.ExpectExec("DELETE FROM queue_messages").
						WithArgs("test_topic", "part-1", tt.minAckedOffset).
						WillReturnError(fmt.Errorf("db error"))
				} else {
					mock.ExpectExec("DELETE FROM queue_messages").
						WithArgs("test_topic", "part-1", tt.minAckedOffset).
						WillReturnResult(sqlmock.NewResult(0, tt.wantDeleted))
				}
			}

			deleted, err := store.GarbageCollect(context.Background(), "test_topic", "part-1", tt.minAckedOffset)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantDeleted, deleted)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
