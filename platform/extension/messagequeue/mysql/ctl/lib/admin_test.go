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

package lib

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListTopics(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"topic", "count"}).
		AddRow("orders", 10).
		AddRow("payments", 5)
	mock.ExpectQuery("SELECT topic, COUNT\\(\\*\\) FROM queue_messages GROUP BY topic ORDER BY topic").
		WillReturnRows(rows)

	topics, err := store.ListTopics(context.Background())
	require.NoError(t, err)
	assert.Len(t, topics, 2)
	assert.Equal(t, "orders", topics[0].Topic)
	assert.Equal(t, int64(10), topics[0].MessageCount)
	assert.Equal(t, "payments", topics[1].Topic)
	assert.Equal(t, int64(5), topics[1].MessageCount)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListTopicsEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"topic", "count"})
	mock.ExpectQuery("SELECT topic, COUNT\\(\\*\\) FROM queue_messages GROUP BY topic ORDER BY topic").
		WillReturnRows(rows)

	topics, err := store.ListTopics(context.Background())
	require.NoError(t, err)
	assert.Empty(t, topics)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGetTopicStats(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	// Total messages
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM queue_messages WHERE topic = \\?").
		WithArgs("orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(100))

	// DLQ count
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM queue_messages WHERE topic = \\?").
		WithArgs("orders_dlq").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	// Distinct partitions
	mock.ExpectQuery("SELECT COUNT\\(DISTINCT partition_key\\) FROM queue_messages WHERE topic = \\?").
		WithArgs("orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(4))

	// Consumer groups
	mock.ExpectQuery("SELECT COUNT\\(DISTINCT consumer_group\\) FROM queue_offsets WHERE topic = \\?").
		WithArgs("orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	stats, err := store.GetTopicStats(context.Background(), "orders", "_dlq")
	require.NoError(t, err)
	assert.Equal(t, "orders", stats.Topic)
	assert.Equal(t, int64(100), stats.TotalMessages)
	assert.Equal(t, int64(3), stats.DLQCount)
	assert.Equal(t, int64(4), stats.PartitionCount)
	assert.Equal(t, int64(2), stats.ConsumerGroupCount)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListMessages(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"offset", "id", "topic", "partition_key", "created_at", "published_at"}).
		AddRow(1, "msg-1", "orders", "repo-1", 1000, 1000).
		AddRow(2, "msg-2", "orders", "repo-1", 2000, 2000)
	mock.ExpectQuery("SELECT .+ FROM queue_messages WHERE topic = \\? ORDER BY `offset` LIMIT \\?").
		WithArgs("orders", 50).
		WillReturnRows(rows)

	messages, err := store.ListMessages(context.Background(), "orders", "", 50)
	require.NoError(t, err)
	assert.Len(t, messages, 2)
	assert.Equal(t, "msg-1", messages[0].ID)
	assert.Equal(t, int64(1), messages[0].Offset)
	assert.Equal(t, "msg-2", messages[1].ID)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListMessagesWithPartition(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"offset", "id", "topic", "partition_key", "created_at", "published_at"}).
		AddRow(1, "msg-1", "orders", "repo-1", 1000, 1000)
	mock.ExpectQuery("SELECT .+ FROM queue_messages WHERE topic = \\? AND partition_key = \\? ORDER BY `offset` LIMIT \\?").
		WithArgs("orders", "repo-1", 10).
		WillReturnRows(rows)

	messages, err := store.ListMessages(context.Background(), "orders", "repo-1", 10)
	require.NoError(t, err)
	assert.Len(t, messages, 1)
	assert.Equal(t, "repo-1", messages[0].PartitionKey)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInspectMessage(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"offset", "id", "topic", "partition_key", "created_at", "published_at", "payload", "metadata", "failed_at", "failure_count", "last_error", "original_topic"}).
		AddRow(1, "msg-1", "orders", "repo-1", 1000, 1000, []byte("hello"), []byte(`{"key":"val"}`), 0, 0, "", "")
	mock.ExpectQuery("SELECT .+ FROM queue_messages WHERE topic = \\? AND id = \\?").
		WithArgs("orders", "msg-1").
		WillReturnRows(rows)

	detail, found, err := store.InspectMessage(context.Background(), "orders", "msg-1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "msg-1", detail.ID)
	assert.Equal(t, []byte("hello"), detail.Payload)
	assert.Equal(t, "val", detail.Metadata["key"])
	assert.Equal(t, int64(0), detail.FailedAt)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInspectMessageNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"offset", "id", "topic", "partition_key", "created_at", "published_at", "payload", "metadata", "failed_at", "failure_count", "last_error", "original_topic"})
	mock.ExpectQuery("SELECT .+ FROM queue_messages WHERE topic = \\? AND id = \\?").
		WithArgs("orders", "missing").
		WillReturnRows(rows)

	_, found, err := store.InspectMessage(context.Background(), "orders", "missing")
	require.NoError(t, err)
	assert.False(t, found)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDeleteMessage(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	mock.ExpectExec("DELETE FROM queue_messages WHERE topic = \\? AND id = \\?").
		WithArgs("orders", "msg-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	affected, err := store.DeleteMessage(context.Background(), "orders", "msg-1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPurgeTopic(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	mock.ExpectExec("DELETE FROM queue_messages WHERE topic = \\?").
		WithArgs("orders").
		WillReturnResult(sqlmock.NewResult(0, 42))

	affected, err := store.PurgeTopic(context.Background(), "orders")
	require.NoError(t, err)
	assert.Equal(t, int64(42), affected)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRequeueDLQ(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .+ FROM queue_messages WHERE topic = \\? AND id = \\?").
		WithArgs("orders_dlq", "msg-1").
		WillReturnRows(sqlmock.NewRows([]string{"payload", "metadata", "partition_key", "created_at", "published_at"}).
			AddRow([]byte("data"), []byte(`{}`), "repo-1", 1000, 1000))
	mock.ExpectExec("INSERT INTO queue_messages").
		WithArgs("orders", "repo-1", "msg-1", []byte("data"), []byte(`{}`), int64(1000), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM queue_messages WHERE topic = \\? AND id = \\?").
		WithArgs("orders_dlq", "msg-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err = store.RequeueDLQ(context.Background(), "orders", "msg-1", "_dlq")
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListOffsets(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"consumer_group", "topic", "partition_key", "offset_acked", "updated_at"}).
		AddRow("group-1", "orders", "repo-1", 100, 5000)
	mock.ExpectQuery("SELECT .+ FROM queue_offsets ORDER BY").
		WillReturnRows(rows)

	offsets, err := store.ListOffsets(context.Background(), "")
	require.NoError(t, err)
	assert.Len(t, offsets, 1)
	assert.Equal(t, "group-1", offsets[0].ConsumerGroup)
	assert.Equal(t, int64(100), offsets[0].OffsetAcked)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListOffsetsFiltered(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"consumer_group", "topic", "partition_key", "offset_acked", "updated_at"}).
		AddRow("group-1", "orders", "repo-1", 100, 5000)
	mock.ExpectQuery("SELECT .+ FROM queue_offsets WHERE consumer_group = \\?").
		WithArgs("group-1").
		WillReturnRows(rows)

	offsets, err := store.ListOffsets(context.Background(), "group-1")
	require.NoError(t, err)
	assert.Len(t, offsets, 1)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestResetOffset(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	mock.ExpectExec("UPDATE queue_offsets SET offset_acked = \\?, updated_at = \\?").
		WithArgs(int64(0), sqlmock.AnyArg(), "group-1", "orders", "repo-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	affected, err := store.ResetOffset(context.Background(), "group-1", "orders", "repo-1", 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestListLeases(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"consumer_group", "topic", "partition_key", "leased_by", "leased_at", "lease_renewed_at"}).
		AddRow("group-1", "orders", "repo-1", "worker-1", 1000, 2000)
	mock.ExpectQuery("SELECT .+ FROM queue_partition_leases ORDER BY").
		WillReturnRows(rows)

	leases, err := store.ListLeases(context.Background())
	require.NoError(t, err)
	assert.Len(t, leases, 1)
	assert.Equal(t, "worker-1", leases[0].LeasedBy)
	assert.Equal(t, int64(1000), leases[0].LeasedAt)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestReleaseLease(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	mock.ExpectExec("DELETE FROM queue_partition_leases WHERE consumer_group = \\? AND topic = \\? AND partition_key = \\?").
		WithArgs("group-1", "orders", "repo-1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	affected, err := store.ReleaseLease(context.Background(), "group-1", "orders", "repo-1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestFormatMillis(t *testing.T) {
	assert.Equal(t, "-", FormatMillis(0))
	// 2024-01-01T00:00:00Z in milliseconds
	assert.Equal(t, "2024-01-01T00:00:00Z", FormatMillis(1704067200000))
}

func TestConsumerLag(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"consumer_group", "topic", "partition_key", "offset_acked", "latest_offset"}).
		AddRow("group-1", "orders", "repo-1", 50, 100).
		AddRow("group-1", "orders", "repo-2", 75, 75)
	mock.ExpectQuery("SELECT .+ FROM queue_offsets .+ LEFT JOIN").
		WithArgs("orders", "orders").
		WillReturnRows(rows)

	lags, err := store.ConsumerLag(context.Background(), "orders")
	require.NoError(t, err)
	assert.Len(t, lags, 2)
	assert.Equal(t, int64(50), lags[0].Lag)
	assert.Equal(t, int64(100), lags[0].LatestOffset)
	assert.Equal(t, int64(50), lags[0].AckedOffset)
	assert.Equal(t, int64(0), lags[1].Lag)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestConsumerLagNoMessages(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	// Consumer has offset but no messages remain (all acked and deleted)
	rows := sqlmock.NewRows([]string{"consumer_group", "topic", "partition_key", "offset_acked", "latest_offset"}).
		AddRow("group-1", "orders", "repo-1", 100, 0)
	mock.ExpectQuery("SELECT .+ FROM queue_offsets .+ LEFT JOIN").
		WithArgs("orders", "orders").
		WillReturnRows(rows)

	lags, err := store.ConsumerLag(context.Background(), "orders")
	require.NoError(t, err)
	assert.Len(t, lags, 1)
	assert.Equal(t, int64(0), lags[0].Lag) // clamped to 0, not negative
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStaleLeases(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"consumer_group", "topic", "partition_key", "leased_by", "leased_at", "lease_renewed_at"}).
		AddRow("group-1", "orders", "repo-1", "worker-1", 1000, 2000)
	mock.ExpectQuery("SELECT .+ FROM queue_partition_leases WHERE lease_renewed_at < \\?").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(rows)

	leases, err := store.StaleLeases(context.Background(), 60000)
	require.NoError(t, err)
	assert.Len(t, leases, 1)
	assert.Equal(t, "worker-1", leases[0].LeasedBy)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStaleLeasesEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := NewAdminStore(db)

	rows := sqlmock.NewRows([]string{"consumer_group", "topic", "partition_key", "leased_by", "leased_at", "lease_renewed_at"})
	mock.ExpectQuery("SELECT .+ FROM queue_partition_leases WHERE lease_renewed_at < \\?").
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(rows)

	leases, err := store.StaleLeases(context.Background(), 60000)
	require.NoError(t, err)
	assert.Empty(t, leases)
	assert.NoError(t, mock.ExpectationsWereMet())
}
