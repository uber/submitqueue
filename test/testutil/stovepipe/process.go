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

// Package stovepipe provides shared helpers for Stovepipe integration and e2e
// tests that observe process-stage outcomes through the storage and queue DBs.
package stovepipe

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	stovepipemq "github.com/uber/submitqueue/stovepipe/core/messagequeue"
	"github.com/uber/submitqueue/stovepipe/entity"
)

const (
	// Topic and consumer group names match service/stovepipe/server/main.go.
	ProcessTopic            = "process"
	ProcessConsumerGroup    = "stovepipe-process"
	BuildTopic              = "build"
	ProcessDLQTopic         = "process_dlq"
	ProcessDLQConsumerGroup = "stovepipe-process-dlq"

	defaultProcessTimeout      = 30 * time.Second
	defaultProcessPollInterval = 500 * time.Millisecond
)

// RequestRow is a snapshot of the mutable process-relevant request fields.
type RequestRow struct {
	ID            string
	Queue         string
	URI           string
	State         string
	BuildStrategy string
	BaseURI       string
}

// QueueRow is a snapshot of per-queue coordination state process reads and writes.
type QueueRow struct {
	Name            string
	LastGreenURI    string
	InFlightCount   int32
	LatestRequestID string
}

// ReadRequest loads the request row for id from the storage database.
func ReadRequest(ctx context.Context, db *sql.DB, id string) (RequestRow, error) {
	var row RequestRow
	err := db.QueryRowContext(ctx, `
		SELECT id, queue, uri, state, build_strategy, base_uri
		FROM request
		WHERE id = ?`, id).Scan(
		&row.ID, &row.Queue, &row.URI, &row.State, &row.BuildStrategy, &row.BaseURI,
	)
	return row, err
}

// ReadQueue loads the queue coordination row for name from the storage database.
func ReadQueue(ctx context.Context, db *sql.DB, name string) (QueueRow, error) {
	var row QueueRow
	err := db.QueryRowContext(ctx, `
		SELECT name, last_green_uri, in_flight_count, latest_request_id
		FROM queue
		WHERE name = ?`, name).Scan(
		&row.Name, &row.LastGreenURI, &row.InFlightCount, &row.LatestRequestID,
	)
	return row, err
}

// AwaitRequestState polls until the request reaches wantState or the timeout elapses.
func AwaitRequestState(t testing.TB, db *sql.DB, id, wantState string) RequestRow {
	t.Helper()
	ctx := context.Background()
	var got RequestRow
	require.Eventually(t, func() bool {
		row, err := ReadRequest(ctx, db, id)
		if err != nil {
			return false
		}
		got = row
		return row.State == wantState
	}, defaultProcessTimeout, defaultProcessPollInterval,
		"request %s should reach state %q", id, wantState)
	return got
}

// AwaitProcessAcked polls the queue consumer group's acked offset for the queue
// partition until it advances past zero.
func AwaitProcessAcked(t testing.TB, queueDB *sql.DB, queue string) {
	t.Helper()
	awaitAckedAfter(t, queueDB, ProcessConsumerGroup, ProcessTopic, queue, 0)
}

// ProcessAckedOffset returns the process consumer's current acknowledged offset.
func ProcessAckedOffset(t testing.TB, queueDB *sql.DB, queue string) int64 {
	t.Helper()
	var offset int64
	err := queueDB.QueryRow(`
		SELECT offset_acked
		FROM queue_offsets
		WHERE consumer_group = ? AND topic = ? AND partition_key = ?`,
		ProcessConsumerGroup, ProcessTopic, queue,
	).Scan(&offset)
	if err == sql.ErrNoRows {
		return 0
	}
	require.NoError(t, err)
	return offset
}

// AwaitProcessAckedAfter waits for a process delivery newer than priorOffset.
func AwaitProcessAckedAfter(t testing.TB, queueDB *sql.DB, queue string, priorOffset int64) {
	t.Helper()
	awaitAckedAfter(t, queueDB, ProcessConsumerGroup, ProcessTopic, queue, priorOffset)
}

// AwaitProcessDLQAcked waits for the DLQ reconciler to acknowledge a delivery.
func AwaitProcessDLQAcked(t testing.TB, queueDB *sql.DB, queue string) {
	t.Helper()
	awaitAckedAfter(t, queueDB, ProcessDLQConsumerGroup, ProcessDLQTopic, queue, 0)
}

func awaitAckedAfter(
	t testing.TB,
	queueDB *sql.DB,
	consumerGroup, topic, queue string,
	priorOffset int64,
) {
	t.Helper()
	const query = `
		SELECT offset_acked
		FROM queue_offsets
		WHERE consumer_group = ? AND topic = ? AND partition_key = ?`
	require.Eventually(t, func() bool {
		var ackedOffset int64
		err := queueDB.QueryRow(query, consumerGroup, topic, queue).Scan(&ackedOffset)
		return err == nil && ackedOffset > priorOffset
	}, defaultProcessTimeout, defaultProcessPollInterval,
		"consumer group %s should ack a newer message on topic %s queue %s",
		consumerGroup, topic, queue)
}

// AwaitBuildRequest waits for and decodes the build-stage message for id.
func AwaitBuildRequest(t testing.TB, queueDB *sql.DB, id string) *stovepipemq.BuildRequest {
	t.Helper()
	var payload []byte
	require.Eventually(t, func() bool {
		return queueDB.QueryRow(`
			SELECT payload
			FROM queue_messages
			WHERE topic = ? AND partition_key = ? AND id = ?`,
			BuildTopic, id, id,
		).Scan(&payload) == nil
	}, defaultProcessTimeout, defaultProcessPollInterval,
		"build request %s should be published", id)

	msg := &stovepipemq.BuildRequest{}
	require.NoError(t, stovepipemq.Unmarshal(payload, msg))
	require.Equal(t, id, msg.Id)
	return msg
}

// BuildMessageCount returns the number of build-stage messages for id.
func BuildMessageCount(t testing.TB, queueDB *sql.DB, id string) int {
	t.Helper()
	var count int
	require.NoError(t, queueDB.QueryRow(`
		SELECT COUNT(*)
		FROM queue_messages
		WHERE topic = ? AND partition_key = ? AND id = ?`,
		BuildTopic, id, id,
	).Scan(&count))
	return count
}

// AssertColdStartAdmitted waits for process to admit id, then checks the cold-start
// full-build outcome: processing state, full strategy, empty baseline, and one slot claimed.
func AssertColdStartAdmitted(t testing.TB, storageDB, queueDB *sql.DB, queue, id string) RequestRow {
	t.Helper()
	AwaitProcessAcked(t, queueDB, queue)
	row := AwaitRequestState(t, storageDB, id, string(entity.RequestStateProcessing))
	require.Equal(t, string(entity.BuildStrategyFull), row.BuildStrategy, "cold start should use full build")
	require.Empty(t, row.BaseURI, "cold start should have no baseline URI")

	ctx := context.Background()
	qrow, err := ReadQueue(ctx, storageDB, queue)
	require.NoError(t, err)
	require.Equal(t, int32(1), qrow.InFlightCount, "admit should claim one build slot")
	require.Equal(t, id, qrow.LatestRequestID, "latest pointer should still reference the admitted head")
	AwaitBuildRequest(t, queueDB, id)
	return row
}

// PublishProcessMessage inserts a process-stage delivery for id on queue into the
// queue database. Tests use this to drive scenarios ingest cannot produce alone
// (for example coalescing an older head behind a newer latest_request_id pointer).
func PublishProcessMessage(t testing.TB, queueDB *sql.DB, id, queue string) {
	t.Helper()
	PublishProcessDelivery(t, queueDB, id, id, queue)
}

// PublishProcessDelivery inserts a process-stage delivery whose queue message
// identity may differ from the referenced request identity.
func PublishProcessDelivery(t testing.TB, queueDB *sql.DB, requestID, messageID, queue string) {
	t.Helper()
	payload, err := stovepipemq.Marshal(&stovepipemq.ProcessRequest{Id: requestID})
	require.NoError(t, err)

	now := time.Now().UnixMilli()
	_, err = queueDB.Exec(`
		INSERT INTO queue_messages (
			topic, partition_key, id, payload, metadata,
			created_at, published_at, visible_after,
			failed_at, failure_count, last_error, original_topic
		) VALUES (?, ?, ?, ?, NULL, ?, ?, 0, 0, 0, '', '')`,
		ProcessTopic, queue, messageID, payload, now, now,
	)
	require.NoError(t, err)
}

// SeedCoalesceScenario prepares an older and newer accepted head on the same queue
// with latest_request_id pointing at the newer id, then publishes a process message
// for the older head so the running consumer should supersede it on sight.
func SeedCoalesceScenario(
	t testing.TB,
	storageDB, queueDB *sql.DB,
	queue, olderID, newerID, olderURI, newerURI string,
) {
	t.Helper()
	ctx := context.Background()

	_, err := storageDB.ExecContext(ctx, `
		INSERT INTO queue (name, last_green_uri, in_flight_count, latest_request_id, version)
		VALUES (?, '', 0, ?, 1)
		ON DUPLICATE KEY UPDATE latest_request_id = VALUES(latest_request_id), version = version + 1`,
		queue, newerID,
	)
	require.NoError(t, err)

	for _, spec := range []struct {
		id, uri string
	}{
		{olderID, olderURI},
		{newerID, newerURI},
	} {
		_, err := storageDB.ExecContext(ctx, `
			INSERT INTO request (id, queue, uri, state, build_strategy, base_uri, version)
			VALUES (?, ?, ?, ?, '', '', 1)`,
			spec.id, queue, spec.uri, string(entity.RequestStateAccepted),
		)
		require.NoError(t, err)
	}

	PublishProcessMessage(t, queueDB, olderID, queue)
}
