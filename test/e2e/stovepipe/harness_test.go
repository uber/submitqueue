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

package e2e_test

// Reusable e2e helpers so tests read as intent, not plumbing. They drive the
// stack through the real Stovepipe gRPC surface (Ingest) and observe outcomes
// two ways:
//
//   - the synchronous side effects of Ingest via raw SQL on the storage DB
//     (the request row and its (queue, URI) mapping) and the queue DB (the
//     published process message); and
//   - the asynchronous completion of the process stage by polling the queue
//     backend's per-consumer-group delivery state until the message is acked.
//
// The process consumer runs inside the stovepipe-service container, so there is
// no in-process signal to await. Polling continues until the condition holds or
// Bazel's test timeout terminates a genuinely stuck suite.

import (
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
)

func pollUntil(interval time.Duration, condition func() bool) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		<-ticker.C
	}
}

// The process consumer's topic and consumer group as wired in
// service/stovepipe/server/main.go (topic name "process", consumer group
// "stovepipe-process"). awaitProcessed reads the queue backend's delivery state
// keyed by this group.
const (
	processTopic         = "process"
	processConsumerGroup = "stovepipe-process"
)

// ingest admits a queue's head commit into the pipeline and returns the minted
// request id.
func (s *StovepipeE2ESuite) ingest(queue string) string {
	t := s.T()
	resp, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: queue})
	require.NoError(t, err, "Ingest failed for queue %s", queue)
	require.NotEmpty(t, resp.Id, "Ingest returned an empty id for queue %s", queue)
	return resp.Id
}

// requestRowCount returns the number of request rows with the given id (0 or 1).
func (s *StovepipeE2ESuite) requestRowCount(id string) int {
	t := s.T()
	var count int
	require.NoError(t, s.db.QueryRow("SELECT COUNT(*) FROM request WHERE id = ?", id).Scan(&count),
		"failed to count request rows for %s", id)
	return count
}

// uriMapping returns the request id the (queue, URI) mapping points at.
func (s *StovepipeE2ESuite) uriMapping(queue string) string {
	t := s.T()
	var mappedID string
	require.NoError(t, s.db.QueryRow("SELECT request_id FROM request_uri WHERE queue = ?", queue).Scan(&mappedID),
		"failed to read request_uri mapping for queue %s", queue)
	return mappedID
}

// publishedMessageCount returns the number of process messages published for the
// given request id (0 or 1).
func (s *StovepipeE2ESuite) publishedMessageCount(id string) int {
	t := s.T()
	var count int
	require.NoError(t, s.queueDB.QueryRow("SELECT COUNT(*) FROM queue_messages WHERE id = ?", id).Scan(&count),
		"failed to count queue messages for %s", id)
	return count
}

// awaitProcessed blocks until the process consumer has acked a message on the
// given queue's partition, proving the ingest→process pipeline ran end-to-end.
// The durable signal is the consumer group's acked-offset watermark in
// queue_offsets: it starts at 0 and only advances once a message is acked (see
// offset_store.go). We poll that rather than the message's own delivery-state
// row because the queue GCs acked messages (and their delivery state) from
// queue_messages once the watermark passes them. The queue uses the queue name
// as the message's partition key (see the ingest controller), so the partition
// key here is the queue.
func (s *StovepipeE2ESuite) awaitProcessed(queue string) {
	const query = `
			SELECT offset_acked
			FROM queue_offsets
			WHERE consumer_group = ? AND topic = ? AND partition_key = ?`
	pollUntil(processPollInterval, func() bool {
		var ackedOffset int64
		err := s.queueDB.QueryRow(query, processConsumerGroup, processTopic, queue).Scan(&ackedOffset)
		if err != nil {
			// sql.ErrNoRows means the partition offset is not initialized yet.
			s.log.Logf("acked offset for queue %s not ready yet: %v", queue, err)
			return false
		}
		s.log.Logf("acked offset for queue %s = %d (want > 0)", queue, ackedOffset)
		return ackedOffset > 0
	})
}

// assertIngestPersisted asserts the synchronous side effects of a successful
// Ingest: the request row, the (queue, URI) mapping pointing at the minted id,
// and exactly one published process message.
func (s *StovepipeE2ESuite) assertIngestPersisted(queue, id string) {
	t := s.T()
	assert.Equal(t, 1, s.requestRowCount(id), "request row should be persisted for %s", id)
	assert.Equal(t, id, s.uriMapping(queue), "URI mapping should point at the minted request id")
	assert.Equal(t, 1, s.publishedMessageCount(id), "should have published one process message for %s", id)
}
