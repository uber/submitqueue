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

package stovepipe

import (
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
	stovepipetest "github.com/uber/submitqueue/test/testutil/stovepipe"
)

// TestProcessAdmitsColdStartHead ingests a queue head and waits for the running
// process consumer to admit it with the cold-start full-build outcome.
func (s *StovepipeIntegrationSuite) TestProcessAdmitsColdStartHead() {
	t := s.T()
	const queue = "monorepo/process-admit"

	s.log.Logf("Ingesting cold-start head for queue=%s", queue)
	resp, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: queue})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Id)

	s.log.Logf("Ingest succeeded: id=%s; waiting for process admission and build handoff", resp.Id)
	stovepipetest.AssertColdStartAdmitted(t, s.db, s.queueDB, queue, resp.Id)
	s.log.Logf("Cold-start admission passed: id=%s queue=%s", resp.Id, queue)
}

// TestProcessSupersedesOlderHead seeds a stale head behind a newer
// latest_request_id pointer and verifies process supersedes it without spending
// a build slot.
func (s *StovepipeIntegrationSuite) TestProcessSupersedesOlderHead() {
	t := s.T()
	const (
		queue    = "monorepo/process-coalesce"
		olderID  = "request/monorepo/process-coalesce/1"
		newerID  = "request/monorepo/process-coalesce/2"
		olderURI = "git://monorepo/process-coalesce/old"
		newerURI = "git://monorepo/process-coalesce/HEAD"
	)

	s.log.Logf("Seeding stale process head: older_id=%s newer_id=%s queue=%s", olderID, newerID, queue)
	stovepipetest.SeedCoalesceScenario(t, s.db, s.queueDB, queue, olderID, newerID, olderURI, newerURI)
	s.log.Logf("Waiting for process consumer to supersede older_id=%s", olderID)
	stovepipetest.AwaitProcessAcked(t, s.queueDB, queue)

	older := stovepipetest.AwaitRequestState(t, s.db, olderID, string(entity.RequestStateSuperseded))
	require.Equal(t, queue, older.Queue)

	newer, err := stovepipetest.ReadRequest(s.ctx, s.db, newerID)
	require.NoError(t, err)
	require.Equal(t, string(entity.RequestStateAccepted), newer.State)

	qrow, err := stovepipetest.ReadQueue(s.ctx, s.db, queue)
	require.NoError(t, err)
	require.Equal(t, int32(0), qrow.InFlightCount)
	require.Equal(t, newerID, qrow.LatestRequestID)
	s.log.Logf("Coalescing passed: superseded=%s latest=%s slots=%d", olderID, newerID, qrow.InFlightCount)
}

// TestProcessRedeliveryIsIdempotent verifies a redelivery of an admitted request
// does not claim another slot or create another build message.
func (s *StovepipeIntegrationSuite) TestProcessRedeliveryIsIdempotent() {
	t := s.T()
	const queue = "monorepo/process-redelivery"

	s.log.Logf("Ingesting request for redelivery test: queue=%s", queue)
	resp, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: queue})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Id)

	stovepipetest.AssertColdStartAdmitted(t, s.db, s.queueDB, queue, resp.Id)
	priorOffset := stovepipetest.ProcessAckedOffset(t, s.queueDB, queue)

	s.log.Logf("Publishing redelivery: id=%s prior_offset=%d", resp.Id, priorOffset)
	stovepipetest.PublishProcessDelivery(t, s.queueDB, resp.Id, resp.Id+"/redelivery", queue)
	stovepipetest.AwaitProcessAckedAfter(t, s.queueDB, queue, priorOffset)

	request, err := stovepipetest.ReadRequest(s.ctx, s.db, resp.Id)
	require.NoError(t, err)
	require.Equal(t, string(entity.RequestStateProcessing), request.State)

	qrow, err := stovepipetest.ReadQueue(s.ctx, s.db, queue)
	require.NoError(t, err)
	require.Equal(t, int32(1), qrow.InFlightCount)
	require.Equal(t, 1, stovepipetest.BuildMessageCount(t, s.queueDB, resp.Id))
	s.log.Logf("Redelivery idempotency passed: id=%s slots=%d build_messages=1", resp.Id, qrow.InFlightCount)
}

// TestProcessNonRetryableFailureIsReconciledThroughDLQ verifies the real
// classifier, queue DLQ routing, and reconciliation consumer work together.
func (s *StovepipeIntegrationSuite) TestProcessNonRetryableFailureIsReconciledThroughDLQ() {
	t := s.T()
	const (
		queue = "monorepo/process-dlq"
		id    = "request/monorepo/process-dlq/1"
	)

	s.log.Logf("Seeding non-retryable process failure: id=%s queue=%s", id, queue)
	_, err := s.db.ExecContext(s.ctx, `
		INSERT INTO queue (name, last_green_uri, in_flight_count, latest_request_id, version)
		VALUES (?, '', 0, ?, 1)`,
		queue, "request/another-queue/2",
	)
	require.NoError(t, err)

	_, err = s.db.ExecContext(s.ctx, `
		INSERT INTO request (id, queue, uri, state, build_strategy, base_uri, version)
		VALUES (?, ?, ?, ?, '', '', 1)`,
		id, queue, "git://monorepo/process-dlq/HEAD", string(entity.RequestStateAccepted),
	)
	require.NoError(t, err)

	stovepipetest.PublishProcessMessage(t, s.queueDB, id, queue)

	s.log.Logf("Waiting for DLQ reconciliation: id=%s", id)
	request := stovepipetest.AwaitRequestState(t, s.db, id, string(entity.RequestStateRecordedNotGreen))
	require.Equal(t, queue, request.Queue)
	stovepipetest.AwaitProcessDLQAcked(t, s.queueDB, queue)

	qrow, err := stovepipetest.ReadQueue(s.ctx, s.db, queue)
	require.NoError(t, err)
	require.Zero(t, qrow.InFlightCount)
	require.Zero(t, stovepipetest.BuildMessageCount(t, s.queueDB, id))
	s.log.Logf("DLQ reconciliation passed: id=%s state=%s slots=%d", id, request.State, qrow.InFlightCount)
}
