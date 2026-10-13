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

import (
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
	"google.golang.org/protobuf/proto"
)

// Container boundaries require polling; Bazel owns the convergence deadline.
const convergencePollInterval = 500 * time.Millisecond

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

const (
	processTopic         = "process"
	processConsumerGroup = "stovepipe-process"
)

func (s *StovepipeE2ESuite) ingest(queue string) string {
	s.T().Helper()
	resp, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: queue})
	require.NoError(s.T(), err, "Ingest failed for queue %s", queue)
	require.NotEmpty(s.T(), resp.Id, "Ingest returned an empty id for queue %s", queue)
	return resp.Id
}

func (s *StovepipeE2ESuite) assertIngestPersisted(queue, id string) {
	s.T().Helper()
	var count int
	require.NoError(s.T(), s.db.QueryRowContext(s.ctx,
		"SELECT COUNT(*) FROM request WHERE queue = ? AND id = ?", queue, id).Scan(&count))
	assert.Equal(s.T(), 1, count)

	var mappedID string
	require.NoError(s.T(), s.db.QueryRowContext(s.ctx,
		"SELECT request_id FROM request_uri WHERE queue = ?", queue).Scan(&mappedID))
	assert.Equal(s.T(), id, mappedID)
}

func (s *StovepipeE2ESuite) awaitProcessed(queue string) {
	s.T().Helper()
	// Acked messages can be garbage-collected; their offset watermark remains durable.
	const query = `
		SELECT offset_acked FROM queue_offsets
		WHERE tenant = ? AND consumer_group = ? AND topic = ? AND partition_key = ?`
	pollUntil(convergencePollInterval, func() bool {
		var ackedOffset int64
		err := s.queueDB.QueryRowContext(s.ctx, query, queue, processConsumerGroup, processTopic, queue).Scan(&ackedOffset)
		if err != nil {
			s.log.Logf("acked offset for queue %s not ready yet: %v", queue, err)
			return false
		}
		s.log.Logf("acked offset for queue %s = %d (want > 0)", queue, ackedOffset)
		return ackedOffset > 0
	})
}

func (s *StovepipeE2ESuite) awaitRequestState(queue, id, want string) {
	s.T().Helper()
	pollUntil(convergencePollInterval, func() bool {
		var state string
		if err := s.db.QueryRowContext(s.ctx, "SELECT state FROM request WHERE queue = ? AND id = ?", queue, id).Scan(&state); err != nil {
			s.log.Logf("request queue=%s id=%s state not readable yet: %v", queue, id, err)
			return false
		}
		s.log.Logf("request queue=%s id=%s state = %q (want %q)", queue, id, state, want)
		return state == want
	})
}

func (s *StovepipeE2ESuite) inFlightCount(queue string) int32 {
	s.T().Helper()
	var count int32
	require.NoError(s.T(), s.db.QueryRowContext(s.ctx, "SELECT in_flight_count FROM queue WHERE name = ?", queue).Scan(&count))
	return count
}

func (s *StovepipeE2ESuite) awaitBuildStatus(queue, requestID, want string) {
	s.T().Helper()
	pollUntil(convergencePollInterval, func() bool {
		var status string
		err := s.db.QueryRowContext(s.ctx, "SELECT status FROM build WHERE queue = ? AND request_id = ?", queue, requestID).Scan(&status)
		if err != nil {
			s.log.Logf("build for queue=%s request=%s not readable yet: %v", queue, requestID, err)
			return false
		}
		s.log.Logf("build for queue=%s request=%s status = %q (want %q)", queue, requestID, status, want)
		return status == want
	})
}

func (s *StovepipeE2ESuite) listRequests(req *pb.ListRequest) *pb.ListResponse {
	s.T().Helper()
	response, err := s.client.List(s.ctx, req)
	require.NoError(s.T(), err, "List failed for queue %s", req.Queue)
	require.NotNil(s.T(), response)
	return response
}

func (s *StovepipeE2ESuite) awaitListedRequest(queue, requestID, state string) *pb.RequestSummary {
	s.T().Helper()
	var found *pb.RequestSummary
	pollUntil(convergencePollInterval, func() bool {
		response := s.listRequests(&pb.ListRequest{Queue: queue, PageSize: 200})
		require.Empty(s.T(), response.NextPageToken)
		found = nil
		matches := 0
		for _, summary := range response.Requests {
			require.Equal(s.T(), queue, summary.Queue)
			if summary.RequestId == requestID {
				found = summary
				matches++
			}
		}
		require.LessOrEqual(s.T(), matches, 1, "duplicate listing for %s", requestID)
		s.log.Logf("List queue=%s id=%s matches=%d state=%q (want %q)",
			queue, requestID, matches, found.GetRequestState(), state)
		return matches == 1 && (state == "" || found.RequestState == state)
	})
	return found
}

func (s *StovepipeE2ESuite) assertListSummaries(response *pb.ListResponse, want ...*pb.RequestSummary) {
	s.T().Helper()
	require.Len(s.T(), response.Requests, len(want))
	for i, summary := range response.Requests {
		assert.True(s.T(), proto.Equal(want[i], summary), "want %v, got %v", want[i], summary)
	}
}

// Fixed read-path fixtures guarantee timestamp ties without depending on clock resolution.
// Projection behavior is covered by the real-ingest scenarios, not these seeded records.
func (s *StovepipeE2ESuite) seedListSummary(queue, requestID string, acceptedAtMs int64) *pb.RequestSummary {
	s.T().Helper()
	summary := entity.RequestSummary{
		Queue: queue, RequestID: requestID, URI: "git://" + queue + "/" + requestID,
		BaseURI: "git://repo/base", State: entity.RequestStateSucceeded, StateTimestampMs: acceptedAtMs + 100,
		AcceptedAtMs: acceptedAtMs, OutcomeReason: entity.RequestOutcomeReasonBuildSucceeded,
		RequestVersion: 3, Version: 1,
	}
	stores, err := s.appStorage.For(queue)
	require.NoError(s.T(), err)
	require.NoError(s.T(), stores.GetRequestSummaryStore().Create(s.ctx, summary))
	if acceptedAtMs > 0 {
		require.NoError(s.T(), stores.GetRequestAcceptanceStore().Create(s.ctx, entity.RequestAcceptance{
			Queue: queue, AcceptedAtMs: acceptedAtMs, RequestID: requestID,
		}))
	}
	return &pb.RequestSummary{
		Queue: queue, RequestId: requestID, ChangeUri: summary.URI, BaseUri: summary.BaseURI,
		RequestState: string(summary.State), StateUpdatedAtMs: summary.StateTimestampMs,
		AcceptedAtMs: acceptedAtMs, OutcomeReason: string(summary.OutcomeReason),
	}
}

func (s *StovepipeE2ESuite) seedListPaginationSummaries(queue string) []*pb.RequestSummary {
	s.T().Helper()
	lower := s.seedListSummary(queue, "list/lower", 1000)
	ten := s.seedListSummary(queue, "list/10", 1500)
	z := s.seedListSummary(queue, "list/z", 1500)
	nine := s.seedListSummary(queue, "list/9", 1500)
	return []*pb.RequestSummary{z, nine, ten, lower}
}
