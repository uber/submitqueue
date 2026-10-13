// Copyright (c) 2026 Uber Technologies, Inc.
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
)

func (s *StovepipeE2ESuite) TestIngest_HappyPath_Processes() {
	const queue = "monorepo/main"
	id := s.ingest(queue)
	s.assertIngestPersisted(queue, id)
	s.awaitProcessed(queue)
}

func (s *StovepipeE2ESuite) TestIngest_Idempotent() {
	const queue = "monorepo/release"
	id := s.ingest(queue)
	first := s.awaitListedRequest(queue, id, "")
	require.Positive(s.T(), first.AcceptedAtMs)

	id2 := s.ingest(queue)
	assert.Equal(s.T(), id, id2, "re-ingest of the same head should dedup to the same id")
	second := s.awaitListedRequest(queue, id2, "")
	assert.Equal(s.T(), first.AcceptedAtMs, second.AcceptedAtMs)
}

func (s *StovepipeE2ESuite) TestIngest_IndependentQueuesReuseIDs() {
	queues := []string{"monorepo/main", "monorepo/release"}
	ids := []string{s.ingest(queues[0]), s.ingest(queues[1])}
	require.Equal(s.T(), ids[0], ids[1], "independent queues may share a resource ID")

	for i, queue := range queues {
		s.Run(queue, func() {
			s.assertIngestPersisted(queue, ids[i])
			s.awaitBuildStatus(queue, ids[i], "succeeded")
			s.awaitRequestState(queue, ids[i], "succeeded")
			summary := s.awaitListedRequest(queue, ids[i], "succeeded")
			assert.Equal(s.T(), "git://"+queue+"/HEAD", summary.ChangeUri)
		})
	}
}

func (s *StovepipeE2ESuite) TestIngest_RejectsUnconfiguredTenant() {
	resp, err := s.client.Ingest(s.ctx, &pb.IngestRequest{Queue: "monorepo/unconfigured"})
	require.Error(s.T(), err)
	assert.Nil(s.T(), resp)
}

func (s *StovepipeE2ESuite) TestIngest_SlowBuild_PollsToCompletion() {
	const queue = "monorepo/slow?buildrunner-fake=build-slow"
	id := s.ingest(queue)
	s.assertIngestPersisted(queue, id)
	first := s.awaitListedRequest(queue, id, "")
	require.Positive(s.T(), first.AcceptedAtMs)

	s.awaitProcessed(queue)
	s.awaitBuildStatus(queue, id, "succeeded")
	s.awaitRequestState(queue, id, "succeeded")
	summary := s.awaitListedRequest(queue, id, "succeeded")
	assert.Equal(s.T(), first.AcceptedAtMs, summary.AcceptedAtMs)
	assert.GreaterOrEqual(s.T(), summary.StateUpdatedAtMs, summary.AcceptedAtMs)
	assert.Equal(s.T(), "build_succeeded", summary.OutcomeReason)
	history, err := s.client.GetRequestHistoryByID(s.ctx, &pb.GetRequestHistoryByIDRequest{Queue: queue, RequestId: id})
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), history.Events)
	assert.Equal(s.T(), "accepted", history.Events[0].GetRequestState())
	assert.Equal(s.T(), history.Events[0].TimestampMs, summary.AcceptedAtMs)
	assert.Equal(s.T(), int32(0), s.inFlightCount(queue),
		"a terminal build should release the queue's build slot")
}
