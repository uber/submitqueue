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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
)

func (s *StovepipeE2ESuite) TestList_Pagination() {
	want := s.seedListPaginationSummaries(listPaginationQueue)
	first := s.listRequests(&pb.ListRequest{
		Queue: listPaginationQueue, PageSize: 2,
		AcceptedLowerBound: &pb.ListRequest_AcceptedAtOrAfterMs{AcceptedAtOrAfterMs: 1000},
		AcceptedUpperBound: &pb.ListRequest_AcceptedBeforeMs{AcceptedBeforeMs: 2000},
	})
	s.assertListSummaries(first, want[:2]...)
	require.NotEmpty(s.T(), first.NextPageToken)

	for _, tt := range []struct {
		name string
		req  *pb.ListRequest
	}{
		{"omitted continuation bounds", &pb.ListRequest{
			Queue: listPaginationQueue, PageSize: 3, PageToken: first.NextPageToken,
		}},
		{"matching continuation bounds", &pb.ListRequest{
			Queue: listPaginationQueue, PageSize: 3, PageToken: first.NextPageToken,
			AcceptedLowerBound: &pb.ListRequest_AcceptedAtOrAfterMs{AcceptedAtOrAfterMs: 1000},
			AcceptedUpperBound: &pb.ListRequest_AcceptedBeforeMs{AcceptedBeforeMs: 2000},
		}},
	} {
		s.Run(tt.name, func() {
			next := s.listRequests(tt.req)
			s.assertListSummaries(next, want[2:]...)
			assert.Empty(s.T(), next.NextPageToken)
		})
	}
}

func (s *StovepipeE2ESuite) TestList_TimeBounds() {
	old := s.seedListSummary(listTimeBoundsQueue, "list/old", 500)
	lower := s.seedListSummary(listTimeBoundsQueue, "list/lower", 1000)
	upper := s.seedListSummary(listTimeBoundsQueue, "list/upper", 2000)
	s.seedListSummary(listTimeBoundsQueue, "list/unknown", 0)
	futureMs := time.Now().AddDate(1, 0, 0).UnixMilli()
	future := s.seedListSummary(listTimeBoundsQueue, "list/future", futureMs)

	for _, tt := range []struct {
		name string
		req  *pb.ListRequest
		want []*pb.RequestSummary
	}{
		{"defaults include old requests and exclude unknown and future times",
			&pb.ListRequest{Queue: listTimeBoundsQueue}, []*pb.RequestSummary{upper, lower, old}},
		{"inclusive lower and exclusive upper",
			&pb.ListRequest{
				Queue:              listTimeBoundsQueue,
				AcceptedLowerBound: &pb.ListRequest_AcceptedAtOrAfterMs{AcceptedAtOrAfterMs: 1000},
				AcceptedUpperBound: &pb.ListRequest_AcceptedBeforeMs{AcceptedBeforeMs: 2000},
			}, []*pb.RequestSummary{lower}},
		{"explicit future window",
			&pb.ListRequest{
				Queue:              listTimeBoundsQueue,
				AcceptedLowerBound: &pb.ListRequest_AcceptedAtOrAfterMs{AcceptedAtOrAfterMs: futureMs},
				AcceptedUpperBound: &pb.ListRequest_AcceptedBeforeMs{AcceptedBeforeMs: futureMs + 1},
			}, []*pb.RequestSummary{future}},
		{"empty window",
			&pb.ListRequest{
				Queue:              listTimeBoundsQueue,
				AcceptedUpperBound: &pb.ListRequest_AcceptedBeforeMs{AcceptedBeforeMs: 500},
			}, nil},
	} {
		s.Run(tt.name, func() {
			response := s.listRequests(tt.req)
			s.assertListSummaries(response, tt.want...)
			assert.Empty(s.T(), response.NextPageToken)
		})
	}
}

func (s *StovepipeE2ESuite) TestList_QueueIsolation() {
	primary := s.seedListSummary(listPrimaryQueue, "list/shared", 1000)
	other := s.seedListSummary(listOtherQueue, "list/shared", 1000)
	otherOnly := s.seedListSummary(listOtherQueue, "list/other-only", 1500)

	for _, tt := range []struct {
		queue string
		want  []*pb.RequestSummary
	}{
		{listPrimaryQueue, []*pb.RequestSummary{primary}},
		{listOtherQueue, []*pb.RequestSummary{otherOnly, other}},
	} {
		s.Run(tt.queue, func() {
			response := s.listRequests(&pb.ListRequest{Queue: tt.queue})
			s.assertListSummaries(response, tt.want...)
			assert.Empty(s.T(), response.NextPageToken)
		})
	}
}

func (s *StovepipeE2ESuite) TestList_InvalidRequests() {
	s.seedListPaginationSummaries(listValidationQueue)
	first := s.listRequests(&pb.ListRequest{
		Queue: listValidationQueue, PageSize: 1,
		AcceptedLowerBound: &pb.ListRequest_AcceptedAtOrAfterMs{AcceptedAtOrAfterMs: 1000},
		AcceptedUpperBound: &pb.ListRequest_AcceptedBeforeMs{AcceptedBeforeMs: 2000},
	})
	require.NotEmpty(s.T(), first.NextPageToken)

	for _, tt := range []struct {
		name string
		req  *pb.ListRequest
	}{
		{"unconfigured queue", &pb.ListRequest{Queue: "monorepo/unconfigured"}},
		{"missing queue", &pb.ListRequest{}},
		{"negative page size", &pb.ListRequest{Queue: listValidationQueue, PageSize: -1}},
		{"oversized page", &pb.ListRequest{Queue: listValidationQueue, PageSize: 201}},
		{"malformed token", &pb.ListRequest{Queue: listValidationQueue, PageToken: "!"}},
		{"token for another queue", &pb.ListRequest{Queue: listOtherQueue, PageToken: first.NextPageToken}},
		{"changed continuation bounds", &pb.ListRequest{
			Queue: listValidationQueue, PageToken: first.NextPageToken,
			AcceptedUpperBound: &pb.ListRequest_AcceptedBeforeMs{AcceptedBeforeMs: 2001},
		}},
		{"explicit zero upper bound", &pb.ListRequest{
			Queue: listValidationQueue, AcceptedUpperBound: &pb.ListRequest_AcceptedBeforeMs{},
		}},
	} {
		s.Run(tt.name, func() {
			response, err := s.client.List(s.ctx, tt.req)
			require.Error(s.T(), err)
			assert.Nil(s.T(), response)
		})
	}
}
