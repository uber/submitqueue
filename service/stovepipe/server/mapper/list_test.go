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

package mapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
)

func TestProtoToListRequest(t *testing.T) {
	for _, tt := range []struct {
		name string
		req  *pb.ListRequest
		want entity.ListRequest
	}{
		{name: "nil request"},
		{name: "omitted bounds", req: &pb.ListRequest{Queue: "queue"}, want: entity.ListRequest{Queue: "queue"}},
		{
			name: "explicit zero lower",
			req:  &pb.ListRequest{AcceptedLowerBound: &pb.ListRequest_AcceptedAtOrAfterMs{}},
			want: entity.ListRequest{HasAcceptedAtOrAfterMs: true},
		},
		{
			name: "explicit zero upper",
			req:  &pb.ListRequest{AcceptedUpperBound: &pb.ListRequest_AcceptedBeforeMs{}},
			want: entity.ListRequest{HasAcceptedBeforeMs: true},
		},
		{
			name: "explicit window and pagination",
			req: &pb.ListRequest{
				Queue: "queue", PageSize: 25, PageToken: "token",
				AcceptedLowerBound: &pb.ListRequest_AcceptedAtOrAfterMs{AcceptedAtOrAfterMs: 100},
				AcceptedUpperBound: &pb.ListRequest_AcceptedBeforeMs{AcceptedBeforeMs: 1000},
			},
			want: entity.ListRequest{
				Queue: "queue", PageSize: 25, PageToken: "token",
				AcceptedAtOrAfterMs: 100, HasAcceptedAtOrAfterMs: true,
				AcceptedBeforeMs: 1000, HasAcceptedBeforeMs: true,
			},
		},
		{
			name: "continuation with omitted bounds",
			req:  &pb.ListRequest{Queue: "queue", PageSize: 10, PageToken: "token"},
			want: entity.ListRequest{Queue: "queue", PageSize: 10, PageToken: "token"},
		},
		{
			name: "invalid values reach controller unchanged",
			req: &pb.ListRequest{
				PageSize: -1, AcceptedLowerBound: &pb.ListRequest_AcceptedAtOrAfterMs{AcceptedAtOrAfterMs: -100},
			},
			want: entity.ListRequest{PageSize: -1, AcceptedAtOrAfterMs: -100, HasAcceptedAtOrAfterMs: true},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ProtoToListRequest(tt.req))
		})
	}
}

func TestRequestSummaryToProto(t *testing.T) {
	for _, tt := range []struct {
		name   string
		state  entity.RequestState
		reason entity.RequestOutcomeReason
	}{
		{name: "known vocabulary", state: entity.RequestStateFailed, reason: entity.RequestOutcomeReasonBuildFailed},
		{name: "unknown vocabulary"},
		{name: "future vocabulary", state: "future_state", reason: "future_reason"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response := RequestSummaryToProto(entity.RequestSummary{
				RequestID: "7", Queue: "queue", URI: "git://repo/change", BaseURI: "git://repo/base",
				State: tt.state, StateTimestampMs: 2000, AcceptedAtMs: 1000, OutcomeReason: tt.reason,
			})
			assert.Equal(t, &pb.RequestSummary{
				RequestId: "7", Queue: "queue", ChangeUri: "git://repo/change", BaseUri: "git://repo/base",
				RequestState: string(tt.state), StateUpdatedAtMs: 2000, AcceptedAtMs: 1000, OutcomeReason: string(tt.reason),
			}, response)
		})
	}
}

func TestListResultToProto(t *testing.T) {
	for _, tt := range []struct {
		name   string
		result entity.ListResult
		ids    []string
	}{
		{name: "empty page", ids: []string{}},
		{
			name: "preserves order and continuation",
			result: entity.ListResult{
				Requests: []entity.RequestSummary{{RequestID: "9"}, {RequestID: "10"}}, NextPageToken: "next-token",
			},
			ids: []string{"9", "10"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			response := ListResultToProto(tt.result)
			require.NotNil(t, response)
			ids := make([]string, 0, len(response.GetRequests()))
			for _, summary := range response.GetRequests() {
				ids = append(ids, summary.GetRequestId())
			}
			assert.Equal(t, tt.ids, ids)
			assert.Equal(t, tt.result.NextPageToken, response.GetNextPageToken())
		})
	}
}
