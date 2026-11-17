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

	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
)

func TestProtoToListIssueHistoryRequest(t *testing.T) {
	for _, tt := range []struct {
		name string
		req  *pb.ListIssueHistoryRequest
		want entity.ListIssueHistoryRequest
	}{
		{name: "nil request"},
		{
			name: "omitted bounds",
			req:  &pb.ListIssueHistoryRequest{Queue: "queue", TaskId: "task"},
			want: entity.ListIssueHistoryRequest{Queue: "queue", TaskID: "task"},
		},
		{
			name: "explicit zero lower bound",
			req:  &pb.ListIssueHistoryRequest{AcceptedLowerBound: &pb.ListIssueHistoryRequest_AcceptedAtOrAfterMs{}},
			want: entity.ListIssueHistoryRequest{HasAcceptedAtOrAfterMs: true},
		},
		{
			name: "explicit zero upper bound",
			req:  &pb.ListIssueHistoryRequest{AcceptedUpperBound: &pb.ListIssueHistoryRequest_AcceptedBeforeMs{}},
			want: entity.ListIssueHistoryRequest{HasAcceptedBeforeMs: true},
		},
		{
			name: "explicit window and pagination",
			req: &pb.ListIssueHistoryRequest{
				Queue:              "queue",
				TaskId:             "task",
				UnitId:             "unit",
				PageSize:           2,
				PageToken:          "token",
				AcceptedLowerBound: &pb.ListIssueHistoryRequest_AcceptedAtOrAfterMs{AcceptedAtOrAfterMs: 100},
				AcceptedUpperBound: &pb.ListIssueHistoryRequest_AcceptedBeforeMs{AcceptedBeforeMs: 200},
			},
			want: entity.ListIssueHistoryRequest{
				Queue: "queue", TaskID: "task", UnitID: "unit", PageSize: 2, PageToken: "token",
				AcceptedAtOrAfterMs: 100, HasAcceptedAtOrAfterMs: true, AcceptedBeforeMs: 200,
				HasAcceptedBeforeMs: true,
			},
		},
		{
			name: "invalid values reach controller unchanged",
			req: &pb.ListIssueHistoryRequest{
				PageSize:           -1,
				AcceptedLowerBound: &pb.ListIssueHistoryRequest_AcceptedAtOrAfterMs{AcceptedAtOrAfterMs: -100},
			},
			want: entity.ListIssueHistoryRequest{PageSize: -1, AcceptedAtOrAfterMs: -100, HasAcceptedAtOrAfterMs: true},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ProtoToListIssueHistoryRequest(tt.req))
		})
	}
}

func TestListIssueHistoryResultToProto(t *testing.T) {
	for _, tt := range []struct {
		name   string
		result entity.ListIssueHistoryResult
		want   *pb.ListIssueHistoryResponse
	}{
		{
			name: "empty page",
			want: &pb.ListIssueHistoryResponse{Events: []*pb.IssueHistoryEvent{}},
		},
		{
			name: "original events and request window",
			result: entity.ListIssueHistoryResult{
				Events: []entity.IssueHistoryEntry{
					{
						Log:       entity.IssueLog{TaskID: "test", UnitID: "unit", Event: entity.IssueEventReported, BuildID: "build/2", TimestampMs: 9007199254740993, FirstSeenBuildID: "build/1"},
						RequestID: "12",
					},
					{Log: entity.IssueLog{Event: entity.IssueEventClosed, BuildID: "build/1", TimestampMs: 100}},
				},
				NextPageToken:       "next",
				AcceptedAtOrAfterMs: 100,
				AcceptedBeforeMs:    200,
			},
			want: &pb.ListIssueHistoryResponse{
				Events: []*pb.IssueHistoryEvent{
					{TaskId: "test", UnitId: "unit", Event: "reported", BuildId: "build/2", TimestampMs: 9007199254740993, FirstSeenBuildId: "build/1", RequestId: "12"},
					{Event: "closed", BuildId: "build/1", TimestampMs: 100},
				},
				NextPageToken:       "next",
				AcceptedAtOrAfterMs: 100,
				AcceptedBeforeMs:    200,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ListIssueHistoryResultToProto(tt.result))
		})
	}
}
