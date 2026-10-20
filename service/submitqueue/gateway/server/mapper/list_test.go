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

package mapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/entity"
	"google.golang.org/protobuf/proto"
)

func TestProtoToListRequest(t *testing.T) {
	for _, tt := range []struct {
		name string
		req  *pb.ListRequest
		want entity.ListRequest
	}{
		{name: "nil request"},
		{
			name: "explicit zero bounds keep presence",
			req: &pb.ListRequest{
				ReceivedLowerBound: &pb.ListRequest_ReceivedAtOrAfterMs{},
				ReceivedUpperBound: &pb.ListRequest_ReceivedBeforeMs{},
			},
			want: entity.ListRequest{HasReceivedAtOrAfterMs: true, HasReceivedBeforeMs: true},
		},
		{
			name: "explicit window and pagination",
			req: &pb.ListRequest{
				Queue: "q", PageSize: 25, PageToken: "token",
				ReceivedLowerBound: &pb.ListRequest_ReceivedAtOrAfterMs{ReceivedAtOrAfterMs: 100},
				ReceivedUpperBound: &pb.ListRequest_ReceivedBeforeMs{ReceivedBeforeMs: 200},
			},
			want: entity.ListRequest{
				Queue: "q", PageSize: 25, PageToken: "token",
				ReceivedAtOrAfterMs: 100, HasReceivedAtOrAfterMs: true,
				ReceivedBeforeMs: 200, HasReceivedBeforeMs: true,
			},
		},
		{
			name: "continuation with omitted bounds",
			req:  &pb.ListRequest{Queue: "q", PageToken: "token"},
			want: entity.ListRequest{Queue: "q", PageToken: "token"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ProtoToListRequest(tt.req))
		})
	}
}

func TestListRequestBoundPresenceSurvivesWire(t *testing.T) {
	for _, tt := range []struct {
		name string
		req  *pb.ListRequest
		want entity.ListRequest
	}{
		{name: "omitted bounds", req: &pb.ListRequest{Queue: "q"}, want: entity.ListRequest{Queue: "q"}},
		{
			name: "explicit zero bounds",
			req: &pb.ListRequest{
				Queue:              "q",
				ReceivedLowerBound: &pb.ListRequest_ReceivedAtOrAfterMs{},
				ReceivedUpperBound: &pb.ListRequest_ReceivedBeforeMs{},
			},
			want: entity.ListRequest{Queue: "q", HasReceivedAtOrAfterMs: true, HasReceivedBeforeMs: true},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wire, err := proto.Marshal(tt.req)
			require.NoError(t, err)
			var decoded pb.ListRequest
			require.NoError(t, proto.Unmarshal(wire, &decoded))
			assert.Equal(t, tt.want, ProtoToListRequest(&decoded))
		})
	}
}

func TestListResultToProto(t *testing.T) {
	result := entity.ListResult{
		Requests: []entity.RequestSummary{{
			RequestID:    "1",
			Queue:        "q",
			ChangeURIs:   []string{"github://uber/repo/pull/1/abc"},
			ReceivedAtMs: 100,
			Status:       entity.RequestStatusError,
			LastError:    "build failed",
			Metadata:     map[string]string{"build": "url"},
		}},
		NextPageToken:       "next",
		ReceivedAtOrAfterMs: 50,
		ReceivedBeforeMs:    250,
	}

	response := ListResultToProto(result)

	assert.Equal(t, &pb.ListResponse{
		Requests: []*pb.RequestSummary{{
			Sqid: "1", Queue: "q", ChangeUris: []string{"github://uber/repo/pull/1/abc"},
			ReceivedAtMs: 100, Status: string(entity.RequestStatusError), LastError: "build failed",
			Metadata: map[string]string{"build": "url"},
		}},
		NextPageToken:       "next",
		ReceivedAtOrAfterMs: 50,
		ReceivedBeforeMs:    250,
	}, response)
}
