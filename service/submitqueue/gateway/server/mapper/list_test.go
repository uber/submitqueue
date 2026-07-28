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
	pb "github.com/uber/submitqueue/api/submitqueue/gateway/protopb"
	"github.com/uber/submitqueue/submitqueue/entity"
)

func TestProtoToListRequest(t *testing.T) {
	req := &pb.ListRequest{
		Queue:               "q",
		ReceivedAtOrAfterMs: 100,
		ReceivedBeforeMs:    200,
		PageSize:            25,
		PageToken:           "token",
	}

	assert.Equal(t, entity.ListRequest{
		Queue:               "q",
		ReceivedAtOrAfterMs: 100,
		ReceivedBeforeMs:    200,
		PageSize:            25,
		PageToken:           "token",
	}, ProtoToListRequest(req))
}

func TestListResultToProto(t *testing.T) {
	result := entity.ListResult{
		Requests: []entity.RequestQueueSummary{{
			RequestID:    "q/1",
			Queue:        "q",
			ChangeURIs:   []string{"github://uber/repo/pull/1/abc"},
			ReceivedAtMs: 100,
			Status:       entity.RequestStatusAccepted,
			Metadata:     map[string]string{},
		}},
		NextPageToken: "next",
	}

	response := ListResultToProto(result)

	assert.Equal(t, "q/1", response.Requests[0].Sqid)
	assert.Equal(t, "next", response.NextPageToken)
}
