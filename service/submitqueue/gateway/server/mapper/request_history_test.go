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

func TestProtoToGetRequestHistoryRequests(t *testing.T) {
	assert.Equal(t,
		entity.GetRequestHistoryByIDRequest{ID: "q/1"},
		ProtoToGetRequestHistoryByIDRequest(&pb.GetRequestHistoryByIDRequest{Sqid: "q/1"}),
	)
	assert.Equal(t,
		entity.GetRequestHistoryByChangeURIRequest{ChangeURI: "uri"},
		ProtoToGetRequestHistoryByChangeURIRequest(&pb.GetRequestHistoryByChangeURIRequest{ChangeUri: "uri"}),
	)
}

func TestHistoryEventsToProto(t *testing.T) {
	events := HistoryEventsToProto([]entity.RequestLog{{
		TimestampMs: 10,
		Status:      entity.RequestStatusError,
		LastError:   "failed",
		Metadata:    map[string]string{"step": "build"},
	}})

	assert.Equal(t, &pb.HistoryEvent{
		TimestampMs: 10,
		Status:      string(entity.RequestStatusError),
		LastError:   "failed",
		Metadata:    map[string]string{"step": "build"},
	}, events[0])
}

func TestRequestHistoriesToProto(t *testing.T) {
	histories := RequestHistoriesToProto([]entity.RequestHistory{{
		RequestID: "q/1",
		Events:    []entity.RequestLog{{TimestampMs: 10, Status: entity.RequestStatusAccepted}},
	}})

	assert.Equal(t, "q/1", histories[0].Sqid)
	assert.Equal(t, string(entity.RequestStatusAccepted), histories[0].Events[0].Status)
}
