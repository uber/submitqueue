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

func TestProtoToGetRequestSummaryByIDRequest(t *testing.T) {
	assert.Equal(t,
		entity.GetRequestSummaryByIDRequest{ID: "test-queue/42"},
		ProtoToGetRequestSummaryByIDRequest(&pb.GetRequestSummaryByIDRequest{Sqid: "test-queue/42"}),
	)
}

func TestProtoToGetRequestSummaryByChangeURIRequest(t *testing.T) {
	assert.Equal(t,
		entity.GetRequestSummaryByChangeURIRequest{ChangeURI: "github://uber/repo/pull/1/abc"},
		ProtoToGetRequestSummaryByChangeURIRequest(&pb.GetRequestSummaryByChangeURIRequest{ChangeUri: "github://uber/repo/pull/1/abc"}),
	)
}

func TestRequestSummaryToProto(t *testing.T) {
	summary := entity.RequestSummary{
		RequestID:    "test-queue/42",
		Queue:        "test-queue",
		ChangeURIs:   []string{"github://uber/repo/pull/1/abc"},
		ReceivedAtMs: 100,
		Status:       entity.RequestStatusValidating,
		LastError:    "validation failed",
		Metadata:     map[string]string{"step": "lint"},
	}

	assert.Equal(t, &pb.RequestSummary{
		Sqid:         "test-queue/42",
		Queue:        "test-queue",
		ChangeUris:   []string{"github://uber/repo/pull/1/abc"},
		ReceivedAtMs: 100,
		Status:       string(entity.RequestStatusValidating),
		LastError:    "validation failed",
		Metadata:     map[string]string{"step": "lint"},
	}, RequestSummaryToProto(summary))
}

func TestRequestSummariesToProto(t *testing.T) {
	summaries := []entity.RequestSummary{
		{RequestID: "test-queue/2", Status: entity.RequestStatusLanded},
		{RequestID: "test-queue/1", Status: entity.RequestStatusError},
	}

	requests := RequestSummariesToProto(summaries)

	assert.Equal(t, []string{"test-queue/2", "test-queue/1"}, []string{requests[0].Sqid, requests[1].Sqid})
}
