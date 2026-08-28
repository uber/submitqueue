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

func TestProtoToGetProjectStatusByURIRequest(t *testing.T) {
	project := ""
	got := ProtoToGetProjectStatusByURIRequest(&pb.GetProjectStatusByURIRequest{
		Queue: "monorepo/main", ChangeUri: "git://commit", Project: &project, PageSize: 10, PageToken: "token",
	})

	assert.Equal(t, entity.GetProjectStatusByURIRequest{
		Queue: "monorepo/main", ChangeURI: "git://commit", Project: "", HasProject: true, PageSize: 10, PageToken: "token",
	}, got)

	omitted := ProtoToGetProjectStatusByURIRequest(&pb.GetProjectStatusByURIRequest{})
	assert.False(t, omitted.HasProject)
}

func TestGetProjectStatusByURIResultToProto(t *testing.T) {
	t.Run("preserves optional field presence", func(t *testing.T) {
		result := entity.GetProjectStatusByURIResult{
			Request: entity.Request{
				ID: "request/monorepo/main/7", Queue: "monorepo/main", URI: "git://commit",
				BaseURI: "git://base", State: entity.RequestStateSucceeded,
			},
			RepositoryValidationFact:    entity.ValidationFact{Degree: entity.DegreeGreen},
			HasRepositoryValidationFact: true,
		}

		got, err := GetProjectStatusByURIResultToProto(result)

		require.NoError(t, err)
		assert.Equal(t, result.Request.ID, got.GetRequestId())
		assert.Equal(t, result.Request.Queue, got.GetQueue())
		assert.Equal(t, result.Request.URI, got.GetChangeUri())
		assert.Equal(t, result.Request.BaseURI, got.GetBaseUri())
		assert.NotNil(t, got.RepositoryBreakageDegree)
		assert.Equal(t, entity.DegreeGreen, got.GetRepositoryBreakageDegree())
		assert.Equal(t, pb.RequestState_REQUEST_STATE_SUCCEEDED, got.GetRequestState())
		assert.Empty(t, got.Projects)
		assert.False(t, got.ProjectResultsComplete)
		assert.Empty(t, got.NextPageToken)
	})

	t.Run("keeps missing repository fact absent", func(t *testing.T) {
		got, err := GetProjectStatusByURIResultToProto(entity.GetProjectStatusByURIResult{
			Request: entity.Request{State: entity.RequestStateAccepted},
		})

		require.NoError(t, err)
		assert.Nil(t, got.RepositoryBreakageDegree)
		assert.Empty(t, got.Projects)
	})

	t.Run("rejects an unrecognized request state", func(t *testing.T) {
		got, err := GetProjectStatusByURIResultToProto(entity.GetProjectStatusByURIResult{
			Request: entity.Request{State: "future"},
		})

		require.Error(t, err)
		assert.Nil(t, got)
	})
}

func TestProjectStatusRequestStateToProto(t *testing.T) {
	tests := []struct {
		name      string
		state     entity.RequestState
		want      pb.RequestState
		wantError bool
	}{
		{name: "accepted", state: entity.RequestStateAccepted, want: pb.RequestState_REQUEST_STATE_ACCEPTED},
		{name: "processing", state: entity.RequestStateProcessing, want: pb.RequestState_REQUEST_STATE_PROCESSING},
		{name: "succeeded", state: entity.RequestStateSucceeded, want: pb.RequestState_REQUEST_STATE_SUCCEEDED},
		{name: "failed", state: entity.RequestStateFailed, want: pb.RequestState_REQUEST_STATE_FAILED},
		{name: "cancelled", state: entity.RequestStateCancelled, want: pb.RequestState_REQUEST_STATE_CANCELLED},
		{name: "superseded", state: entity.RequestStateSuperseded, want: pb.RequestState_REQUEST_STATE_SUPERSEDED},
		{name: "unknown", state: entity.RequestStateUnknown, wantError: true},
		{name: "unrecognized", state: "future", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := projectStatusRequestStateToProto(tt.state)

			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
