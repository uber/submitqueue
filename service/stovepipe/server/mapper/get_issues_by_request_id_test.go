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

func TestRequestIssueMappings(t *testing.T) {
	require.Empty(t, ProtoToGetIssuesByRequestIDRequest(nil))
	require.Equal(t, entity.GetIssuesByRequestIDRequest{Queue: "queue", RequestID: "12", PageSize: 10, PageToken: "token"}, ProtoToGetIssuesByRequestIDRequest(&pb.GetIssuesByRequestIDRequest{Queue: "queue", RequestId: "12", PageSize: 10, PageToken: "token"}))
	result := entity.GetIssuesByRequestIDResult{Issues: []entity.RequestIssueSummary{{RequestID: "12", TaskID: "task", UnitID: "unit", Kind: entity.RequestIssueKindExisting, BuildID: "b/12", FirstSeenBuildID: "b/10"}}, Status: entity.RequestIssuesStatusComplete, NextPageToken: "next"}
	require.Equal(t, &pb.GetIssuesByRequestIDResponse{Issues: []*pb.RequestIssueSummary{{TaskId: "task", UnitId: "unit", Kind: "existing", BuildId: "b/12", FirstSeenBuildId: "b/10"}}, Status: "complete", NextPageToken: "next"}, GetIssuesByRequestIDResultToProto(result))
	require.Empty(t, GetIssuesByRequestIDResultToProto(entity.GetIssuesByRequestIDResult{}).Issues)
}
