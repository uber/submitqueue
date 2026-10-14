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

func TestQueueStatusSelectedEnablement(t *testing.T) {
	for _, tt := range []struct {
		name      string
		policy    entity.QueueEnablement
		wantState pb.QueueEnablement
	}{
		{"enabled", entity.QueueEnablementEnabled, pb.QueueEnablement_QUEUE_ENABLEMENT_ENABLED},
		{"disabled", entity.QueueEnablementDisabled, pb.QueueEnablement_QUEUE_ENABLEMENT_DISABLED},
		{"unknown values stay unknown", entity.QueueEnablementUnknown, pb.QueueEnablement_QUEUE_ENABLEMENT_UNKNOWN},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := entity.QueueState{Version: 3, State: tt.policy, EffectiveFromCommitURI: "A", ChangedAtMs: 1000}
			result := GetQueueStatusResultToProto(entity.GetQueueStatusResult{Queue: "repo/main", Enablement: policy})
			require.Equal(t, "repo/main", result.Queue)
			require.Equal(t, tt.wantState, result.Enablement.State)
			require.Equal(t, int64(3), result.Enablement.Version)
			require.Equal(t, "A", result.Enablement.EffectiveFromCommitUri)
			require.Equal(t, int64(1000), result.Enablement.ChangedAtMs)
		})
	}
	require.Equal(t, entity.GetQueueStatusRequest{}, ProtoToGetQueueStatusRequest(nil))
	require.Equal(t, entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: "C"}, ProtoToGetQueueStatusRequest(&pb.GetQueueStatusRequest{Queue: "repo/main", ChangeUri: "C"}))
}
