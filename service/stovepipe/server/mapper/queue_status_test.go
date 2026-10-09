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

func TestQueueStatusPolicyPresenceAndExecution(t *testing.T) {
	for _, tt := range []struct {
		name          string
		hasCommit     bool
		policy        entity.QueuePolicyState
		execution     entity.QueueExecutionState
		wantPolicy    pb.QueuePolicyState
		wantExecution pb.QueueExecutionState
	}{
		{"enabled and paused", true, entity.QueuePolicyStateEnabled, entity.QueueExecutionStatePaused, pb.QueuePolicyState_QUEUE_POLICY_STATE_ENABLED, pb.QueueExecutionState_QUEUE_EXECUTION_STATE_PAUSED},
		{"current disabled", false, entity.QueuePolicyStateDisabled, entity.QueueExecutionStateRunning, pb.QueuePolicyState_QUEUE_POLICY_STATE_DISABLED, pb.QueueExecutionState_QUEUE_EXECUTION_STATE_RUNNING},
		{"unknown values stay unknown", true, entity.QueuePolicyStateUnknown, entity.QueueExecutionStateUnknown, pb.QueuePolicyState_QUEUE_POLICY_STATE_UNKNOWN, pb.QueueExecutionState_QUEUE_EXECUTION_STATE_UNKNOWN},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := entity.QueuePolicy{Revision: 3, State: tt.policy, EffectiveFromCommitURI: "A", ChangedAtMs: 1000}
			result := GetQueueStatusResultToProto(entity.GetQueueStatusResult{Queue: "repo/main", CurrentPolicy: policy, PolicyForCommit: policy, HasPolicyForCommit: tt.hasCommit, Execution: entity.QueueExecutionStatus{State: tt.execution, ObservedAtMs: 2000}})
			require.Equal(t, tt.wantPolicy, result.CurrentPolicy.State)
			require.Equal(t, int64(3), result.CurrentPolicy.Revision)
			require.Equal(t, "A", result.CurrentPolicy.EffectiveFromCommitUri)
			require.Equal(t, int64(1000), result.CurrentPolicy.ChangedAtMs)
			require.Equal(t, tt.wantExecution, result.Execution.State)
			require.Equal(t, int64(2000), result.Execution.ObservedAtMs)
			if tt.hasCommit {
				require.Equal(t, result.CurrentPolicy, result.PolicyForCommit)
			} else {
				require.Nil(t, result.PolicyForCommit)
			}
		})
	}
	require.Equal(t, entity.GetQueueStatusRequest{}, ProtoToGetQueueStatusRequest(nil))
	require.Equal(t, entity.GetQueueStatusRequest{Queue: "repo/main", ChangeURI: "C"}, ProtoToGetQueueStatusRequest(&pb.GetQueueStatusRequest{Queue: "repo/main", ChangeUri: "C"}))
}
