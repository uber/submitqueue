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
	pb "github.com/uber/submitqueue/api/stovepipe/protopb"
	"github.com/uber/submitqueue/stovepipe/entity"
)

// ProtoToGetQueueStatusRequest preserves the optional commit selector.
func ProtoToGetQueueStatusRequest(req *pb.GetQueueStatusRequest) entity.GetQueueStatusRequest {
	return entity.GetQueueStatusRequest{Queue: req.GetQueue(), ChangeURI: req.GetChangeUri()}
}

// GetQueueStatusResultToProto preserves commit-policy presence independently of execution status.
func GetQueueStatusResultToProto(result entity.GetQueueStatusResult) *pb.GetQueueStatusResponse {
	execution := pb.QueueExecutionState_QUEUE_EXECUTION_STATE_UNKNOWN
	switch result.Execution.State {
	case entity.QueueExecutionStateRunning:
		execution = pb.QueueExecutionState_QUEUE_EXECUTION_STATE_RUNNING
	case entity.QueueExecutionStatePaused:
		execution = pb.QueueExecutionState_QUEUE_EXECUTION_STATE_PAUSED
	}
	response := &pb.GetQueueStatusResponse{
		Queue: result.Queue, CurrentPolicy: queuePolicyToProto(result.CurrentPolicy),
		Execution: &pb.QueueExecutionStatus{State: execution, ObservedAtMs: result.Execution.ObservedAtMs},
	}
	if result.HasPolicyForCommit {
		response.PolicyForCommit = queuePolicyToProto(result.PolicyForCommit)
	}
	return response
}

func queuePolicyToProto(policy entity.QueuePolicy) *pb.QueuePolicy {
	state := pb.QueuePolicyState_QUEUE_POLICY_STATE_UNKNOWN
	switch policy.State {
	case entity.QueuePolicyStateEnabled:
		state = pb.QueuePolicyState_QUEUE_POLICY_STATE_ENABLED
	case entity.QueuePolicyStateDisabled:
		state = pb.QueuePolicyState_QUEUE_POLICY_STATE_DISABLED
	}
	return &pb.QueuePolicy{Revision: policy.Revision, State: state, EffectiveFromCommitUri: policy.EffectiveFromCommitURI, ChangedAtMs: policy.ChangedAtMs}
}
